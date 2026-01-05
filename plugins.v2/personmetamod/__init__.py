import base64
import copy
import datetime
import json
import re
import threading
import time
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional

import pytz
import zhconv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from requests import RequestException

from app import schemas
from app.chain.mediaserver import MediaServerChain
from app.chain.tmdb import TmdbChain
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.core.meta import MetaBase
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import MediaInfo, MediaServerItem, ServiceInfo
from app.schemas.types import EventType, MediaType
from app.utils.common import retry
from app.utils.http import RequestUtils
from app.utils.string import StringUtils


class personmetamod(_PluginBase):
    # 插件名称
    plugin_name = "演职人员刮削(自由版)"
    # 插件描述
    plugin_desc = "混合策略(豆瓣/TMDB)，支持繁转简，Emby/Jellyfin专用。"
    # 插件图标
    plugin_icon = "actor.png"
    # 插件版本
    plugin_version = "2.3.3_mod_v9_optional_lock"
    # 插件作者
    plugin_author = "jxxghp"
    # 作者主页
    author_url = "https://github.com/jxxghp"
    # 插件配置项ID前缀
    plugin_config_prefix = "personmeta_mod_"
    # 加载顺序
    plugin_order = 24
    # 可使用的用户级别
    auth_level = 1

    # 退出事件
    _event = threading.Event()

    # 私有属性
    _scheduler = None
    _enabled = False
    _onlyonce = False
    _cron = None
    _delay = 0
    _type = "all"
    _remove_nozh = False
    _lock_info = False  # 默认关闭锁定
    _mediaservers = []

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._cron = config.get("cron")
            self._type = config.get("type") or "all"
            self._delay = config.get("delay") or 0
            self._remove_nozh = config.get("remove_nozh") or False
            # 获取锁定配置，默认为 False (不锁定)
            self._lock_info = config.get("lock_info") or False
            self._mediaservers = config.get("mediaservers") or []

        # 停止现有任务
        self.stop_service()

        # 启动服务
        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(func=self.scrap_library, trigger='date',
                                    run_date=datetime.datetime.now(
                                        tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3)
                                    )
            logger.info(f"演职人员刮削(自由版)服务启动，立即运行一次")
            # 关闭一次性开关
            self._onlyonce = False
            # 保存配置
            self.__update_config()
            # 启动服务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __update_config(self):
        """
        更新配置
        """
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "type": self._type,
            "delay": self._delay,
            "remove_nozh": self._remove_nozh,
            "lock_info": self._lock_info,
            "mediaservers": self._mediaservers
        })

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        """
        if self._enabled and self._cron:
            return [{
                "id": "personmetamod",
                "name": "演职人员刮削服务(自由版)",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.scrap_library,
                "kwargs": {}
            }]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面
        """
        return [
            {
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12,
                            'md': 6
                        },
                        'content': [
                            {
                                'component': 'VSwitch',
                                'props': {
                                    'model': 'enabled',
                                    'label': '启用插件',
                                }
                            }
                        ]
                    },
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12,
                            'md': 6
                        },
                        'content': [
                            {
                                'component': 'VSwitch',
                                'props': {
                                    'model': 'onlyonce',
                                    'label': '立即运行一次',
                                }
                            }
                        ]
                    }
                ]
            },
            {
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12,
                            'md': 4
                        },
                        'content': [
                            {
                                'component': 'VCronField',
                                'props': {
                                    'model': 'cron',
                                    'label': '媒体库扫描周期',
                                    'placeholder': '5位cron表达式'
                                }
                            }
                        ]
                    },
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12,
                            'md': 4
                        },
                        'content': [
                            {
                                'component': 'VTextField',
                                'props': {
                                    'model': 'delay',
                                    'label': '入库延迟时间（秒）',
                                    'placeholder': '30'
                                }
                            }
                        ]
                    },
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12,
                            'md': 4
                        },
                        'content': [
                            {
                                'component': 'VSelect',
                                'props': {
                                    'model': 'type',
                                    'label': '刮削条件',
                                    'items': [
                                        {'title': '全部', 'value': 'all'},
                                        {'title': '演员非中文', 'value': 'name'},
                                        {'title': '角色非中文', 'value': 'role'},
                                    ]
                                }
                            }
                        ]
                    }
                ]
            },
            {
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12
                        },
                        'content': [
                            {
                                'component': 'VSelect',
                                'props': {
                                    'multiple': True,
                                    'chips': True,
                                    'clearable': True,
                                    'model': 'mediaservers',
                                    'label': '媒体服务器',
                                    'items': [{"title": config.name, "value": config.name}
                                              for config in MediaServerHelper().get_configs().values()]
                                }
                            }
                        ]
                    }
                ]
            },
            {
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12,
                            'md': 6
                        },
                        'content': [
                            {
                                'component': 'VSwitch',
                                'props': {
                                    'model': 'remove_nozh',
                                    'label': '删除非中文演员',
                                }
                            }
                        ]
                    },
                    # 这里的开关默认值为 False，如果不开启，绝不会执行锁定逻辑
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12,
                            'md': 6
                        },
                        'content': [
                            {
                                'component': 'VSwitch',
                                'props': {
                                    'model': 'lock_info',
                                    'label': '锁定元数据 (可选)',
                                    'hint': '开启后，修改过的信息将被锁定(Name/Overview)，防止被NFO覆盖。建议保持关闭。',
                                }
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "cron": "",
            "type": "all",
            "delay": 30,
            "remove_nozh": False,
            "lock_info": False,  # 默认关闭
            "mediaservers": []
        }

    def get_page(self) -> List[dict]:
        pass

    def service_infos(self, type_filter: Optional[str] = None) -> Optional[Dict[str, ServiceInfo]]:
        """
        服务信息
        """
        if not self._mediaservers:
            logger.warning("尚未配置媒体服务器，请检查配置")
            return None

        # 过滤 Emby 和 Jellyfin
        services = MediaServerHelper().get_services(type_filter=type_filter, name_filters=self._mediaservers)
        if not services:
            logger.warning("获取媒体服务器实例失败，请检查配置")
            return None

        active_services = {}
        for service_name, service_info in services.items():
            if service_info.type == 'plex':
                continue
            if service_info.instance.is_inactive():
                logger.warning(f"媒体服务器 {service_name} 未连接，请检查配置")
            else:
                active_services[service_name] = service_info

        if not active_services:
            logger.warning("没有已连接的媒体服务器 (Emby/Jellyfin)，请检查配置")
            return None

        return active_services

    @eventmanager.register(EventType.TransferComplete)
    def scrap_rt(self, event: Event):
        """
        根据事件实时刮削演员信息
        """
        if not self._enabled:
            return
        # 事件数据
        if not event or not event.event_data:
            logger.warn("TransferComplete事件数据为空")
            return
        mediainfo: MediaInfo = event.event_data.get("mediainfo")
        meta: MetaBase = event.event_data.get("meta")
        if not mediainfo or not meta:
            return
        # 延迟
        if self._delay:
            time.sleep(int(self._delay))
        # 查询媒体服务器中的条目
        existsinfo = self.chain.media_exists(mediainfo=mediainfo)
        if not existsinfo or not existsinfo.itemid:
            logger.warn(f"{mediainfo.title_year} 在媒体库中不存在")
            return
        # Emby/Jellyfin Only
        if existsinfo.server_type == 'plex':
            return
            
        # 查询条目详情
        iteminfo = MediaServerChain().iteminfo(server=existsinfo.server, item_id=existsinfo.itemid)
        if not iteminfo:
            logger.warn(f"{mediainfo.title_year} 条目详情获取失败")
            return
        # 刮削演职人员信息
        self.__update_item(server=existsinfo.server, server_type=existsinfo.server_type,
                           item=iteminfo, mediainfo=mediainfo, season=meta.begin_season)

    def scrap_library(self):
        """
        扫描整个媒体库，刮削演员信息
        """
        # 所有媒体服务器
        service_infos = self.service_infos()
        if not service_infos:
            return
        mediaserverchain = MediaServerChain()
        for server, service in service_infos.items():
            # 扫描所有媒体库
            logger.info(f"开始刮削服务器 {server} 的演员信息 ...")
            for library in mediaserverchain.librarys(server):
                logger.info(f"开始刮削媒体库 {library.name} 的演员信息 ...")
                for item in mediaserverchain.items(server, library.id):
                    if not item:
                        continue
                    if not item.item_id:
                        continue
                    if "Series" not in item.item_type \
                            and "Movie" not in item.item_type:
                        continue
                    if self._event.is_set():
                        logger.info(f"演职人员刮削服务停止")
                        return
                    # 处理条目
                    logger.debug(f"开始刮削 {item.title} 的演员信息 ...")
                    self.__update_item(server=server, item=item, server_type=service.type)
                logger.info(f"媒体库 {library.name} 的演员信息刮削完成")
            logger.info(f"服务器 {server} 的演员信息刮削完成")

    def __update_peoples(self, server: str, server_type: str,
                         itemid: str, iteminfo: dict, douban_actors):
        peoples = []
        # 更新当前媒体项人物
        for people in iteminfo.get("People", []) or []:
            if self._event.is_set():
                logger.info(f"演职人员刮削服务停止")
                return
            if not people.get("Name"):
                continue
            # 若全是中文则跳过（根据配置）
            if StringUtils.is_chinese(people.get("Name")) \
                    and StringUtils.is_chinese(people.get("Role")):
                peoples.append(people)
                continue
            
            info = self.__update_people(server=server, server_type=server_type,
                                        people=people, douban_actors=douban_actors)
            if info:
                peoples.append(info)
            elif not self._remove_nozh:
                peoples.append(people)
        # 保存媒体项信息
        if peoples:
            iteminfo["People"] = peoples
            self.set_iteminfo(server=server, server_type=server_type,
                              itemid=itemid, iteminfo=iteminfo)

    def __update_item(self, server: str, item: MediaServerItem, server_type: str = None,
                      mediainfo: MediaInfo = None, season: int = None):
        """
        更新媒体服务器中的条目
        """

        def __need_trans_actor(_item):
            """
            是否需要处理人物信息
            """
            if self._type == "name":
                _peoples = [x for x in _item.get("People", []) if
                            (x.get("Name") and not StringUtils.is_chinese(x.get("Name")))]
            elif self._type == "role":
                _peoples = [x for x in _item.get("People", []) if
                            (x.get("Role") and not StringUtils.is_chinese(x.get("Role")))]
            else:
                _peoples = [x for x in _item.get("People", []) if
                            (x.get("Name") and not StringUtils.is_chinese(x.get("Name")))
                            or (x.get("Role") and not StringUtils.is_chinese(x.get("Role")))]
            if _peoples:
                return True
            return False

        # 识别媒体信息
        if not mediainfo:
            if not item.tmdbid:
                logger.debug(f"{item.title} 未找到tmdbid，无法识别媒体信息")
                return
            mtype = MediaType.TV if item.item_type in ['Series', 'show'] else MediaType.MOVIE
            mediainfo = self.chain.recognize_media(mtype=mtype, tmdbid=item.tmdbid)
            if not mediainfo:
                logger.warn(f"{item.title} 未识别到媒体信息")
                return

        # 获取媒体项
        iteminfo = self.get_iteminfo(server=server, server_type=server_type, itemid=item.item_id)
        if not iteminfo:
            logger.warn(f"{item.title} 未找到媒体项")
            return

        if __need_trans_actor(iteminfo):
            # 获取豆瓣演员信息
            douban_actors = self.__get_douban_actors(mediainfo=mediainfo, season=season)
            self.__update_peoples(server=server, server_type=server_type,
                                  itemid=item.item_id, iteminfo=iteminfo, douban_actors=douban_actors)

        # 处理季和集人物
        if iteminfo.get("Type") and "Series" in iteminfo["Type"]:
            # 获取季媒体项
            seasons = self.get_items(server=server, server_type=server_type,
                                     parentid=item.item_id, mtype="Season")
            if not seasons:
                return
            for season in seasons.get("Items", []):
                # 获取豆瓣演员信息
                season_actors = self.__get_douban_actors(mediainfo=mediainfo, season=season.get("IndexNumber"))
                # 如果是Jellyfin，更新季的人物，Emby/Plex季没有人物
                if server_type == "jellyfin":
                    seasoninfo = self.get_iteminfo(server=server, server_type=server_type,
                                                   itemid=season.get("Id"))
                    if seasoninfo and __need_trans_actor(seasoninfo):
                        self.__update_peoples(server=server, server_type=server_type,
                                              itemid=season.get("Id"), iteminfo=seasoninfo,
                                              douban_actors=season_actors)
                # 获取集媒体项
                episodes = self.get_items(server=server, server_type=server_type,
                                          parentid=season.get("Id"), mtype="Episode")
                if not episodes:
                    continue
                # 更新集媒体项人物
                for episode in episodes.get("Items", []):
                    episodeinfo = self.get_iteminfo(server=server, server_type=server_type,
                                                    itemid=episode.get("Id"))
                    if episodeinfo and __need_trans_actor(episodeinfo):
                        self.__update_peoples(server=server, server_type=server_type,
                                              itemid=episode.get("Id"), iteminfo=episodeinfo,
                                              douban_actors=season_actors)

    def __update_people(self, server: str, server_type: str,
                        people: dict, douban_actors: list = None) -> Optional[dict]:
        """
        更新人物信息
        """
        original_name = people.get("Name")
        logger.debug(f"正在处理人物: {original_name} (ID: {people.get('Id')}) ...")

        # 辅助函数：获取 TMDB ID
        def __get_peopleid(p: dict) -> Tuple[Optional[str], Optional[str]]:
            if not p.get("ProviderIds"):
                return None, None
            pid = p["ProviderIds"]
            peopletmdbid = pid.get("Tmdb") or pid.get("tmdb")
            peopleimdbid = pid.get("Imdb") or pid.get("imdb")
            return peopletmdbid, peopleimdbid

        # 辅助函数：繁转简
        def __to_zh_cn(text: str) -> str:
            if not text:
                return text
            return zhconv.convert(text, 'zh-cn')

        # 返回给媒体项本身的人物信息
        ret_people = copy.deepcopy(people)

        try:
            # 1. 查询媒体库人物全局详情
            personinfo = self.get_iteminfo(server=server, server_type=server_type,
                                           itemid=people.get("Id"))
            if not personinfo:
                logger.debug(f"未在 Emby 中找到人物详情: {original_name}")
                return None

            # 标记是否需要更新全局信息
            updated_global = False
            
            # 最终决策值
            final_name = None
            final_overview = None
            final_img = None
            
            # --- 准备 TMDB 数据 ---
            tmdb_name_cn = None
            tmdb_name_en = None
            tmdb_overview_cn = None
            tmdb_overview_en = None
            tmdb_img = None
            
            person_tmdbid, _ = __get_peopleid(personinfo)
            if person_tmdbid:
                try:
                    logger.debug(f"正在获取 TMDB 人物详情: {person_tmdbid} ...")
                    person_detail = TmdbChain().person_detail(int(person_tmdbid))
                    if person_detail:
                        # 图片
                        _path = person_detail.profile_path
                        if _path:
                            tmdb_img = f"https://{settings.TMDB_IMAGE_DOMAIN}/t/p/original{_path}"
                        
                        # 姓名
                        if person_detail.name:
                            if StringUtils.is_chinese(person_detail.name):
                                tmdb_name_cn = person_detail.name
                            else:
                                tmdb_name_en = person_detail.name

                        # 简介
                        if person_detail.biography:
                            if StringUtils.is_chinese(person_detail.biography):
                                tmdb_overview_cn = person_detail.biography
                            else:
                                tmdb_overview_en = person_detail.biography
                        
                        logger.debug(f"TMDB获取结果: 中文名={tmdb_name_cn}, 英文名={tmdb_name_en}, 简介长度={len(person_detail.biography) if person_detail.biography else 0}")
                except Exception as e:
                    logger.warn(f"TMDB获取人物详情失败: {e}")

            # --- 准备 豆瓣 数据 ---
            douban_match = None
            if douban_actors:
                for douban_actor in douban_actors:
                    is_match = False
                    # 匹配逻辑：匹配当前英文名，或者匹配 TMDB 获取到的中文名
                    current_name = people.get("Name")
                    if douban_actor.get("latin_name") == current_name or \
                       douban_actor.get("name") == current_name:
                        is_match = True
                    elif tmdb_name_cn and douban_actor.get("name") == tmdb_name_cn:
                        is_match = True
                    
                    if is_match:
                        douban_match = douban_actor
                        logger.info(f"豆瓣匹配成功: {current_name} => {douban_match.get('name')}")
                        break

            # --- 决策逻辑 ---

            # 1. 【姓名 (Name)】
            # 优先级：豆瓣中文 > TMDB中文 > TMDB英文
            douban_name = douban_match.get("name") if douban_match else None
            is_douban_zh = StringUtils.is_chinese(douban_name) if douban_name else False
            
            if is_douban_zh:
                final_name = douban_name
                logger.debug(f"姓名策略: 使用豆瓣中文名 [{final_name}]")
            elif tmdb_name_cn:
                final_name = tmdb_name_cn
                logger.debug(f"姓名策略: 使用 TMDB 中文名 [{final_name}]")
            elif tmdb_name_en:
                final_name = tmdb_name_en
                logger.debug(f"姓名策略: 使用 TMDB 英文名 [{final_name}]")
            
            # 繁转简
            if final_name:
                final_name_sc = __to_zh_cn(final_name)
                if final_name_sc != final_name:
                    logger.debug(f"姓名繁转简: {final_name} -> {final_name_sc}")
                    final_name = final_name_sc

            # 2. 【简介 (Overview)】
            # 优先级：TMDB中文 > TMDB英文 > 豆瓣
            if tmdb_overview_cn:
                final_overview = tmdb_overview_cn
                logger.debug(f"简介策略: 使用 TMDB 中文简介")
            elif tmdb_overview_en:
                final_overview = tmdb_overview_en
                logger.debug(f"简介策略: 使用 TMDB 英文简介")
            elif douban_match:
                # 尝试获取豆瓣简介
                raw_intro = douban_match.get("summary") or douban_match.get("intro") or douban_match.get("biography")
                if raw_intro:
                     final_overview = raw_intro
                     logger.debug(f"简介策略: 使用 豆瓣 简介")

            # 繁转简
            if final_overview:
                final_overview = __to_zh_cn(final_overview)

            # 3. 【图片 (Image)】
            # 优先级：豆瓣 > TMDB
            img_source = "None"
            if douban_match:
                avatar = douban_match.get("avatar")
                if isinstance(avatar, dict) and avatar.get("large"):
                    final_img = avatar.get("large")
                    img_source = "Douban"
                elif isinstance(avatar, str):
                    final_img = avatar
                    img_source = "Douban"
            
            # 豆瓣没图，用 TMDB
            if not final_img and tmdb_img:
                final_img = tmdb_img
                img_source = "TMDB"

            # 4. 【角色 (Role)】
            final_role = None
            if douban_match and douban_match.get("character"):
                # 清洗
                character = re.sub(r"饰\s*|演员\s*", "", douban_match.get("character")).strip()
                if character:
                    final_role = __to_zh_cn(character)
                    logger.debug(f"角色信息: 从豆瓣获取 [{final_role}]")

            # --- 执行更新判断 ---

            # A. 全局信息更新 (Name, Overview)
            if final_name and final_name != personinfo.get("Name"):
                logger.info(f"更新人物姓名: {personinfo.get('Name')} -> {final_name}")
                personinfo["Name"] = final_name
                updated_global = True
            
            if final_overview and final_overview != personinfo.get("Overview"):
                logger.info(f"更新人物简介: {final_name} ...")
                personinfo["Overview"] = final_overview
                updated_global = True

            # B. 媒体项角色更新 (Role)
            if final_role:
                ret_people["Role"] = final_role
                if final_name: ret_people["Name"] = final_name
            
            # C. 图片更新
            if final_img:
                logger.info(f"正在更新图片 ({img_source}): {final_name}")
                if not self.set_item_image(server=server, server_type=server_type, 
                                    itemid=people.get("Id"), imageurl=final_img):
                    logger.warn(f"图片下载/更新失败: {final_img}")

            # D. 锁定逻辑 (仅当配置开关开启时才执行)
            if self._lock_info and updated_global:
                if "LockedFields" not in personinfo: 
                    personinfo["LockedFields"] = []
                
                fields_to_lock = []
                if personinfo.get("Name"): fields_to_lock.append("Name")
                if personinfo.get("Overview"): fields_to_lock.append("Overview")
                
                for f in fields_to_lock:
                    if f not in personinfo["LockedFields"]:
                        personinfo["LockedFields"].append(f)
                        logger.info(f"锁定字段: {f}")
            
            # --- 提交全局修改 ---
            if updated_global:
                logger.info(f"提交更新人物全局信息: {final_name}")
                self.set_iteminfo(server=server, server_type=server_type,
                                  itemid=people.get("Id"), iteminfo=personinfo)
                return ret_people
            
            # 仅角色或名字变动返回
            if final_role or (final_name and final_name != people.get("Name")):
                logger.info(f"仅更新影片内角色/姓名: {final_name} - {final_role}")
                if final_name: ret_people["Name"] = final_name
                return ret_people

        except Exception as err:
            logger.error(f"更新人物信息发生错误：{str(err)}")
        
        return None

    def __get_douban_actors(self, mediainfo: MediaInfo, season: int = None) -> List[dict]:
        sleep_time = 3 + int(time.time()) % 7
        time.sleep(sleep_time)
        doubaninfo = self.chain.match_doubaninfo(name=mediainfo.title,
                                                 imdbid=mediainfo.imdb_id,
                                                 mtype=mediainfo.type,
                                                 year=mediainfo.year,
                                                 season=season)
        if doubaninfo:
            doubanitem = self.chain.douban_info(doubaninfo.get("id")) or {}
            logger.info(f"获取豆瓣条目成功: {mediainfo.title_year} (ID: {doubaninfo.get('id')})")
            return (doubanitem.get("actors") or []) + (doubanitem.get("directors") or [])
        else:
            logger.debug(f"未找到豆瓣信息：{mediainfo.title_year}")
        return []

    def get_iteminfo(self, server: str, server_type: str, itemid: str) -> dict:
        service = self.service_infos(server_type).get(server)
        if not service: return {}

        def __get_emby_iteminfo() -> dict:
            try:
                # Emby/Jellyfin 通用
                url = f'[HOST]emby/Users/[USER]/Items/{itemid}?Fields=ChannelMappingInfo&api_key=[APIKEY]'
                if server_type == 'jellyfin':
                    url = f'[HOST]Users/[USER]/Items/{itemid}?Fields=ChannelMappingInfo&api_key=[APIKEY]'
                    
                res = service.instance.get_data(url=url)
                if res: 
                    result = res.json()
                    if result and result.get("Path"):
                        result['FileName'] = Path(result['Path']).name
                    return result
            except Exception as err:
                logger.error(f"获取媒体项详情失败：{str(err)}")
            return {}

        return __get_emby_iteminfo()

    def get_items(self, server: str, server_type: str, parentid: str, mtype: str = None) -> dict:
        service = self.service_infos(server_type).get(server)
        if not service: return {}

        def __get_emby_items() -> dict:
            try:
                base_url = f'[HOST]emby/Users/[USER]/Items?api_key=[APIKEY]'
                if server_type == 'jellyfin':
                    base_url = f'[HOST]Users/[USER]/Items?api_key=[APIKEY]'
                
                if parentid:
                    url = f"{base_url}&ParentId={parentid}"
                else:
                    url = base_url
                res = service.instance.get_data(url=url)
                if res: return res.json()
            except Exception as err:
                logger.error(f"获取子媒体项失败：{str(err)}")
            return {}

        return __get_emby_items()

    def set_iteminfo(self, server: str, server_type: str, itemid: str, iteminfo: dict):
        service = self.service_infos(server_type).get(server)
        if not service: return {}
        try:
            url = f'[HOST]emby/Items/{itemid}?api_key=[APIKEY]&reqformat=json'
            if server_type == "jellyfin":
                url = f'[HOST]Items/{itemid}?api_key=[APIKEY]'
            
            res = service.instance.post_data(
                url=url,
                data=json.dumps(iteminfo),
                headers={"Content-Type": "application/json"}
            )
            if res and res.status_code in [200, 204]: return True
        except Exception as err:
            logger.error(f"更新媒体项详情失败：{str(err)}")
        return False

    @retry(RequestException, logger=logger)
    def set_item_image(self, server: str, server_type: str, itemid: str, imageurl: str):
        service = self.service_infos(server_type).get(server)
        if not service: return {}

        def __download_image():
            try:
                headers = {}
                if "doubanio.com" in imageurl:
                    headers['Referer'] = "https://movie.douban.com/"
                
                r = RequestUtils(proxies=settings.PROXY, ua=settings.USER_AGENT, headers=headers).get_res(url=imageurl, raise_exception=True)
                if r: return base64.b64encode(r.content).decode()
            except Exception as err:
                logger.warn(f"下载图片失败 ({imageurl}): {str(err)}")
            return None

        def __set_emby_item_image(_base64: str):
            try:
                url = f'[HOST]emby/Items/{itemid}/Images/Primary?api_key=[APIKEY]'
                if server_type == "jellyfin":
                    url = f'[HOST]Items/{itemid}/Images/Primary?api_key=[APIKEY]'
                
                res = service.instance.post_data(url=url, data=_base64, headers={"Content-Type": "image/png"})
                if res and res.status_code in [200, 204]: return True
            except Exception as result:
                logger.error(f"推送图片到媒体服务器失败：{result}")
            return False

        image_base64 = __download_image()
        if image_base64: 
            return __set_emby_item_image(image_base64)
        return False

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            print(str(e))
