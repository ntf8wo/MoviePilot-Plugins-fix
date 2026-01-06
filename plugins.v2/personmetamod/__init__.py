import base64
import copy
import datetime
import json
import re
import threading
import time
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional
from urllib.parse import quote

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
    plugin_name = "演职人员刮削-Mod"
    # 插件描述
    plugin_desc = "刮削演职人员详细元数据（姓名/角色仅限TMDB，简介/图片TMDB优先豆瓣兜底）。支持Emby出生/逝世/外部ID等字段。"
    # 插件图标
    plugin_icon = "actor.png"
    # 插件版本
    plugin_version = "3.0.0"
    # 插件作者
    plugin_author = "ntf8wo"
    # 作者主页
    author_url = ""
    # 插件配置项ID前缀
    plugin_config_prefix = "personmetamod_"
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
    _mediaservers = []

    def init_plugin(self, config: dict = None):

        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._cron = config.get("cron")
            self._type = config.get("type") or "all"
            self._delay = config.get("delay") or 0
            self._remove_nozh = config.get("remove_nozh") or False
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
            logger.info(f"演职人员刮削服务(Mod)启动，立即运行一次")
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
                "name": "演职人员刮削服务(Mod)",
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
                'component': 'VForm',
                'content': [
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
            "remove_nozh": False
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

        services = MediaServerHelper().get_services(type_filter=type_filter, name_filters=self._mediaservers)
        if not services:
            logger.warning("获取媒体服务器实例失败，请检查配置")
            return None

        active_services = {}
        for service_name, service_info in services.items():
            if service_info.instance.is_inactive():
                logger.warning(f"媒体服务器 {service_name} 未连接，请检查配置")
            else:
                active_services[service_name] = service_info

        if not active_services:
            logger.warning("没有已连接的媒体服务器，请检查配置")
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
                    logger.info(f"开始刮削 {item.title} 的演员信息 ...")
                    self.__update_item(server=server, item=item, server_type=service.type)
                    logger.info(f"{item.title} 的演员信息刮削完成")
                logger.info(f"媒体库 {library.name} 的演员信息刮削完成")
            logger.info(f"服务器 {server} 的演员信息刮削完成")

    def __update_peoples(self, server: str, server_type: str,
                         itemid: str, iteminfo: dict, douban_actors):
        # 处理媒体项中的人物信息
        """
        "People": [
            {
              "Name": "丹尼尔·克雷格",
              "Id": "33625",
              "Role": "James Bond",
              "Type": "Actor",
              "PrimaryImageTag": "bef4f764540f10577f804201d8d27918"
            }
        ]
        """
        peoples = []
        is_modified = False
        
        # 更新当前媒体项人物
        for people in iteminfo.get("People", []) or []:
            if self._event.is_set():
                logger.info(f"演职人员刮削服务停止")
                return
            
            # 仅仅跳过无名字的
            if not people.get("Name"):
                continue

            # 调用核心更新逻辑
            info = self.__update_people(server=server, server_type=server_type,
                                        people=people, douban_actors=douban_actors)
            
            if info:
                # 只有返回了新的信息才加入列表（或者被修改了）
                # 注意：__update_people 内部如果发现名字变了，返回的是新的 info
                # 如果仅仅是更新了人物的元数据（简介/生日等），info也会返回
                peoples.append(info)
                is_modified = True
            elif not self._remove_nozh:
                # 没更新但也没被删除
                peoples.append(people)
            else:
                # 被删除了（info为None 且 remove_nozh=True）
                is_modified = True
                logger.info(f"人物 {people.get('Name')} 因非中文且开启了删除选项，已被移除")

        # 保存媒体项信息（如果列表有变化）
        if is_modified and peoples:
            iteminfo["People"] = peoples
            # 这里是更新 Movie/Series 这一层的 People 列表
            # 实际上 __update_people 内部已经更新了 Person 这个实体
            # 但为了确保 Movie 界面显示的列表也是最新的，这里也提交一次
            logger.info(f"正在更新媒体条目 {iteminfo.get('Name')} 的演职员列表...")
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
                # 是否需要处理人物名称
                _peoples = [x for x in _item.get("People", []) if
                            (x.get("Name") and not StringUtils.is_chinese(x.get("Name")))]
            elif self._type == "role":
                # 是否需要处理人物角色
                _peoples = [x for x in _item.get("People", []) if
                            (x.get("Role") and not StringUtils.is_chinese(x.get("Role")))]
            else:
                # 全部
                return True
                
            if _peoples:
                return True
            return False

        # 识别媒体信息
        if not mediainfo:
            if not item.tmdbid:
                logger.warn(f"{item.title} 未找到tmdbid，无法识别媒体信息")
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
            # 获取豆瓣演员信息 (仅作为简介/图片的兜底)
            logger.info(f"开始检查 {item.title} 的演职员信息 ...")
            douban_actors = self.__get_douban_actors(mediainfo=mediainfo, season=season)
            self.__update_peoples(server=server, server_type=server_type,
                                  itemid=item.item_id, iteminfo=iteminfo, douban_actors=douban_actors)
        else:
            logger.info(f"{item.title} 无需更新")

        # 处理季和集人物
        if iteminfo.get("Type") and "Series" in iteminfo["Type"]:
            # 获取季媒体项
            seasons = self.get_items(server=server, server_type=server_type,
                                     parentid=item.item_id, mtype="Season")
            if not seasons:
                logger.warn(f"{item.title} 未找到季媒体项")
                return
            for season in seasons.get("Items", []):
                # 获取豆瓣演员信息
                season_actors = self.__get_douban_actors(mediainfo=mediainfo, season=season.get("IndexNumber"))
                # 如果是Jellyfin，更新季的人物，Emby/Plex季没有人物
                if server_type == "jellyfin":
                    seasoninfo = self.get_iteminfo(server=server, server_type=server_type,
                                                   itemid=season.get("Id"))
                    if not seasoninfo:
                        logger.warn(f"{item.title} 未找到季媒体项：{season.get('Id')}")
                        continue

                    if __need_trans_actor(seasoninfo):
                        # 更新季媒体项人物
                        self.__update_peoples(server=server, server_type=server_type,
                                              itemid=season.get("Id"), iteminfo=seasoninfo,
                                              douban_actors=season_actors)
                        logger.info(f"季 {seasoninfo.get('Id')} 的人物信息更新完成")
                # 获取集媒体项
                episodes = self.get_items(server=server, server_type=server_type,
                                          parentid=season.get("Id"), mtype="Episode")
                if not episodes:
                    logger.warn(f"{item.title} 未找到集媒体项")
                    continue
                # 更新集媒体项人物
                for episode in episodes.get("Items", []):
                    # 获取集媒体项详情
                    episodeinfo = self.get_iteminfo(server=server, server_type=server_type,
                                                    itemid=episode.get("Id"))
                    if not episodeinfo:
                        logger.warn(f"{item.title} 未找到集媒体项：{episode.get('Id')}")
                        continue
                    if __need_trans_actor(episodeinfo):
                        # 更新集媒体项人物
                        self.__update_peoples(server=server, server_type=server_type,
                                              itemid=episode.get("Id"), iteminfo=episodeinfo,
                                              douban_actors=season_actors)
                        logger.info(f"集 {episodeinfo.get('Id')} 的人物信息更新完成")

    def __get_tmdb_person_full(self, person_id: int) -> Optional[dict]:
        """
        获取TMDB人物详细信息，包含external_ids (Imdb, Tvdb)
        """
        if not settings.TMDB_API_KEY:
            logger.error("未配置TMDB API KEY")
            return None
            
        url = f"https://api.themoviedb.org/3/person/{person_id}"
        params = {
            "api_key": settings.TMDB_API_KEY,
            "language": "zh-CN",
            "append_to_response": "external_ids"
        }
        
        logger.info(f"正在请求TMDB人物详情: ID={person_id}, URL={url}")
        try:
            res = RequestUtils(ua=settings.USER_AGENT).get_res(url=url, params=params)
            if res and res.status_code == 200:
                data = res.json()
                logger.info(f"TMDB人物详情请求成功: ID={person_id}")
                logger.debug(f"TMDB返回数据: {json.dumps(data, ensure_ascii=False)}")
                return data
            else:
                logger.error(f"TMDB人物详情请求失败: ID={person_id}, Code={res.status_code if res else 'Unknown'}, Msg={res.text if res else ''}")
        except Exception as e:
            logger.error(f"TMDB人物详情请求异常: {e}")
        return None

    def __update_people(self, server: str, server_type: str,
                        people: dict, douban_actors: list = None) -> Optional[dict]:
        """
        更新人物信息，返回替换后的人物信息
        """

        def __get_peopleid(p: dict) -> Tuple[Optional[str], Optional[str]]:
            """
            获取人物的TMDBID、IMDBID
            """
            if not p.get("ProviderIds"):
                return None, None
            peopletmdbid, peopleimdbid = None, None
            # 兼容不同大小写
            for key in p["ProviderIds"]:
                if key.lower() == "tmdb":
                    peopletmdbid = p["ProviderIds"][key]
                if key.lower() == "imdb":
                    peopleimdbid = p["ProviderIds"][key]
            return peopletmdbid, peopleimdbid

        # 返回的人物信息（用于列表显示）
        ret_people = copy.deepcopy(people)
        
        # 记录是否需要更新
        needs_update = False
        update_fields = []

        try:
            logger.info(f"正在处理人物: {people.get('Name')} (ID: {people.get('Id')})")
            
            # 1. 查询媒体服务器中现有的人物详情
            personinfo = self.get_iteminfo(server=server, server_type=server_type,
                                           itemid=people.get("Id"))
            if not personinfo:
                logger.warn(f"未找到人物 {people.get('Name')} 的媒体库详情，跳过")
                return None
            
            # 初始化 ProviderIds
            if "ProviderIds" not in personinfo:
                personinfo["ProviderIds"] = {}

            # 2. 获取 TMDB ID
            person_tmdbid, person_imdbid = __get_peopleid(personinfo)
            if not person_tmdbid:
                logger.warn(f"人物 {people.get('Name')} 缺少 TMDB ID，无法获取TMDB数据")
                return people # 原样返回

            # 3. 从 TMDB 获取全量数据 (API请求)
            tmdb_data = self.__get_tmdb_person_full(int(person_tmdbid))
            if not tmdb_data:
                logger.warn(f"无法获取 TMDB 数据: {people.get('Name')}")
                return people

            # 4. 解析 TMDB 数据并准备更新字段
            
            # 姓名 (Name) - 强制简中
            tmdb_name = tmdb_data.get("name")
            if tmdb_name:
                if StringUtils.is_chinese(tmdb_name):
                    tmdb_name = zhconv.convert(tmdb_name, "zh-hans")
                
                # 无论是否中文，严格使用 TMDB Name
                if personinfo.get("Name") != tmdb_name:
                    logger.info(f"发现姓名变更: '{personinfo.get('Name')}' -> '{tmdb_name}'")
                    personinfo["Name"] = tmdb_name
                    personinfo["ForcedSortName"] = tmdb_name # 排序名同步
                    personinfo["SortName"] = tmdb_name
                    ret_people["Name"] = tmdb_name # 列表显示名同步
                    needs_update = True
                    update_fields.append("Name")
                else:
                    logger.info(f"姓名一致: {tmdb_name}")

            # 简介 (Overview) - 优先 TMDB，其次豆瓣
            tmdb_bio = tmdb_data.get("biography")
            new_overview = ""
            overview_source = "None"
            
            if tmdb_bio:
                new_overview = zhconv.convert(tmdb_bio, "zh-hans") if StringUtils.is_chinese(tmdb_bio) else tmdb_bio
                overview_source = "TMDB"
            
            # 如果TMDB简介为空，尝试查找豆瓣
            if not new_overview and douban_actors:
                logger.info("TMDB简介为空，尝试搜索豆瓣数据...")
                for db_actor in douban_actors:
                    # 尝试匹配名字
                    if (db_actor.get("latin_name") and db_actor.get("latin_name") == tmdb_name) \
                            or (db_actor.get("name") and db_actor.get("name") == tmdb_name) \
                            or (db_actor.get("name") == people.get("Name")):
                        if db_actor.get("title"): # 豆瓣的title字段通常是简介或相关描述
                             new_overview = db_actor.get("title")
                             overview_source = "Douban"
                             break
            
            if new_overview and personinfo.get("Overview") != new_overview:
                logger.info(f"发现简介变更 (来源: {overview_source}): 更新前长度 {len(personinfo.get('Overview') or '')}, 更新后长度 {len(new_overview)}")
                personinfo["Overview"] = new_overview
                needs_update = True
                update_fields.append("Overview")

            # 出生日期 (BirthDate / PremiereDate)
            tmdb_birth = tmdb_data.get("birthday")
            if tmdb_birth:
                # 格式化为 ISO (Emby需要)
                try:
                    # TMDB返回 YYYY-MM-DD
                    birth_dt = datetime.datetime.strptime(tmdb_birth, "%Y-%m-%d")
                    # Emby 格式: 1974-01-30T00:00:00.0000000Z
                    emby_birth = birth_dt.strftime("%Y-%m-%dT00:00:00.0000000Z")
                    
                    # 检查 Emby 现有日期 (PremiereDate)
                    if personinfo.get("PremiereDate") != emby_birth:
                        logger.info(f"发现出生日期变更: {personinfo.get('PremiereDate')} -> {emby_birth}")
                        personinfo["PremiereDate"] = emby_birth
                        needs_update = True
                        update_fields.append("PremiereDate")
                except Exception as e:
                    logger.warn(f"日期解析失败: {tmdb_birth}, Error: {e}")

            # 逝世日期 (DeathDate / EndDate)
            tmdb_death = tmdb_data.get("deathday")
            if tmdb_death:
                try:
                    death_dt = datetime.datetime.strptime(tmdb_death, "%Y-%m-%d")
                    emby_death = death_dt.strftime("%Y-%m-%dT00:00:00.0000000Z")
                    if personinfo.get("EndDate") != emby_death:
                        logger.info(f"发现逝世日期变更: {personinfo.get('EndDate')} -> {emby_death}")
                        personinfo["EndDate"] = emby_death
                        needs_update = True
                        update_fields.append("EndDate")
                except Exception:
                    pass

            # 出生地点 (Place of Birth / ProductionLocations)
            tmdb_place = tmdb_data.get("place_of_birth")
            if tmdb_place:
                # Emby 使用列表存储 ProductionLocations
                current_locs = personinfo.get("ProductionLocations", [])
                if not current_locs or current_locs[0] != tmdb_place:
                     logger.info(f"发现出生地变更: {current_locs} -> {[tmdb_place]}")
                     personinfo["ProductionLocations"] = [tmdb_place]
                     needs_update = True
                     update_fields.append("ProductionLocations")

            # 外部标识符 (ProviderIds: Imdb, Tmdb, Tvdb)
            ext_ids = tmdb_data.get("external_ids", {})
            new_imdb_id = ext_ids.get("imdb_id")
            new_tvdb_id = ext_ids.get("tvdb_id")
            
            # 更新 Imdb
            if new_imdb_id and personinfo["ProviderIds"].get("Imdb") != new_imdb_id:
                logger.info(f"更新 Imdb ID: {new_imdb_id}")
                personinfo["ProviderIds"]["Imdb"] = new_imdb_id
                needs_update = True
                update_fields.append("ProviderIds")
            
            # 更新 Tvdb
            if new_tvdb_id:
                # Tvdb可能是int
                new_tvdb_id = str(new_tvdb_id)
                if personinfo["ProviderIds"].get("Tvdb") != new_tvdb_id:
                    logger.info(f"更新 Tvdb ID: {new_tvdb_id}")
                    personinfo["ProviderIds"]["Tvdb"] = new_tvdb_id
                    needs_update = True
                    update_fields.append("ProviderIds")

            # 锁定字段 (防止Emby自动刷新覆盖)
            if "LockedFields" not in personinfo:
                personinfo["LockedFields"] = []
            
            for field in ["Name", "Overview", "PremiereDate", "EndDate", "ProductionLocations", "OfficialRating"]:
                if field not in personinfo["LockedFields"]:
                    personinfo["LockedFields"].append(field)
                    # 锁定字段通常不需要视为数据变更，但为了保险起见，如果改变了锁定状态也提交
            
            # 5. 处理图片 (Profile)
            profile_path = None
            image_source = "None"
            
            # 优先 TMDB
            if tmdb_data.get("profile_path"):
                profile_path = f"https://{settings.TMDB_IMAGE_DOMAIN}/t/p/original{tmdb_data.get('profile_path')}"
                image_source = "TMDB"
            
            # 其次 豆瓣
            if not profile_path and douban_actors:
                for db_actor in douban_actors:
                     if (db_actor.get("latin_name") and db_actor.get("latin_name") == tmdb_name) \
                            or (db_actor.get("name") and db_actor.get("name") == tmdb_name):
                         avatar = db_actor.get("avatar") or {}
                         if avatar.get("large"):
                             profile_path = avatar.get("large")
                             image_source = "Douban"
                             break
            
            # 提交图片更新
            if profile_path:
                logger.info(f"正在更新图片 (来源: {image_source}): {profile_path}")
                self.set_item_image(server=server, server_type=server_type, 
                                    itemid=people.get("Id"), imageurl=profile_path)

            # 6. 提交元数据更新
            if needs_update:
                logger.info(f"提交人物 {tmdb_name} 的元数据更新, 字段: {update_fields}")
                logger.debug(f"更新Payload: {json.dumps(personinfo, ensure_ascii=False)}")
                
                ret = self.set_iteminfo(server=server, server_type=server_type,
                                        itemid=people.get("Id"), iteminfo=personinfo)
                if ret:
                    logger.info(f"人物 {tmdb_name} 更新成功!")
                    return ret_people
                else:
                    logger.error(f"人物 {tmdb_name} 更新失败!")
            else:
                logger.info(f"人物 {tmdb_name} 无需更新元数据")

        except Exception as err:
            logger.error(f"更新人物信息发生未捕获异常: {str(err)}")
            import traceback
            logger.error(traceback.format_exc())
        
        return None

    def __get_douban_actors(self, mediainfo: MediaInfo, season: int = None) -> List[dict]:
        """
        获取豆瓣演员信息
        """
        # 随机休眠 3-10 秒
        sleep_time = 3 + int(time.time()) % 7
        logger.info(f"为防止触发反爬，随机休眠 {sleep_time}秒 ...")
        time.sleep(sleep_time)
        # 匹配豆瓣信息
        doubaninfo = self.chain.match_doubaninfo(name=mediainfo.title,
                                                 imdbid=mediainfo.imdb_id,
                                                 mtype=mediainfo.type,
                                                 year=mediainfo.year,
                                                 season=season)
        # 豆瓣演员
        if doubaninfo:
            logger.info(f"已匹配到豆瓣信息 ID: {doubaninfo.get('id')}")
            doubanitem = self.chain.douban_info(doubaninfo.get("id")) or {}
            actors = (doubanitem.get("actors") or []) + (doubanitem.get("directors") or [])
            logger.info(f"获取到豆瓣演职人员共 {len(actors)} 人")
            return actors
        else:
            logger.warn(f"未找到豆瓣信息：{mediainfo.title_year}")
        return []

    def get_iteminfo(self, server: str, server_type: str, itemid: str) -> dict:
        """
        获得媒体项详情
        """

        service = self.service_infos(server_type).get(server)
        if not service:
            logger.warn(f"未找到媒体服务器 {server} 的实例")
            return {}

        def __get_emby_iteminfo() -> dict:
            """
            获得Emby媒体项详情
            """
            try:
                # 增加 Fields 确保获取ProviderIds等详细信息
                url = f'[HOST]emby/Users/[USER]/Items/{itemid}?' \
                      f'Fields=ChannelMappingInfo,ProviderIds,ProductionLocations,OfficialRating,PremiereDate,EndDate,Overview&api_key=[APIKEY]'
                res = service.instance.get_data(url=url)
                if res:
                    return res.json()
            except Exception as err:
                logger.error(f"获取Emby媒体项详情失败：{str(err)}")
            return {}

        def __get_jellyfin_iteminfo() -> dict:
            """
            获得Jellyfin媒体项详情
            """
            try:
                url = f'[HOST]Users/[USER]/Items/{itemid}?Fields=ChannelMappingInfo,ProviderIds,ProductionLocations,OfficialRating,PremiereDate,EndDate,Overview&api_key=[APIKEY]'
                res = service.instance.get_data(url=url)
                if res:
                    result = res.json()
                    if result:
                        result['FileName'] = Path(result['Path']).name if result.get('Path') else ""
                    return result
            except Exception as err:
                logger.error(f"获取Jellyfin媒体项详情失败：{str(err)}")
            return {}

        def __get_plex_iteminfo() -> dict:
            """
            获得Plex媒体项详情
            """
            iteminfo = {}
            try:
                plexitem = service.instance.get_plex().library.fetchItem(ekey=itemid)
                if 'movie' in plexitem.METADATA_TYPE:
                    iteminfo['Type'] = 'Movie'
                    iteminfo['IsFolder'] = False
                elif 'episode' in plexitem.METADATA_TYPE:
                    iteminfo['Type'] = 'Series'
                    iteminfo['IsFolder'] = False
                    if 'show' in plexitem.TYPE:
                        iteminfo['ChildCount'] = plexitem.childCount
                iteminfo['Name'] = plexitem.title
                iteminfo['Id'] = plexitem.key
                iteminfo['ProductionYear'] = plexitem.year
                iteminfo['ProviderIds'] = {}
                for guid in plexitem.guids:
                    idlist = str(guid.id).split(sep='://')
                    if len(idlist) < 2:
                        continue
                    iteminfo['ProviderIds'][idlist[0]] = idlist[1]
                for location in plexitem.locations:
                    iteminfo['Path'] = location
                    iteminfo['FileName'] = Path(location).name
                iteminfo['Overview'] = plexitem.summary
                iteminfo['CommunityRating'] = plexitem.audienceRating
                return iteminfo
            except Exception as err:
                logger.error(f"获取Plex媒体项详情失败：{str(err)}")
            return {}

        if server_type == "emby":
            return __get_emby_iteminfo()
        elif server_type == "jellyfin":
            return __get_jellyfin_iteminfo()
        else:
            return __get_plex_iteminfo()

    def get_items(self, server: str, server_type: str, parentid: str, mtype: str = None) -> dict:
        """
        获得媒体的所有子媒体项
        """
        service = self.service_infos(server_type).get(server)
        if not service:
            logger.warn(f"未找到媒体服务器 {server} 的实例")
            return {}

        def __get_emby_items() -> dict:
            """
            获得Emby媒体的所有子媒体项
            """
            try:
                if parentid:
                    url = f'[HOST]emby/Users/[USER]/Items?ParentId={parentid}&api_key=[APIKEY]'
                else:
                    url = '[HOST]emby/Users/[USER]/Items?api_key=[APIKEY]'
                res = service.instance.get_data(url=url)
                if res:
                    return res.json()
            except Exception as err:
                logger.error(f"获取Emby媒体的所有子媒体项失败：{str(err)}")
            return {}

        def __get_jellyfin_items() -> dict:
            """
            获得Jellyfin媒体的所有子媒体项
            """
            try:
                if parentid:
                    url = f'[HOST]Users/[USER]/Items?ParentId={parentid}&api_key=[APIKEY]'
                else:
                    url = '[HOST]Users/[USER]/Items?api_key=[APIKEY]'
                res = service.instance.get_data(url=url)
                if res:
                    return res.json()
            except Exception as err:
                logger.error(f"获取Jellyfin媒体的所有子媒体项失败：{str(err)}")
            return {}

        def __get_plex_items() -> dict:
            """
            获得Plex媒体的所有子媒体项
            """
            items = {}
            try:
                plex = service.instance.get_plex()
                items['Items'] = []
                if parentid:
                    if mtype and 'Season' in mtype:
                        plexitem = plex.library.fetchItem(ekey=parentid)
                        items['Items'] = []
                        for season in plexitem.seasons():
                            item = {
                                'Name': season.title,
                                'Id': season.key,
                                'IndexNumber': season.seasonNumber,
                                'Overview': season.summary
                            }
                            items['Items'].append(item)
                    elif mtype and 'Episode' in mtype:
                        plexitem = plex.library.fetchItem(ekey=parentid)
                        items['Items'] = []
                        for episode in plexitem.episodes():
                            item = {
                                'Name': episode.title,
                                'Id': episode.key,
                                'IndexNumber': episode.episodeNumber,
                                'Overview': episode.summary,
                                'CommunityRating': episode.audienceRating
                            }
                            items['Items'].append(item)
                    else:
                        plexitems = plex.library.sectionByID(sectionID=parentid)
                        for plexitem in plexitems.all():
                            item = {}
                            if 'movie' in plexitem.METADATA_TYPE:
                                item['Type'] = 'Movie'
                                item['IsFolder'] = False
                            elif 'episode' in plexitem.METADATA_TYPE:
                                item['Type'] = 'Series'
                                item['IsFolder'] = False
                            item['Name'] = plexitem.title
                            item['Id'] = plexitem.key
                            items['Items'].append(item)
                else:
                    plexitems = plex.library.sections()
                    for plexitem in plexitems:
                        item = {}
                        if 'Directory' in plexitem.TAG:
                            item['Type'] = 'Folder'
                            item['IsFolder'] = True
                        elif 'movie' in plexitem.METADATA_TYPE:
                            item['Type'] = 'Movie'
                            item['IsFolder'] = False
                        elif 'episode' in plexitem.METADATA_TYPE:
                            item['Type'] = 'Series'
                            item['IsFolder'] = False
                        item['Name'] = plexitem.title
                        item['Id'] = plexitem.key
                        items['Items'].append(item)
                return items
            except Exception as err:
                logger.error(f"获取Plex媒体的所有子媒体项失败：{str(err)}")
            return {}

        if server_type == "emby":
            return __get_emby_items()
        elif server_type == "jellyfin":
            return __get_jellyfin_items()
        else:
            return __get_plex_items()

    def set_iteminfo(self, server: str, server_type: str, itemid: str, iteminfo: dict):
        """
        更新媒体项详情
        """

        service = self.service_infos(server_type).get(server)
        if not service:
            logger.warn(f"未找到媒体服务器 {server} 的实例")
            return {}

        def __set_emby_iteminfo():
            """
            更新Emby媒体项详情
            """
            try:
                url = f'[HOST]emby/Items/{itemid}?api_key=[APIKEY]&reqformat=json'
                logger.info(f"正在发送Emby更新请求: {itemid}")
                res = service.instance.post_data(
                    url=url,
                    data=json.dumps(iteminfo),
                    headers={
                        "Content-Type": "application/json"
                    }
                )
                if res and res.status_code in [200, 204]:
                    logger.info(f"Emby更新成功: {itemid}")
                    return True
                else:
                    logger.error(f"更新Emby媒体项详情失败，错误码：{res.status_code}，响应：{res.text}")
                    return False
            except Exception as err:
                logger.error(f"更新Emby媒体项详情失败：{str(err)}")
            return False

        def __set_jellyfin_iteminfo():
            """
            更新Jellyfin媒体项详情
            """
            try:
                res = service.instance.post_data(
                    url=f'[HOST]Items/{itemid}?api_key=[APIKEY]',
                    data=json.dumps(iteminfo),
                    headers={
                        "Content-Type": "application/json"
                    }
                )
                if res and res.status_code in [200, 204]:
                    logger.info(f"Jellyfin更新成功: {itemid}")
                    return True
                else:
                    logger.error(f"更新Jellyfin媒体项详情失败，错误码：{res.status_code}")
                    return False
            except Exception as err:
                logger.error(f"更新Jellyfin媒体项详情失败：{str(err)}")
            return False

        def __set_plex_iteminfo():
            """
            更新Plex媒体项详情
            """
            try:
                plexitem = service.instance.get_plex().library.fetchItem(ekey=itemid)
                if 'CommunityRating' in iteminfo:
                    edits = {
                        'audienceRating.value': iteminfo['CommunityRating'],
                        'audienceRating.locked': 1
                    }
                    plexitem.edit(**edits)
                plexitem.editTitle(iteminfo['Name']).editSummary(iteminfo['Overview']).reload()
                return True
            except Exception as err:
                logger.error(f"更新Plex媒体项详情失败：{str(err)}")
            return False

        if server_type == "emby":
            return __set_emby_iteminfo()
        elif server_type == "jellyfin":
            return __set_jellyfin_iteminfo()
        else:
            return __set_plex_iteminfo()

    @retry(RequestException, logger=logger)
    def set_item_image(self, server: str, server_type: str, itemid: str, imageurl: str):
        """
        更新媒体项图片
        """

        service = self.service_infos(server_type).get(server)
        if not service:
            logger.warn(f"未找到媒体服务器 {server} 的实例")
            return {}

        def __download_image():
            """
            下载图片
            """
            try:
                logger.info(f"正在下载图片: {imageurl}")
                if "doubanio.com" in imageurl:
                    r = RequestUtils(headers={
                        'Referer': "https://movie.douban.com/"
                    }, ua=settings.USER_AGENT).get_res(url=imageurl, raise_exception=True)
                else:
                    r = RequestUtils(proxies=settings.PROXY,
                                     ua=settings.USER_AGENT).get_res(url=imageurl, raise_exception=True)
                if r:
                    logger.info("图片下载成功")
                    return base64.b64encode(r.content).decode()
                else:
                    logger.warn(f"{imageurl} 图片下载失败，请检查网络连通性")
            except Exception as err:
                logger.error(f"下载图片失败：{str(err)}")
            return None

        def __set_emby_item_image(_base64: str):
            """
            更新Emby媒体项图片
            """
            try:
                url = f'[HOST]emby/Items/{itemid}/Images/Primary?api_key=[APIKEY]'
                logger.info(f"正在向Emby上传图片: {itemid}")
                res = service.instance.post_data(
                    url=url,
                    data=_base64,
                    headers={
                        "Content-Type": "image/png"
                    }
                )
                if res and res.status_code in [200, 204]:
                    logger.info("Emby图片上传成功")
                    return True
                else:
                    logger.error(f"更新Emby媒体项图片失败，错误码：{res.status_code}")
                    return False
            except Exception as result:
                logger.error(f"更新Emby媒体项图片失败：{result}")
            return False

        def __set_jellyfin_item_image():
            """
            更新Jellyfin媒体项图片
            """
            try:
                url = f'[HOST]Items/{itemid}/RemoteImages/Download?' \
                      f'Type=Primary&ImageUrl={imageurl}&ProviderName=TheMovieDb&api_key=[APIKEY]'
                res = service.instance.post_data(url=url)
                if res and res.status_code in [200, 204]:
                    return True
                elif res is not None:
                    logger.error(f"更新Jellyfin媒体项图片失败，错误码：{res.status_code}")
                    return False
                else:
                    logger.error(f"更新Jellyfin媒体项图片失败，返回结果为空")
                    return False
            except Exception as err:
                logger.error(f"更新Jellyfin媒体项图片失败：{err}")
            return False

        def __set_plex_item_image():
            """
            更新Plex媒体项图片
            """
            try:
                plexitem = service.instance.get_plex().library.fetchItem(ekey=itemid)
                plexitem.uploadPoster(url=imageurl)
                return True
            except Exception as err:
                logger.error(f"更新Plex媒体项图片失败：{err}")
            return False

        if server_type == "emby":
            # 下载图片获取base64
            image_base64 = __download_image()
            if image_base64:
                return __set_emby_item_image(image_base64)
        elif server_type == "jellyfin":
            return __set_jellyfin_item_image()
        else:
            return __set_plex_item_image()
        return None

    def stop_service(self):
        """
        停止服务
        """
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
