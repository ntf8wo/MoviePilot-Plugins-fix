import base64
import copy
import datetime
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    plugin_desc = "混合策略(豆瓣/TMDB)，支持社交ID同步，智能补全缺失图片/简介，多线程保序处理。"
    # 插件图标
    plugin_icon = "actor.png"
    # 插件版本
    plugin_version = "2.6.0_ordered_concurrent"
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
    _lock_info = False
    _mediaservers = []
    
    # 简单的内存缓存，减少重复请求 (key=tmdb_id, value=data)
    _tmdb_cache = {}

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._cron = config.get("cron")
            self._type = config.get("type") or "all"
            self._delay = config.get("delay") or 0
            self._remove_nozh = config.get("remove_nozh") or False
            self._lock_info = config.get("lock_info") or False
            self._mediaservers = config.get("mediaservers") or []

        # 停止现有任务
        self.stop_service()
        
        # 清空缓存
        self._tmdb_cache = {}

        # 启动服务
        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(func=self.scrap_library, trigger='date',
                                    run_date=datetime.datetime.now(
                                        tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3)
                                    )
            logger.info(f"演职人员刮削(自由版)服务启动，立即运行一次")
            self._onlyonce = False
            self.__update_config()
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __update_config(self):
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
        if self._enabled and self._cron:
            return [{
                "id": "personmetamod",
                "name": "演职人员刮削服务(自由版)",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.scrap_library,
                "kwargs": {}
            }]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol',
                        'props': {'cols': 12, 'md': 6},
                        'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]
                    },
                    {
                        'component': 'VCol',
                        'props': {'cols': 12, 'md': 6},
                        'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}]
                    }
                ]
            },
            {
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol',
                        'props': {'cols': 12, 'md': 4},
                        'content': [{'component': 'VCronField', 'props': {'model': 'cron', 'label': '媒体库扫描周期', 'placeholder': '5位cron表达式'}}]
                    },
                    {
                        'component': 'VCol',
                        'props': {'cols': 12, 'md': 4},
                        'content': [{'component': 'VTextField', 'props': {'model': 'delay', 'label': '入库延迟时间（秒）', 'placeholder': '30'}}]
                    },
                    {
                        'component': 'VCol',
                        'props': {'cols': 12, 'md': 4},
                        'content': [
                            {
                                'component': 'VSelect',
                                'props': {
                                    'model': 'type',
                                    'label': '刮削触发条件',
                                    'items': [
                                        {'title': '全部 (推荐)', 'value': 'all'},
                                        {'title': '演员非中文', 'value': 'name'},
                                        {'title': '角色非中文', 'value': 'role'},
                                    ],
                                    'hint': '除了语言判断，缺图/缺简介的演员也会被自动处理。'
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
                        'props': {'cols': 12},
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
                        'props': {'cols': 12, 'md': 6},
                        'content': [{'component': 'VSwitch', 'props': {'model': 'remove_nozh', 'label': '删除非中文演员'}}]
                    },
                    {
                        'component': 'VCol',
                        'props': {'cols': 12, 'md': 6},
                        'content': [
                            {
                                'component': 'VSwitch',
                                'props': {
                                    'model': 'lock_info',
                                    'label': '锁定元数据 (可选)',
                                    'hint': '开启后，修改过的信息(姓名/简介)将被锁定，防止被NFO/刷新覆盖。建议保持关闭。',
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
            "lock_info": False,
            "mediaservers": []
        }

    def get_page(self) -> List[dict]:
        pass

    def service_infos(self, type_filter: Optional[str] = None) -> Optional[Dict[str, ServiceInfo]]:
        if not self._mediaservers:
            return None
        services = MediaServerHelper().get_services(type_filter=type_filter, name_filters=self._mediaservers)
        if not services:
            return None
        active_services = {}
        for service_name, service_info in services.items():
            if service_info.type == 'plex':
                continue
            if service_info.instance.is_inactive():
                pass
            else:
                active_services[service_name] = service_info
        return active_services

    @eventmanager.register(EventType.TransferComplete)
    def scrap_rt(self, event: Event):
        if not self._enabled:
            return
        if not event or not event.event_data:
            return
        mediainfo: MediaInfo = event.event_data.get("mediainfo")
        meta: MetaBase = event.event_data.get("meta")
        if not mediainfo or not meta:
            return
        if self._delay:
            time.sleep(int(self._delay))
        existsinfo = self.chain.media_exists(mediainfo=mediainfo)
        if not existsinfo or not existsinfo.itemid:
            return
        if existsinfo.server_type == 'plex':
            return
        iteminfo = MediaServerChain().iteminfo(server=existsinfo.server, item_id=existsinfo.itemid)
        if not iteminfo:
            return
        self.__update_item(server=existsinfo.server, server_type=existsinfo.server_type,
                           item=iteminfo, mediainfo=mediainfo, season=meta.begin_season)

    def scrap_library(self):
        service_infos = self.service_infos()
        if not service_infos:
            return
        mediaserverchain = MediaServerChain()
        for server, service in service_infos.items():
            logger.info(f"开始刮削服务器 {server} 的演员信息 ...")
            for library in mediaserverchain.librarys(server):
                logger.info(f"开始刮削媒体库 {library.name} 的演员信息 ...")
                for item in mediaserverchain.items(server, library.id):
                    if not item or not item.item_id:
                        continue
                    if "Series" not in item.item_type and "Movie" not in item.item_type:
                        continue
                    if self._event.is_set():
                        return
                    self.__update_item(server=server, item=item, server_type=service.type)
                logger.info(f"媒体库 {library.name} 的演员信息刮削完成")
            logger.info(f"服务器 {server} 的演员信息刮削完成")
            # 扫描完一个服务器清理一次缓存
            self._tmdb_cache.clear()

    def __update_peoples(self, server: str, server_type: str,
                         itemid: str, iteminfo: dict, douban_actors):
        """
        并发处理演员信息，但严格保持原始顺序 (Index-based)
        """
        people_list = iteminfo.get("People", []) or []
        if not people_list:
            return

        # 1. 初始化结果占位列表 (长度与原始列表一致)
        # 默认值为 None，表示该位置待处理
        final_peoples = [None] * len(people_list)
        
        # 并发池
        max_workers = 5
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_index = {}
            
            for idx, people in enumerate(people_list):
                if self._event.is_set():
                    return
                
                # 如果没名字，直接保留原样，填入结果表
                if not people.get("Name"):
                    final_peoples[idx] = people
                    continue

                # --- 智能跳过逻辑 ---
                has_image = people.get("PrimaryImageTag") is not None
                is_zh_name = StringUtils.is_chinese(people.get("Name"))
                is_zh_role = StringUtils.is_chinese(people.get("Role"))
                
                # 只有：名字中文 + 角色中文 + 有图，才跳过 (完全满意)
                if is_zh_name and is_zh_role and has_image:
                    final_peoples[idx] = people
                    continue
                
                # 否则提交任务，并记录 Index
                future = executor.submit(self.__update_people, server, server_type, people, douban_actors)
                future_to_index[future] = idx

            # 2. 回收结果，填入对应的坑位
            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                original_data = people_list[idx]
                try:
                    updated_data = future.result()
                    if updated_data:
                        final_peoples[idx] = updated_data
                    else:
                        # 如果处理失败或返回None，根据配置决定去留
                        if not self._remove_nozh:
                            final_peoples[idx] = original_data
                        else:
                            # 如果开启了删除非中文，且处理失败，这里留 None
                            # 后续清理步骤会把 None 删掉
                            pass
                except Exception as e:
                    logger.error(f"处理人物 {original_data.get('Name')} 异常: {e}")
                    if not self._remove_nozh:
                        final_peoples[idx] = original_data

        # 3. 清理结果 (去除 None)
        # 如果 _remove_nozh=False，上面都会有兜底值，除了本来就是None的情况
        valid_peoples = [p for p in final_peoples if p is not None]

        # 4. 提交保存 (此时 valid_peoples 的顺序与 iteminfo['People'] 严格一致)
        if valid_peoples:
            iteminfo["People"] = valid_peoples
            self.set_iteminfo(server=server, server_type=server_type,
                              itemid=itemid, iteminfo=iteminfo)

    def __update_item(self, server: str, item: MediaServerItem, server_type: str = None,
                      mediainfo: MediaInfo = None, season: int = None):
        
        # 内部判断：是否需要处理
        def __need_trans_actor(_item):
            people_list = _item.get("People", []) or []
            for x in people_list:
                name = x.get("Name")
                role = x.get("Role")
                if not name:
                    continue
                
                # 1. 检查缺失项：没有图片，强制处理
                if not x.get("PrimaryImageTag"):
                    return True
                
                # 2. 检查语言项
                if self._type == "name" and not StringUtils.is_chinese(name):
                    return True
                if self._type == "role" and role and not StringUtils.is_chinese(role):
                    return True
                if self._type == "all":
                    if not StringUtils.is_chinese(name): return True
                    if role and not StringUtils.is_chinese(role): return True
            return False

        if not mediainfo:
            if not item.tmdbid:
                return
            mtype = MediaType.TV if item.item_type in ['Series', 'show'] else MediaType.MOVIE
            mediainfo = self.chain.recognize_media(mtype=mtype, tmdbid=item.tmdbid)
            if not mediainfo:
                return

        iteminfo = self.get_iteminfo(server=server, server_type=server_type, itemid=item.item_id)
        if not iteminfo:
            return

        douban_actors = []
        if __need_trans_actor(iteminfo):
            douban_actors = self.__get_douban_actors(mediainfo=mediainfo, season=season)
            self.__update_peoples(server=server, server_type=server_type,
                                  itemid=item.item_id, iteminfo=iteminfo, douban_actors=douban_actors)

        # 处理 Series
        if iteminfo.get("Type") and "Series" in iteminfo["Type"]:
            seasons = self.get_items(server=server, server_type=server_type,
                                     parentid=item.item_id, mtype="Season")
            if not seasons:
                return
            
            for season_item in seasons.get("Items", []):
                season_num = season_item.get("IndexNumber")
                # 按季获取豆瓣 (缓存或新请求)
                season_actors = self.__get_douban_actors(mediainfo=mediainfo, season=season_num)
                
                # Jellyfin 季处理
                if server_type == "jellyfin":
                    seasoninfo = self.get_iteminfo(server=server, server_type=server_type, itemid=season_item.get("Id"))
                    if seasoninfo and __need_trans_actor(seasoninfo):
                        self.__update_peoples(server=server, server_type=server_type,
                                              itemid=season_item.get("Id"), iteminfo=seasoninfo,
                                              douban_actors=season_actors)
                
                # Episode 处理
                episodes = self.get_items(server=server, server_type=server_type,
                                          parentid=season_item.get("Id"), mtype="Episode")
                if not episodes:
                    continue
                
                for episode in episodes.get("Items", []):
                    episodeinfo = self.get_iteminfo(server=server, server_type=server_type, itemid=episode.get("Id"))
                    if episodeinfo and __need_trans_actor(episodeinfo):
                        self.__update_peoples(server=server, server_type=server_type,
                                              itemid=episode.get("Id"), iteminfo=episodeinfo,
                                              douban_actors=season_actors)

    def __get_tmdb_extra_info(self, tmdb_id: str) -> Tuple[Optional[dict], Optional[dict]]:
        if not settings.TMDB_API_KEY or not tmdb_id:
            return None, None
        
        # 查缓存
        if tmdb_id in self._tmdb_cache:
            return self._tmdb_cache[tmdb_id]

        # 3次重试机制
        retry_count = 3
        for i in range(retry_count):
            try:
                base_url = "https://api.themoviedb.org/3"
                tmdb_domain = getattr(settings, "TMDB_DOMAIN", None)
                if tmdb_domain: 
                    base_url = f"https://{tmdb_domain}/3"
                
                url = f"{base_url}/person/{tmdb_id}?api_key={settings.TMDB_API_KEY}&language=zh-CN&append_to_response=external_ids"
                
                res = RequestUtils(proxies=settings.PROXY, ua=settings.USER_AGENT).get_res(url=url)
                if res and res.status_code == 200:
                    data = res.json()
                    external_ids = data.get("external_ids", {})
                    # 写入缓存
                    self._tmdb_cache[tmdb_id] = (data, external_ids)
                    return data, external_ids
                elif res and res.status_code == 429:
                    time.sleep(2)
            except Exception as e:
                logger.debug(f"请求TMDB失败(第{i+1}次): {e}")
                time.sleep(1)
        
        return None, None

    def __update_people(self, server: str, server_type: str,
                        people: dict, douban_actors: list = None) -> Optional[dict]:
        original_name = people.get("Name")

        def __get_peopleid(p: dict) -> str:
            if not p.get("ProviderIds"):
                return None
            pid = p["ProviderIds"]
            return pid.get("Tmdb") or pid.get("tmdb")

        def __to_zh_cn(text: str) -> str:
            if not text:
                return text
            return zhconv.convert(text, 'zh-cn')

        ret_people = copy.deepcopy(people)

        try:
            personinfo = self.get_iteminfo(server=server, server_type=server_type, itemid=people.get("Id"))
            if not personinfo:
                return None

            updated_global = False
            final_name, final_overview, final_img = None, None, None
            tmdb_name_cn, tmdb_name_en, tmdb_overview_cn, tmdb_overview_en, tmdb_img = None, None, None, None, None
            tmdb_external_ids = {}
            
            # --- TMDB 获取 ---
            person_tmdbid = __get_peopleid(personinfo)
            if person_tmdbid:
                tmdb_details, tmdb_ext_ids = self.__get_tmdb_extra_info(person_tmdbid)
                if tmdb_details:
                    tmdb_external_ids = tmdb_ext_ids or {}
                    _path = tmdb_details.get("profile_path")
                    if _path:
                        tmdb_img = f"https://{settings.TMDB_IMAGE_DOMAIN}/t/p/original{_path}"
                    _name = tmdb_details.get("name")
                    if _name:
                        if StringUtils.is_chinese(_name): tmdb_name_cn = _name
                        else: tmdb_name_en = _name
                    _bio = tmdb_details.get("biography")
                    if _bio:
                        if StringUtils.is_chinese(_bio): tmdb_overview_cn = _bio
                        else: tmdb_overview_en = _bio

            # --- 豆瓣匹配 ---
            douban_match = None
            if douban_actors:
                for douban_actor in douban_actors:
                    is_match = False
                    current_name = people.get("Name")
                    if douban_actor.get("latin_name") == current_name or douban_actor.get("name") == current_name:
                        is_match = True
                    elif tmdb_name_cn and douban_actor.get("name") == tmdb_name_cn:
                        is_match = True
                    
                    if is_match:
                        douban_match = douban_actor
                        break

            # --- 决策 Name ---
            douban_name = douban_match.get("name") if douban_match else None
            if douban_name and StringUtils.is_chinese(douban_name):
                final_name = douban_name
            elif tmdb_name_cn:
                final_name = tmdb_name_cn
            elif tmdb_name_en:
                final_name = tmdb_name_en
            
            if final_name:
                final_name = __to_zh_cn(final_name)

            # --- 决策 Overview ---
            if tmdb_overview_cn:
                final_overview = tmdb_overview_cn
            elif tmdb_overview_en:
                final_overview = tmdb_overview_en
            elif douban_match:
                final_overview = douban_match.get("summary") or douban_match.get("intro") or douban_match.get("biography")

            if final_overview:
                final_overview = __to_zh_cn(final_overview)

            # --- 决策 Image ---
            has_local_img = people.get("PrimaryImageTag") is not None
            
            img_source = "None"
            if douban_match:
                avatar = douban_match.get("avatar")
                if isinstance(avatar, dict) and avatar.get("large"):
                    final_img = avatar.get("large")
                    img_source = "Douban"
                elif isinstance(avatar, str) and avatar:
                    final_img = avatar
                    img_source = "Douban"
            
            if not final_img and tmdb_img:
                final_img = tmdb_img
                img_source = "TMDB"

            # --- 决策 Role ---
            final_role = None
            if douban_match and douban_match.get("character"):
                raw_char = douban_match.get("character")
                cleaned_role = re.sub(r"饰\s*|演员\s*|配音\s*", "", raw_char).strip()
                blacklist_roles = ["配音", "配音演员", "声优", "演员", "Voice", "Actor", "Guest", "Self", "Himself", "Herself"]
                if cleaned_role and cleaned_role not in blacklist_roles:
                    final_role = __to_zh_cn(cleaned_role)

            # --- 更新操作 ---

            # 1. External IDs
            id_mapping = {"imdb_id": "Imdb", "facebook_id": "Facebook", "instagram_id": "Instagram", "twitter_id": "Twitter"}
            current_pids = personinfo.get("ProviderIds", {})
            pids_updated = False
            for tmdb_k, emby_k in id_mapping.items():
                val = tmdb_external_ids.get(tmdb_k)
                if val and str(val) != str(current_pids.get(emby_k, "")):
                    current_pids[emby_k] = str(val)
                    pids_updated = True
            
            if pids_updated:
                personinfo["ProviderIds"] = current_pids
                updated_global = True

            # 2. Global Info
            if final_name and final_name != personinfo.get("Name"):
                personinfo["Name"] = final_name
                updated_global = True
            
            if final_overview and final_overview != personinfo.get("Overview"):
                personinfo["Overview"] = final_overview
                updated_global = True

            # 3. Image (Retry included inside set_item_image)
            # 策略：如果没图，则下载。如果本来就有图，通常不覆盖（除非你希望强制覆盖，可删掉 not has_local_img）
            if final_img and not has_local_img:
                logger.info(f"补全图片 [{img_source}]: {final_name}")
                if self.set_item_image(server=server, server_type=server_type, itemid=people.get("Id"), imageurl=final_img):
                    ret_people["PrimaryImageTag"] = "new" 
                else:
                    logger.warn(f"图片下载失败: {final_name}")

            # 4. Lock Info
            if self._lock_info and updated_global:
                if "LockedFields" not in personinfo: personinfo["LockedFields"] = []
                for f in ["Name", "Overview"]:
                    if personinfo.get(f) and f not in personinfo["LockedFields"]:
                        personinfo["LockedFields"].append(f)

            if updated_global:
                self.set_iteminfo(server=server, server_type=server_type,
                                  itemid=people.get("Id"), iteminfo=personinfo)
                ret_people["Name"] = personinfo["Name"]
                if final_role: ret_people["Role"] = final_role
                return ret_people
            
            if final_role:
                ret_people["Role"] = final_role
                if final_name: ret_people["Name"] = final_name
                return ret_people

            if final_name and final_name != people.get("Name"):
                 ret_people["Name"] = final_name
                 return ret_people

        except Exception as err:
            logger.error(f"更新人物错误 {people.get('Name')}: {str(err)}")
        
        return None

    def __get_douban_actors(self, mediainfo: MediaInfo, season: int = None) -> List[dict]:
        time.sleep(2) 
        doubaninfo = self.chain.match_doubaninfo(name=mediainfo.title,
                                                 imdbid=mediainfo.imdb_id,
                                                 mtype=mediainfo.type,
                                                 year=mediainfo.year,
                                                 season=season)
        if doubaninfo:
            doubanitem = self.chain.douban_info(doubaninfo.get("id")) or {}
            return (doubanitem.get("actors") or []) + (doubanitem.get("directors") or [])
        return []

    def get_iteminfo(self, server: str, server_type: str, itemid: str) -> dict:
        service = self.service_infos(server_type).get(server)
        if not service: return {}
        try:
            url = f'[HOST]emby/Users/[USER]/Items/{itemid}?Fields=ChannelMappingInfo,ProviderIds,Overview&api_key=[APIKEY]'
            if server_type == 'jellyfin':
                url = f'[HOST]Users/[USER]/Items/{itemid}?Fields=ChannelMappingInfo,ProviderIds,Overview&api_key=[APIKEY]'
            res = service.instance.get_data(url=url)
            if res: 
                return res.json()
        except Exception:
            pass
        return {}

    def get_items(self, server: str, server_type: str, parentid: str, mtype: str = None) -> dict:
        service = self.service_infos(server_type).get(server)
        if not service: return {}
        try:
            base_url = f'[HOST]emby/Users/[USER]/Items?api_key=[APIKEY]'
            if server_type == 'jellyfin':
                base_url = f'[HOST]Users/[USER]/Items?api_key=[APIKEY]'
            url = f"{base_url}&ParentId={parentid}" if parentid else base_url
            res = service.instance.get_data(url=url)
            if res: return res.json()
        except Exception:
            pass
        return {}

    def set_iteminfo(self, server: str, server_type: str, itemid: str, iteminfo: dict):
        service = self.service_infos(server_type).get(server)
        if not service: return {}
        try:
            url = f'[HOST]emby/Items/{itemid}?api_key=[APIKEY]&reqformat=json'
            if server_type == "jellyfin":
                url = f'[HOST]Items/{itemid}?api_key=[APIKEY]'
            service.instance.post_data(url=url, data=json.dumps(iteminfo), headers={"Content-Type": "application/json"})
            return True
        except Exception:
            pass
        return False

    def set_item_image(self, server: str, server_type: str, itemid: str, imageurl: str):
        service = self.service_infos(server_type).get(server)
        if not service: return False

        def __download_image_with_retry(url, retries=3):
            headers = {}
            if "doubanio.com" in url:
                headers['Referer'] = "https://movie.douban.com/"
            
            for i in range(retries):
                try:
                    r = RequestUtils(proxies=settings.PROXY, ua=settings.USER_AGENT, headers=headers).get_res(url=url, raise_exception=True)
                    if r and r.status_code == 200:
                        return base64.b64encode(r.content).decode()
                except Exception as e:
                    if i == retries - 1:
                        logger.warn(f"下载图片最终失败 {url}: {e}")
                    time.sleep(1)
            return None

        image_base64 = __download_image_with_retry(imageurl)
        if not image_base64:
            return False

        try:
            url = f'[HOST]emby/Items/{itemid}/Images/Primary?api_key=[APIKEY]'
            if server_type == "jellyfin":
                url = f'[HOST]Items/{itemid}/Images/Primary?api_key=[APIKEY]'
            
            res = service.instance.post_data(url=url, data=image_base64, headers={"Content-Type": "image/png"})
            if res and res.status_code in [200, 204]: 
                return True
        except Exception as e:
            logger.error(f"推送图片到媒体服务器失败：{e}")
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
