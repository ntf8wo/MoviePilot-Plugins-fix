import base64
import copy
import datetime
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from typing import Any, List, Dict, Tuple, Optional

import pytz
import zhconv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from requests import RequestException

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
from app.utils.http import RequestUtils
from app.utils.string import StringUtils


class personmetamod(_PluginBase):
    # 插件名称
    plugin_name = "演职人员刮削(v4.3_Debug版)"
    plugin_desc = "增加图片下载失败的详细日志，排查网络问题。"
    plugin_icon = "actor.png"
    plugin_version = "4.3.0_debug"
    plugin_author = "jxxghp_mod_by_gemini"
    author_url = "https://github.com/jxxghp"
    plugin_config_prefix = "personmeta_mod_"
    plugin_order = 24
    auth_level = 1

    _event = threading.Event()
    _write_lock = threading.Lock()
    _scheduler = None
    _enabled = False
    _onlyonce = False
    _cron = None
    _delay = 0
    _type = "all"
    _remove_nozh = False
    _lock_info = False
    _mediaservers = []
    
    _tmdb_person_cache = {}
    _tmdb_credits_cache = {}
    _executor = None

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

        self.stop_service()
        self._tmdb_person_cache = {}
        self._tmdb_credits_cache = {}
        
        if not self._executor:
            # 保持低并发
            self._executor = ThreadPoolExecutor(max_workers=2)

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(func=self.scrap_library, trigger='date',
                                    run_date=datetime.datetime.now(
                                        tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3)
                                    )
            logger.info(f"【{self.plugin_name}】服务启动，立即运行一次")
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
                "name": "演职人员刮削服务(Debug版)",
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
                        'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                        'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]
                    },
                    {
                        'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                        'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}]
                    }
                ]
            },
            {
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                        'content': [{'component': 'VCronField', 'props': {'model': 'cron', 'label': '媒体库扫描周期', 'placeholder': '5位cron表达式'}}]
                    },
                    {
                        'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                        'content': [{'component': 'VTextField', 'props': {'model': 'delay', 'label': '入库延迟时间（秒）', 'placeholder': '30'}}]
                    },
                    {
                        'component': 'VCol', 'props': {'cols': 12, 'md': 4},
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
                                    'hint': '缺图/缺简介也会自动触发。'
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
                        'component': 'VCol', 'props': {'cols': 12},
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
                        'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                        'content': [{'component': 'VSwitch', 'props': {'model': 'remove_nozh', 'label': '删除未匹配的演员 (慎用)'}}]
                    },
                    {
                        'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                        'content': [
                            {
                                'component': 'VSwitch',
                                'props': {
                                    'model': 'lock_info',
                                    'label': '锁定元数据 (可选)',
                                    'hint': '开启后，修改过的信息将被锁定。',
                                }
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False, "onlyonce": False, "cron": "", "type": "all",
            "delay": 30, "remove_nozh": False, "lock_info": False, "mediaservers": []
        }

    def get_page(self) -> List[dict]:
        pass

    def service_infos(self, type_filter: Optional[str] = None) -> Optional[Dict[str, ServiceInfo]]:
        if not self._mediaservers: return None
        services = MediaServerHelper().get_services(type_filter=type_filter, name_filters=self._mediaservers)
        if not services: return None
        active_services = {}
        for service_name, service_info in services.items():
            if service_info.type == 'plex': continue
            if service_info.instance.is_inactive(): pass
            else: active_services[service_name] = service_info
        return active_services

    @eventmanager.register(EventType.TransferComplete)
    def scrap_rt(self, event: Event):
        if not self._enabled: return
        if not event or not event.event_data: return
        mediainfo: MediaInfo = event.event_data.get("mediainfo")
        meta: MetaBase = event.event_data.get("meta")
        if not mediainfo or not meta: return
        if self._delay: time.sleep(int(self._delay))
        existsinfo = self.chain.media_exists(mediainfo=mediainfo)
        if not existsinfo or not existsinfo.itemid: return
        if existsinfo.server_type == 'plex': return
        iteminfo = MediaServerChain().iteminfo(server=existsinfo.server, item_id=existsinfo.itemid)
        if not iteminfo: return
        self._tmdb_person_cache.clear()
        self._tmdb_credits_cache.clear()
        self.__update_item(server=existsinfo.server, server_type=existsinfo.server_type,
                           item=iteminfo, mediainfo=mediainfo, season=meta.begin_season)

    def scrap_library(self):
        service_infos = self.service_infos()
        if not service_infos: return
        mediaserverchain = MediaServerChain()
        
        process_count = 0
        
        for server, service in service_infos.items():
            logger.info(f"【{self.plugin_name}】开始刮削服务器 {server} ...")
            for library in mediaserverchain.librarys(server):
                logger.info(f"【{self.plugin_name}】正在扫描媒体库: {library.name}")
                for item in mediaserverchain.items(server, library.id):
                    if not item or not item.item_id: continue
                    if "Series" not in item.item_type and "Movie" not in item.item_type: continue
                    if self._event.is_set(): return
                    self.__update_item(server=server, item=item, server_type=service.type)
                    
                    process_count += 1
                    if process_count % 50 == 0:
                        self._tmdb_person_cache.clear()
                        self._tmdb_credits_cache.clear()
                        
                logger.info(f"【{self.plugin_name}】媒体库 {library.name} 扫描完成")
        
        self._tmdb_person_cache.clear()
        self._tmdb_credits_cache.clear()

    def __update_peoples(self, server: str, server_type: str,
                         itemid: str, iteminfo: dict, tmdb_credits: dict, douban_actors: list):
        people_list = iteminfo.get("People", []) or []
        if not people_list:
            return

        logger.info(f"    - 开始并发处理 {len(people_list)} 位演职人员...")
        final_peoples = [None] * len(people_list)
        
        future_to_index = {}
        for idx, people in enumerate(people_list):
            if self._event.is_set(): return
            
            if not people.get("Name"):
                final_peoples[idx] = people
                continue

            has_image = people.get("PrimaryImageTag") is not None
            is_zh_name = StringUtils.is_chinese(people.get("Name"))
            is_zh_role = StringUtils.is_chinese(people.get("Role"))
            has_overview = bool(people.get("Overview"))
            
            if is_zh_name and is_zh_role and has_image and has_overview:
                final_peoples[idx] = people
                continue
            
            future = self._executor.submit(self.__update_people, server, server_type, people, tmdb_credits, douban_actors)
            future_to_index[future] = idx

        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            original_data = people_list[idx]
            try:
                updated_data = future.result()
                if updated_data:
                    final_peoples[idx] = updated_data
                else:
                    if not self._remove_nozh:
                        final_peoples[idx] = original_data
            except Exception as e:
                logger.error(f"    ! [异常] 处理 {original_data.get('Name')} 出错: {e}")
                if not self._remove_nozh:
                    final_peoples[idx] = original_data

        valid_peoples = [p for p in final_peoples if p is not None]
        if valid_peoples:
            iteminfo["People"] = valid_peoples
            self.set_iteminfo(server=server, server_type=server_type,
                              itemid=itemid, iteminfo=iteminfo)

    def __update_item(self, server: str, item: MediaServerItem, server_type: str = None,
                      mediainfo: MediaInfo = None, season: int = None):
        
        logger.info(f"--> [处理中] {item.title} (ID: {item.item_id})")

        def __need_trans_actor(_item):
            people_list = _item.get("People", []) or []
            need = False
            for x in people_list:
                name = x.get("Name")
                if not name: continue
                if not x.get("PrimaryImageTag"): 
                    need = True; break
                if not x.get("Overview"): 
                    need = True; break
                if self._type == "name" and not StringUtils.is_chinese(name): 
                    need = True; break
                if self._type == "all":
                    if not StringUtils.is_chinese(name): 
                        need = True; break
            return need

        if not mediainfo:
            if not item.tmdbid: 
                logger.info(f"    - 跳过: 无TMDB ID")
                return
            mtype = MediaType.TV if item.item_type in ['Series', 'show'] else MediaType.MOVIE
            mediainfo = self.chain.recognize_media(mtype=mtype, tmdbid=item.tmdbid)
            if not mediainfo: 
                logger.info(f"    - 跳过: 无法识别媒体信息")
                return

        iteminfo = self.get_iteminfo(server=server, server_type=server_type, itemid=item.item_id)
        if not iteminfo: return

        if __need_trans_actor(iteminfo):
            logger.info(f"    + 获取 TMDB Credits (ID: {mediainfo.tmdb_id})...")
            tmdb_credits = self.__get_tmdb_credits(mediainfo.tmdb_id, mediainfo.type, season=season)
            logger.info(f"    + 获取 豆瓣信息...")
            douban_actors = self.__get_douban_actors(mediainfo, season)
            
            self.__update_peoples(server=server, server_type=server_type,
                                  itemid=item.item_id, iteminfo=iteminfo, 
                                  tmdb_credits=tmdb_credits, douban_actors=douban_actors)
        else:
            logger.info(f"    - 跳过: 检查通过，无需更新")

        if iteminfo.get("Type") and "Series" in iteminfo["Type"]:
            seasons = self.get_items(server=server, server_type=server_type,
                                     parentid=item.item_id, mtype="Season")
            if not seasons: return
            
            for season_item in seasons.get("Items", []):
                season_num = season_item.get("IndexNumber")
                logger.info(f"    > 处理第 {season_num} 季...")
                
                season_credits = self.__get_tmdb_credits(mediainfo.tmdb_id, MediaType.TV, season=season_num)
                douban_actors = self.__get_douban_actors(mediainfo, season_num)
                
                if server_type == "jellyfin":
                    seasoninfo = self.get_iteminfo(server=server, server_type=server_type, itemid=season_item.get("Id"))
                    if seasoninfo and __need_trans_actor(seasoninfo):
                        self.__update_peoples(server=server, server_type=server_type,
                                              itemid=season_item.get("Id"), iteminfo=seasoninfo,
                                              tmdb_credits=season_credits, douban_actors=douban_actors)
                
                episodes = self.get_items(server=server, server_type=server_type,
                                          parentid=season_item.get("Id"), mtype="Episode")
                if not episodes: continue
                for episode in episodes.get("Items", []):
                    episode_num = episode.get("IndexNumber")
                    episodeinfo = self.get_iteminfo(server=server, server_type=server_type, itemid=episode.get("Id"))
                    
                    if episodeinfo and __need_trans_actor(episodeinfo):
                        episode_credits = self.__get_tmdb_credits(mediainfo.tmdb_id, MediaType.TV, season=season_num, episode=episode_num)
                        target_credits = episode_credits if episode_credits and (episode_credits.get("cast") or episode_credits.get("crew")) else season_credits
                        
                        self.__update_peoples(server=server, server_type=server_type,
                                              itemid=episode.get("Id"), iteminfo=episodeinfo,
                                              tmdb_credits=target_credits, douban_actors=douban_actors)

    def __get_tmdb_credits(self, tmdb_id: int, mtype: MediaType, season: int = None, episode: int = None) -> dict:
        if not settings.TMDB_API_KEY or not tmdb_id: return {}
        cache_key = f"{mtype}_{tmdb_id}_{season}_{episode}"
        if cache_key in self._tmdb_credits_cache: return self._tmdb_credits_cache[cache_key]
        
        base_url = "https://api.themoviedb.org/3"
        tmdb_domain = getattr(settings, "TMDB_DOMAIN", None)
        if tmdb_domain: base_url = f"https://{tmdb_domain}/3"
        
        url = ""
        if mtype == MediaType.MOVIE:
            url = f"{base_url}/movie/{tmdb_id}/credits?api_key={settings.TMDB_API_KEY}&language=zh-CN"
        else:
            if season is not None:
                if episode is not None:
                     url = f"{base_url}/tv/{tmdb_id}/season/{season}/episode/{episode}/credits?api_key={settings.TMDB_API_KEY}&language=zh-CN"
                else:
                     url = f"{base_url}/tv/{tmdb_id}/season/{season}/credits?api_key={settings.TMDB_API_KEY}&language=zh-CN"
            else:
                url = f"{base_url}/tv/{tmdb_id}/credits?api_key={settings.TMDB_API_KEY}&language=zh-CN"
        
        try:
            res = RequestUtils(proxies=settings.PROXY, ua=settings.USER_AGENT).get_res(url=url)
            if res and res.status_code == 200:
                data = res.json()
                self._tmdb_credits_cache[cache_key] = data
                return data
        except Exception: pass
        return {}

    def __get_douban_actors(self, mediainfo: MediaInfo, season: int = None) -> List[dict]:
        doubaninfo = self.chain.match_doubaninfo(name=mediainfo.title,
                                                 imdbid=mediainfo.imdb_id,
                                                 mtype=mediainfo.type,
                                                 year=mediainfo.year,
                                                 season=season)
        if doubaninfo:
            doubanitem = self.chain.douban_info(doubaninfo.get("id")) or {}
            return (doubanitem.get("actors") or []) + (doubanitem.get("directors") or [])
        else:
            pass
        return []

    def __get_tmdb_person_detail(self, tmdb_id: str) -> Tuple[Optional[dict], Optional[dict]]:
        if not settings.TMDB_API_KEY or not tmdb_id: return None, None
        if tmdb_id in self._tmdb_person_cache: return self._tmdb_person_cache[tmdb_id]

        retry_count = 3
        for i in range(retry_count):
            try:
                base_url = "https://api.themoviedb.org/3"
                tmdb_domain = getattr(settings, "TMDB_DOMAIN", None)
                if tmdb_domain: base_url = f"https://{tmdb_domain}/3"
                url = f"{base_url}/person/{tmdb_id}?api_key={settings.TMDB_API_KEY}&language=zh-CN&append_to_response=external_ids"
                res = RequestUtils(proxies=settings.PROXY, ua=settings.USER_AGENT).get_res(url=url)
                if res and res.status_code == 200:
                    data = res.json()
                    external_ids = data.get("external_ids", {})
                    self._tmdb_person_cache[tmdb_id] = (data, external_ids)
                    return data, external_ids
                elif res and res.status_code == 429: time.sleep(2)
            except Exception: time.sleep(1)
        return None, None

    def __update_people(self, server: str, server_type: str,
                        people: dict, tmdb_credits: dict, douban_actors: list) -> Optional[dict]:
        
        ret_people = copy.deepcopy(people)
        
        personinfo = self.get_iteminfo(server=server, server_type=server_type, itemid=people.get("Id"))
        if not personinfo: 
            return None
        
        def __get_id(p):
            if not p.get("ProviderIds"): return None
            return p["ProviderIds"].get("Tmdb") or p["ProviderIds"].get("tmdb")
        
        tmdb_id = __get_id(personinfo)
        matched_credit = None
        cast_list = tmdb_credits.get("cast", [])
        crew_list = tmdb_credits.get("crew", [])
        all_credits = cast_list + crew_list
        
        if tmdb_id:
            for c in all_credits:
                if str(c.get("id")) == str(tmdb_id):
                    matched_credit = c
                    break
        
        if not matched_credit:
            current_name = people.get("Name")
            for c in all_credits:
                if (c.get("name") and c.get("name").lower() == current_name.lower()) or \
                   (c.get("original_name") and c.get("original_name").lower() == current_name.lower()):
                    matched_credit = c
                    tmdb_id = str(c.get("id"))
                    break
        
        if not tmdb_id:
            return None

        tmdb_details, tmdb_ext_ids = self.__get_tmdb_person_detail(tmdb_id)
        if not tmdb_details: 
            logger.error(f"      x [失败] TMDB信息获取失败: ID {tmdb_id} (可能网络不通)")
            return None

        douban_match = None
        if douban_actors:
            tmdb_name_cn = None
            if tmdb_details.get("name") and StringUtils.is_chinese(tmdb_details.get("name")):
                 tmdb_name_cn = tmdb_details.get("name")
            
            for d in douban_actors:
                if tmdb_name_cn and d.get("name") == tmdb_name_cn:
                    douban_match = d
                    break
                if tmdb_details.get("name") and d.get("latin_name") and \
                   tmdb_details.get("name").lower() == d.get("latin_name").lower():
                    douban_match = d
                    break

        updated_global = False
        log_msgs = []

        # --- (A) 姓名 ---
        final_name = None
        tmdb_name = tmdb_details.get("name")
        
        if tmdb_name and StringUtils.is_chinese(tmdb_name):
            final_name = tmdb_name
        elif douban_match and douban_match.get("name") and StringUtils.is_chinese(douban_match.get("name")):
            final_name = douban_match.get("name")
        else:
            final_name = tmdb_name

        if final_name:
            final_name = zhconv.convert(final_name, 'zh-cn')
            if final_name != personinfo.get("Name"):
                personinfo["Name"] = final_name
                updated_global = True
                ret_people["Name"] = final_name
                log_msgs.append(f"姓名更新[{final_name}]")

        # --- (B) 简介 ---
        final_bio = None
        tmdb_bio = tmdb_details.get("biography")
        
        if tmdb_bio:
            final_bio = tmdb_bio
        elif douban_match:
            final_bio = douban_match.get("summary") or douban_match.get("intro") or douban_match.get("biography")
        
        if final_bio:
            final_bio = zhconv.convert(final_bio, 'zh-cn')
            if final_bio != personinfo.get("Overview"):
                personinfo["Overview"] = final_bio
                updated_global = True
                log_msgs.append("简介更新")

        # --- (C) 图片 ---
        final_img = None
        img_source = ""
        _path = tmdb_details.get("profile_path")
        if _path:
            # 优先使用配置的域名，否则默认
            domain = getattr(settings, "TMDB_IMAGE_DOMAIN", "image.tmdb.org")
            final_img = f"https://{domain}/t/p/original{_path}"
            img_source = "TMDB"
        elif douban_match:
            avatar = douban_match.get("avatar")
            if isinstance(avatar, dict) and avatar.get("large"):
                final_img = avatar.get("large")
                img_source = "Douban"
            elif isinstance(avatar, str) and avatar:
                final_img = avatar
                img_source = "Douban"
        
        # 只要找到了URL，就尝试下载，哪怕本地有PrimaryImageTag也尝试检查一下（此处保留原逻辑：本地无图才下）
        has_local_img = people.get("PrimaryImageTag") is not None
        
        if final_img:
            if not has_local_img:
                # 传入人名用于日志
                is_success = self.set_item_image(server, server_type, people.get("Id"), final_img, people.get("Name"))
                if is_success:
                    log_msgs.append(f"图片补全({img_source})")
            # else:
                # logger.debug(f"      - [跳过图片] {people.get('Name')} 已有图片")

        # --- (D) 社交ID ---
        id_mapping = {"imdb_id": "Imdb", "facebook_id": "Facebook", "instagram_id": "Instagram", "twitter_id": "Twitter"}
        current_pids = personinfo.get("ProviderIds", {})
        pids_updated = False
        
        if str(tmdb_id) != str(current_pids.get("Tmdb", "")):
            current_pids["Tmdb"] = str(tmdb_id)
            pids_updated = True
            
        for tmdb_k, emby_k in id_mapping.items():
            val = tmdb_ext_ids.get(tmdb_k)
            if val and str(val) != str(current_pids.get(emby_k, "")):
                current_pids[emby_k] = str(val)
                pids_updated = True
                
        if pids_updated:
            personinfo["ProviderIds"] = current_pids
            updated_global = True
            log_msgs.append("ID回写")

        # --- (E) 角色名 ---
        final_role = None
        if matched_credit:
            _char = matched_credit.get("character") or matched_credit.get("job")
            if _char:
                final_role = zhconv.convert(_char, 'zh-cn')
        
        if final_role:
            ret_people["Role"] = final_role

        # --- (F) 锁定 ---
        if self._lock_info and updated_global:
            if "LockedFields" not in personinfo: personinfo["LockedFields"] = []
            for f in ["Name", "Overview"]:
                if personinfo.get(f) and f not in personinfo["LockedFields"]:
                    personinfo["LockedFields"].append(f)

        if updated_global:
            self.set_iteminfo(server=server, server_type=server_type,
                              itemid=people.get("Id"), iteminfo=personinfo)
        
        if log_msgs:
            logger.info(f"      √ [更新] {people.get('Name')} -> {final_name or people.get('Name')}: {', '.join(log_msgs)}")
        
        return ret_people

    def get_iteminfo(self, server: str, server_type: str, itemid: str) -> dict:
        service = self.service_infos(server_type).get(server)
        if not service: return {}
        try:
            url = f'[HOST]emby/Users/[USER]/Items/{itemid}?Fields=ChannelMappingInfo,ProviderIds,Overview&api_key=[APIKEY]'
            if server_type == 'jellyfin':
                url = f'[HOST]Users/[USER]/Items/{itemid}?Fields=ChannelMappingInfo,ProviderIds,Overview&api_key=[APIKEY]'
            res = service.instance.get_data(url=url)
            if res: return res.json()
        except Exception: pass
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
        except Exception: pass
        return {}

    def set_iteminfo(self, server: str, server_type: str, itemid: str, iteminfo: dict):
        service = self.service_infos(server_type).get(server)
        if not service: return {}
        
        with self._write_lock:
            try:
                time.sleep(1)
                
                url = f'[HOST]emby/Items/{itemid}?api_key=[APIKEY]&reqformat=json'
                if server_type == "jellyfin":
                    url = f'[HOST]Items/{itemid}?api_key=[APIKEY]'
                service.instance.post_data(url=url, data=json.dumps(iteminfo), headers={"Content-Type": "application/json"})
                return True
            except Exception as e:
                logger.error(f"      x [入库失败] ItemID: {itemid} - {e}")
                pass
        return False

    def set_item_image(self, server: str, server_type: str, itemid: str, imageurl: str, person_name: str = "未知"):
        service = self.service_infos(server_type).get(server)
        if not service: return False
        
        # 内嵌下载函数，增加详细日志
        def __download_image_with_retry(url, retries=3):
            # 更加完善的Headers，防止被图床拦截
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
            }
            if "doubanio.com" in url: 
                headers['Referer'] = "https://movie.douban.com/"
            
            for i in range(retries):
                try:
                    # 使用配置的代理 settings.PROXY
                    r = RequestUtils(proxies=settings.PROXY, ua=settings.USER_AGENT, headers=headers).get_res(url=url, raise_exception=True)
                    if r and r.status_code == 200: 
                        return base64.b64encode(r.content).decode()
                    else:
                        logger.warning(f"      ! [{i+1}/{retries}] 下载失败 HTTP {r.status_code if r else 'Unknown'}: {url}")
                except Exception as e:
                    # 【关键修改】打印出具体的下载错误原因
                    logger.warning(f"      ! [{i+1}/{retries}] 下载异常: {e} -> {url}")
                    time.sleep(1)
            return None
            
        image_base64 = __download_image_with_retry(imageurl)
        
        if not image_base64: 
            logger.error(f"      x [图片失败] {person_name}: 下载图片超时或无法连接 -> {imageurl}")
            return False
        
        with self._write_lock:
            try:
                time.sleep(1.5)
                
                url = f'[HOST]emby/Items/{itemid}/Images/Primary?api_key=[APIKEY]'
                if server_type == "jellyfin": url = f'[HOST]Items/{itemid}/Images/Primary?api_key=[APIKEY]'
                res = service.instance.post_data(url=url, data=image_base64, headers={"Content-Type": "image/png"})
                if res and res.status_code in [200, 204]: return True
            except Exception as e:
                logger.error(f"      x [图片上传失败] ItemID: {itemid} - {e}")
                pass
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
            if self._executor:
                self._executor.shutdown(wait=False)
                self._executor = None
        except Exception as e:
            print(str(e))
