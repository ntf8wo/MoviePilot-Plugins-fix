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
    plugin_name = "演职人员刮削(Emby专用版)"
    # 插件描述
    plugin_desc = "Emby专用：简介强力匹配TMDB，名字优先豆瓣(防别名)，已精简代码。"
    # 插件图标
    plugin_icon = "actor.png"
    # 插件版本
    plugin_version = "2.2.2_mod_v6"
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

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(func=self.scrap_library, trigger='date',
                                    run_date=datetime.datetime.now(
                                        tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3)
                                    )
            logger.info(f"演职人员刮削(Emby专用版)服务启动，立即运行一次")
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
                "name": "演职人员刮削服务(Emby专用版)",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.scrap_library,
                "kwargs": {}
            }]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
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
                                'content': [{'component': 'VSwitch', 'props': {'model': 'lock_info', 'label': '锁定元数据 (建议关闭)'}}]
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
            "lock_info": False
        }

    def service_infos(self, type_filter: Optional[str] = None) -> Optional[Dict[str, ServiceInfo]]:
        if not self._mediaservers:
            logger.warning("尚未配置媒体服务器，请检查配置")
            return None
        # 这里只为了兼容性保留调用，实际 Emby 逻辑已硬编码在下方
        services = MediaServerHelper().get_services(type_filter=type_filter, name_filters=self._mediaservers)
        if not services:
            logger.warning("获取媒体服务器实例失败，请检查配置")
            return None
        return {name: info for name, info in services.items() if not info.instance.is_inactive()}

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
        # 只处理 Emby
        if existsinfo.server_type != 'emby': return
        
        iteminfo = MediaServerChain().iteminfo(server=existsinfo.server, item_id=existsinfo.itemid)
        if not iteminfo: return
        self.__update_item(server=existsinfo.server, server_type=existsinfo.server_type,
                           item=iteminfo, mediainfo=mediainfo, season=meta.begin_season)

    def scrap_library(self):
        service_infos = self.service_infos()
        if not service_infos: return
        mediaserverchain = MediaServerChain()
        for server, service in service_infos.items():
            if service.type != 'emby': continue # 跳过非 Emby
            logger.info(f"开始刮削服务器 {server} 的演员信息 ...")
            for library in mediaserverchain.librarys(server):
                logger.info(f"开始刮削媒体库 {library.name} 的演员信息 ...")
                for item in mediaserverchain.items(server, library.id):
                    if not item or not item.item_id: continue
                    if "Series" not in item.item_type and "Movie" not in item.item_type: continue
                    if self._event.is_set(): return
                    logger.info(f"开始刮削 {item.title} 的演员信息 ...")
                    self.__update_item(server=server, item=item, server_type=service.type)
            logger.info(f"服务器 {server} 的演员信息刮削完成")

    def __update_peoples(self, server: str, server_type: str, itemid: str, iteminfo: dict, douban_actors):
        peoples = []
        for people in iteminfo.get("People", []) or []:
            if self._event.is_set(): return
            if not people.get("Name"): continue
            # 如果已经是纯中文名和角色，且不强制更新，则跳过 (这里为了保证简介更新，建议放宽)
            # 但为了效率，如果名字和角色都是中文，我们通常认为不需要更新名字
            # 可是简介可能需要更新，所以这里我们继续进入 logic
            info = self.__update_people(server=server, server_type=server_type,
                                        people=people, douban_actors=douban_actors)
            if info: peoples.append(info)
            elif not self._remove_nozh: peoples.append(people)
        if peoples:
            iteminfo["People"] = peoples
            self.set_iteminfo(server=server, server_type=server_type, itemid=itemid, iteminfo=iteminfo)

    def __update_item(self, server: str, item: MediaServerItem, server_type: str = None,
                      mediainfo: MediaInfo = None, season: int = None):
        def __need_trans_actor(_item):
            # 只要开启，就总是尝试更新，因为我们要补充 TMDB 简介
            return True

        if not mediainfo:
            if not item.tmdbid: return
            mtype = MediaType.TV if item.item_type in ['Series', 'show'] else MediaType.MOVIE
            mediainfo = self.chain.recognize_media(mtype=mtype, tmdbid=item.tmdbid)
            if not mediainfo: return

        iteminfo = self.get_iteminfo(server=server, server_type=server_type, itemid=item.item_id)
        if not iteminfo: return

        if __need_trans_actor(iteminfo):
            logger.info(f"开始获取 {item.title} 的豆瓣演员信息 ...")
            douban_actors = self.__get_douban_actors(mediainfo=mediainfo, season=season)
            self.__update_peoples(server=server, server_type=server_type,
                                  itemid=item.item_id, iteminfo=iteminfo, douban_actors=douban_actors)

        if iteminfo.get("Type") and "Series" in iteminfo["Type"]:
            seasons = self.get_items(server=server, server_type=server_type, parentid=item.item_id, mtype="Season")
            if not seasons: return
            for season in seasons.get("Items", []):
                season_actors = self.__get_douban_actors(mediainfo=mediainfo, season=season.get("IndexNumber"))
                episodes = self.get_items(server=server, server_type=server_type, parentid=season.get("Id"), mtype="Episode")
                if not episodes: continue
                for episode in episodes.get("Items", []):
                    episodeinfo = self.get_iteminfo(server=server, server_type=server_type, itemid=episode.get("Id"))
                    if not episodeinfo: continue
                    self.__update_peoples(server=server, server_type=server_type,
                                          itemid=episode.get("Id"), iteminfo=episodeinfo,
                                          douban_actors=season_actors)

    def __update_people(self, server: str, server_type: str, people: dict, douban_actors: list = None) -> Optional[dict]:
        def __get_peopleid(p: dict) -> Tuple[Optional[str], Optional[str]]:
            if not p.get("ProviderIds"): return None, None
            # Emby 常见的键值大小写处理
            tmdb_id = p["ProviderIds"].get("Tmdb") or p["ProviderIds"].get("tmdb")
            imdb_id = p["ProviderIds"].get("Imdb") or p["ProviderIds"].get("imdb")
            return tmdb_id, imdb_id

        ret_people = copy.deepcopy(people)
        try:
            personinfo = self.get_iteminfo(server=server, server_type=server_type, itemid=people.get("Id"))
            if not personinfo: return None

            updated_name = False
            updated_overview = False
            update_character = False
            
            final_name = None
            final_overview = None
            final_img = None
            
            # 1. TMDB 数据 (简介强力匹配)
            tmdb_cn_name = None
            tmdb_overview = None
            tmdb_img = None

            person_tmdbid, person_imdbid = __get_peopleid(personinfo)
            
            if person_tmdbid:
                # 记录一下我们找到了 TMDB ID
                logger.debug(f"正在查询 TMDB ID: {person_tmdbid} ({people.get('Name')})")
                person_detail = TmdbChain().person_detail(int(person_tmdbid))
                
                if person_detail:
                    # 图片
                    _path = person_detail.profile_path
                    if _path:
                        tmdb_img = f"https://{settings.TMDB_IMAGE_DOMAIN}/t/p/original{_path}"
                    
                    # 简介：去掉中文检测！只要有简介就拿来用！
                    if person_detail.biography:
                        tmdb_overview = person_detail.biography
                        logger.debug(f"TMDB 获取到简介 (长度: {len(tmdb_overview)})")
                    else:
                        logger.debug(f"TMDB 简介为空")

                    # 名字：依然只信中文本名
                    if person_detail.name and StringUtils.is_chinese(person_detail.name):
                        tmdb_cn_name = person_detail.name

            # 2. 豆瓣 数据匹配
            douban_match = None
            if douban_actors:
                for douban_actor in douban_actors:
                    is_match = False
                    # 宽松匹配
                    if douban_actor.get("latin_name") == people.get("Name") or \
                       douban_actor.get("name") == people.get("Name"):
                        is_match = True
                    elif tmdb_cn_name and douban_actor.get("name") == tmdb_cn_name:
                        is_match = True
                    
                    if is_match:
                        douban_match = douban_actor
                        break
            
            # 3. 决策阶段
            # 名字：豆瓣 > TMDB中文本名
            if douban_match and douban_match.get("name"):
                final_name = douban_match.get("name")
            elif tmdb_cn_name:
                final_name = tmdb_cn_name

            # 简介：TMDB(无条件) > 豆瓣
            if tmdb_overview:
                final_overview = tmdb_overview
            elif douban_match and douban_match.get("title"):
                final_overview = douban_match.get("title")

            # 图片：TMDB > 豆瓣
            if tmdb_img:
                final_img = tmdb_img
            elif douban_match:
                avatar = douban_match.get("avatar") or {}
                if avatar.get("large"):
                    final_img = avatar.get("large")
            
            # 角色
            final_role = None
            if douban_match and douban_match.get("character"):
                character = re.sub(r"饰\s+", "", douban_match.get("character"))
                character = re.sub("演员", "", character)
                if character: final_role = character

            # 4. 应用变更
            if final_name:
                personinfo["Name"] = final_name
                ret_people["Name"] = final_name
                updated_name = True
            
            if final_overview:
                personinfo["Overview"] = final_overview
                updated_overview = True

            if final_role:
                ret_people["Role"] = final_role
                update_character = True

            if final_img:
                self.set_item_image(server=server, server_type=server_type, itemid=people.get("Id"), imageurl=final_img)

            # 5. 锁定 (如果开关开启)
            if self._lock_info:
                if updated_name:
                    if "LockedFields" not in personinfo: personinfo["LockedFields"] = []
                    if "Name" not in personinfo["LockedFields"]: personinfo["LockedFields"].append("Name")
                if updated_overview:
                    if "LockedFields" not in personinfo: personinfo["LockedFields"] = []
                    if "Overview" not in personinfo["LockedFields"]: personinfo["LockedFields"].append("Overview")

            if updated_name or updated_overview or update_character:
                self.set_iteminfo(server=server, server_type=server_type, itemid=people.get("Id"), iteminfo=personinfo)
                return ret_people

        except Exception as err:
            logger.error(f"更新人物信息失败：{str(err)}")
        return None

    def __get_douban_actors(self, mediainfo: MediaInfo, season: int = None) -> List[dict]:
        sleep_time = 3 + int(time.time()) % 7
        time.sleep(sleep_time)
        doubaninfo = self.chain.match_doubaninfo(name=mediainfo.title, imdbid=mediainfo.imdb_id, mtype=mediainfo.type, year=mediainfo.year, season=season)
        if doubaninfo:
            doubanitem = self.chain.douban_info(doubaninfo.get("id")) or {}
            return (doubanitem.get("actors") or []) + (doubanitem.get("directors") or [])
        return []

    # --- Emby 专用精简版 API ---

    def get_iteminfo(self, server: str, server_type: str, itemid: str) -> dict:
        service = self.service_infos(server_type).get(server)
        if not service: return {}
        try:
            url = f'[HOST]emby/Users/[USER]/Items/{itemid}?Fields=ChannelMappingInfo&api_key=[APIKEY]'
            res = service.instance.get_data(url=url)
            if res: return res.json()
        except Exception as err:
            logger.error(f"获取Emby媒体项详情失败：{str(err)}")
        return {}

    def get_items(self, server: str, server_type: str, parentid: str, mtype: str = None) -> dict:
        service = self.service_infos(server_type).get(server)
        if not service: return {}
        try:
            if parentid: url = f'[HOST]emby/Users/[USER]/Items?ParentId={parentid}&api_key=[APIKEY]'
            else: url = '[HOST]emby/Users/[USER]/Items?api_key=[APIKEY]'
            res = service.instance.get_data(url=url)
            if res: return res.json()
        except Exception as err:
            logger.error(f"获取Emby媒体的所有子媒体项失败：{str(err)}")
        return {}

    def set_iteminfo(self, server: str, server_type: str, itemid: str, iteminfo: dict):
        service = self.service_infos(server_type).get(server)
        if not service: return {}
        try:
            res = service.instance.post_data(
                url=f'[HOST]emby/Items/{itemid}?api_key=[APIKEY]&reqformat=json',
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
                if "doubanio.com" in imageurl:
                    r = RequestUtils(headers={'Referer': "https://movie.douban.com/"}, ua=settings.USER_AGENT).get_res(url=imageurl, raise_exception=True)
                else:
                    r = RequestUtils(proxies=settings.PROXY, ua=settings.USER_AGENT).get_res(url=imageurl, raise_exception=True)
                if r: return base64.b64encode(r.content).decode()
            except Exception as err:
                logger.error(f"下载图片失败：{str(err)}")
            return None

        try:
            image_base64 = __download_image()
            if image_base64:
                res = service.instance.post_data(url=f'[HOST]emby/Items/{itemid}/Images/Primary?api_key=[APIKEY]', data=image_base64, headers={"Content-Type": "image/png"})
                if res and res.status_code in [200, 204]: return True
        except Exception as result:
            logger.error(f"更新Emby媒体项图片失败：{result}")
        return False

    @staticmethod
    def __get_chinese_name(personinfo: schemas.MediaPerson) -> str:
        try:
            if personinfo.name and StringUtils.is_chinese(personinfo.name):
                return personinfo.name
        except Exception as err:
            logger.error(f"获取人物中文名失败：{err}")
        return ""

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
