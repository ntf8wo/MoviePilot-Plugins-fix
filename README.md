# MoviePilot-Plugins-fix

ntf8wo 的 MoviePilot v2 插件仓库（Mod 版）。

## 安装

在 MoviePilot 的「设定 → 系统 → 插件市场」中添加本仓库地址：

```
https://github.com/ntf8wo/MoviePilot-Plugins-fix
```

## 插件列表

| 插件 | 说明 | MoviePilot v2 | MoviePilot v3 |
|---|---|---|---|
| 演职人员刮削-Mod (`personmetamod`) | 刮削演职人员详细元数据（姓名/角色仅限TMDB，简介/图片TMDB优先豆瓣兜底）。支持Emby出生/逝世/外部ID等字段。 | 1.1 (`plugins.v2`) | 1.1 (`plugins.v3`) |

> MoviePilot v3 用户会自动从 `plugins.v3/` + `package.v3.json` 加载；v2 用户仍从 `plugins.v2/` + `package.v2.json` 加载。

## 演职人员刮削-Mod 主要特性

- 基于 TMDB 刮削人物：姓名（强制简中）、简介、出生/逝世日期、出生地、Imdb/Tvdb 外部 ID，并锁定字段防止媒体服务器刷新覆盖
- 简介与头像在 TMDB 缺失时以豆瓣数据兜底
- 支持 Emby / Jellyfin / Plex，支持条目入库事件实时刮削与定时全库扫描
- 可选：删除非中文演员、强制更新头像
- 性能优化：TMDB 人物请求与媒体项详情运行期缓存、豆瓣数据懒加载（仅兜底时请求）、已有头像自动跳过
