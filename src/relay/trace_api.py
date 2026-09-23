"""跨端任务追踪的查询接口。

数据存在 SQLite 里（见 trace_db.py），**默认永久保留**；
这些接口只负责按条件取，不做任何删除。

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | /api/trace/overview | 顶部 KPI + 左侧用户列表（含在线状态） |
| GET | /api/trace/links | **链路列表**：按会话对聚合的多轮往返 |
| GET | /api/trace/links/{link_key} | 单条链路的完整回合时间轴 |
| GET | /api/trace/tasks | 任务列表，按用户 / 角色 / 状态 / 关键词 / 时间段过滤，可分页 |
| GET | /api/trace/task-groups | **任务组**：用 parent 溯源到源头，一条调用链一组，按组分页 |
| GET | /api/trace/tasks/{task_id} | 单任务时间线 |
| GET | /api/trace/tasks/{task_id}/events | 该任务的原始事件明细（含每一轮 probe）|
| GET | /api/trace/stream | SSE 实时推送（任务变更） |
| GET | /api/trace/stats | 追踪自身运行状况（库大小、行数、数据跨度）|
| POST | /api/trace/maintain | 立即合并 WAL（备份前用）|

时间范围有两种写法，`from`/`to` 优先于 `days`：
  * `days=14`（默认，看板默认值来自 RELAY_TRACE_DEFAULT_DAYS）
  * `days=0` 表示**不限时间**（查全部历史）
  * `from`/`to` 接受 epoch 秒 或 `YYYY-MM-DD` 或 `YYYY-MM-DDTHH:MM`

默认**不鉴权**（RELAY_TRACE_REQUIRE_TOKEN=1 可打开），因为部署在内网，
且看板要能被随手打开。任务摘要里会带业务正文片段，公网暴露前请开鉴权。
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from .security import extract_bearer, token_matches
from .trace import TraceStore

logger = logging.getLogger(__name__)

trace_router = APIRouter(prefix="/api/trace", tags=["trace"])

#: SSE 心跳间隔（秒）。中间有 Nginx 时，太久不发数据连接会被掐
_SSE_HEARTBEAT = 15.0

_TIME_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d")


def _store(request: Request) -> TraceStore:
    store = getattr(request.app.state, "tracer", None)
    if store is None or not store.enabled:
        raise HTTPException(
            status_code=503,
            detail={"code": "TRACE_DISABLED", "message": "追踪未启用（RELAY_TRACE_ENABLED=0）"},
        )
    return store


def _guard(request: Request) -> None:
    """可选鉴权：默认关闭；打开后与其他 REST 接口用同一个 token。"""
    settings = getattr(request.app.state, "settings", None)
    if settings is None or not getattr(settings, "trace_require_token", False):
        return
    token = (
        request.headers.get("x-auth-token")
        or request.query_params.get("token")
        or extract_bearer(request.headers.get("authorization"))
        or ""
    )
    if not token_matches(settings, token):
        raise HTTPException(status_code=401, detail={"code": "UNAUTHORIZED", "message": "token 无效"})


def _parse_time(value: str | None) -> Optional[float]:
    """接受 epoch 秒 / `YYYY-MM-DD` / `YYYY-MM-DD HH:MM[:SS]`（本地时区）。"""
    text = (value or "").strip()
    if not text:
        return None
    try:
        number = float(text)
        # 毫秒时间戳兜底：13 位数字按毫秒处理
        return number / 1000.0 if number > 1e11 else number
    except ValueError:
        pass
    for fmt in _TIME_FORMATS:
        try:
            return datetime.strptime(text, fmt).timestamp()
        except ValueError:
            continue
    raise HTTPException(
        status_code=400,
        detail={"code": "BAD_REQUEST", "message": f"无法解析时间 '{text}'（用 YYYY-MM-DD 或时间戳）"},
    )


def _range(
    store: TraceStore, days: Optional[int], since: str | None, until: str | None
) -> tuple[Optional[float], Optional[float]]:
    """算出查询区间。from/to 优先；days=0 表示不限时间（全部历史）。"""
    start = _parse_time(since)
    end = _parse_time(until)
    if start is not None or end is not None:
        return start, end
    effective = store.default_days if days is None else days
    if effective <= 0:
        return None, None
    return time.time() - 86400 * effective, None


def _percentile_note(days: Optional[int]) -> Optional[int]:
    return days


@trace_router.get("/overview", summary="追踪总览：KPI + 用户维度统计")
async def overview(
    request: Request,
    days: Optional[int] = Query(None, ge=0, le=3650, description="统计范围（天），0=全部历史"),
    from_: Optional[str] = Query(None, alias="from", description="起始时间：YYYY-MM-DD 或时间戳"),
    to: Optional[str] = Query(None, description="结束时间：YYYY-MM-DD 或时间戳"),
) -> dict[str, Any]:
    _guard(request)
    store = _store(request)
    registry = getattr(request.app.state, "registry", None)
    now = time.time()
    since, until = _range(store, days, from_, to)

    data = await asyncio.to_thread(store.overview, since=since, until=until)
    users: dict[str, dict[str, Any]] = data["users"]

    # 在线状态来自注册表（实时），历史统计来自库
    for item in users.values():
        client_id = item["client_id"]
        item["online"] = bool(registry.is_online(client_id)) if registry else False
        item["connections"] = registry.connection_count(client_id) if registry else 0

    # 在线但这段时间没有任务的人也要出现在左栏，否则「他今天没干活」看不出来
    if registry:
        for client_id in registry.client_ids():
            if client_id not in users:
                users[client_id] = {
                    "client_id": client_id, "online": True,
                    "connections": registry.connection_count(client_id),
                    "sent_total": 0, "sent_active": 0, "sent_failed": 0,
                    "handled_total": 0, "handled_active": 0, "handled_failed": 0,
                    "last_activity": 0.0,
                }

    ordered = sorted(
        users.values(),
        key=lambda u: (-(u["sent_active"] + u["handled_active"]), -u["last_activity"], u["client_id"]),
    )

    kpi = dict(data["kpi"])
    kpi["online_clients"] = len(registry.client_ids()) if registry else 0

    return {
        "ts": now,
        "days": days if days is not None else store.default_days,
        "since": since,
        "until": until,
        "kpi": kpi,
        "users": ordered,
    }


@trace_router.get("/tasks", summary="任务列表")
async def list_tasks(
    request: Request,
    user: str = Query("", description="按用户名过滤（client_id）"),
    role: str = Query("all", pattern="^(all|sent|handled)$", description="该用户是发起方还是处理方"),
    state: str = Query("all", description="all/active/done/failed/cancelled/notified/silent 或具体状态"),
    q: str = Query("", description="关键词：摘要 / task_id / conv_id / 会话ID"),
    days: Optional[int] = Query(None, ge=0, le=3650, description="时间范围（天），0=全部历史"),
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    _guard(request)
    store = _store(request)
    now = time.time()
    since, until = _range(store, days, from_, to)

    total, tasks = await asyncio.to_thread(
        store.query_tasks,
        since=since, until=until, user=user, role=role,
        state=state, keyword=q, limit=limit, offset=offset,
    )
    return {
        "ts": now,
        "total": total,
        "offset": offset,
        "limit": limit,
        "since": since,
        "until": until,
        "tasks": [task.brief(now) for task in tasks],
    }


#: 建树时一次性拉取的任务上限。溯源在内存里做，不走递归 SQL：
#: 数据量在万级以内时这比递归 CTE 快也好懂；超过上限则截断并在响应里给出 truncated 标志。
_GROUP_SCAN_CAP = 5000


def _parent_is_real(row: Any, parent_row: Any) -> bool:
    """判定一个 parent_msg_id 是不是真的成立。

    parent_msg_id 的语义是「我正在处理的那条入站消息」，所以父任务的
    **接收方**必须就是当前这条消息的**发起方** —— B 收了 A 的活再转派给 C，
    这才叫子环节。

    发起方在自己发出的两条消息之间填 parent（A 发了任务1，又发一条指向它的
    消息）属于对这个字段的误用：那是 A 又开了一件新事，应当独立成链，
    而不是挂到前一条下面。

    信息不全时（老客户端没上报会话）一律放行，宁可少断不可误断。
    """
    parent_handler = str(
        getattr(parent_row, "handler_session_real", "")
        or getattr(parent_row, "handler_session", "") or ""
    )
    requester = str(getattr(row, "requester_session", "") or "")
    if not parent_handler or not requester:
        return True
    return parent_handler == requester


def _resolve_root(task_id: str, index: dict[str, Any]) -> str:
    """沿 parent_task_id 一路向上，找到这条消息所属链路的源头。

    停止条件：没有 parent（真正的源头）、parent 不在本次结果集（跨时间窗或
    已被淘汰）、parent 不成立（见 _parent_is_real）、以及环路。

    环路保护不是多余的：parent_msg_id 由 agent 填，填成互相指向或指向自己
    都有可能，没有 seen 集合这里会直接死循环。
    """
    cur = task_id
    seen: set[str] = set()
    while cur not in seen:
        seen.add(cur)
        row = index.get(cur)
        if row is None:
            break
        parent = str(row.parent_task_id or "")
        if not parent or parent not in index:
            break
        if not _parent_is_real(row, index[parent]):
            break
        cur = parent
    return cur


@trace_router.get("/task-groups", summary="任务组（用 parent 溯源到源头，一条调用链一组）")
async def list_task_groups(
    request: Request,
    user: str = Query("", description="按用户名过滤（client_id）"),
    role: str = Query("all", pattern="^(all|sent|handled)$"),
    state: str = Query("all", description="all/active/done/failed/cancelled/notified/silent"),
    q: str = Query("", description="关键词：摘要 / task_id / conv_id / 会话ID"),
    error: str = Query("", description="按失败原因筛（error_code，如 session_not_found）"),
    dur_min: Optional[float] = Query(None, ge=0, description="耗时下限（秒，含）"),
    dur_max: Optional[float] = Query(None, ge=0, description="耗时上限（秒，不含）"),
    depth: Optional[int] = Query(None, ge=0, description="转派层数（0=直接发起）"),
    struct: str = Query("", description="通信结构：cross=跳用户，self=同用户不同会话"),
    pair: str = Query("", description="会话对，格式 发起方→处理方（用户名或会话ID）"),
    days: Optional[int] = Query(None, ge=0, le=3650),
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=200, description="返回多少**组**（不是多少条）"),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """把任务按【源头】归并，一条调用链返回一组。

    分页单位是「组」而不是「条」，所以一条链不会被翻页切断。

    筛选条件的语义是「命中链上任意一个节点，整条链就保留」：
    只显示命中的那几个节点会把调用链拆碎，失去分组的意义。
    """
    _guard(request)
    store = _store(request)
    now = time.time()
    since, until = _range(store, days, from_, to)

    # 先把时间窗内的任务全拿出来建索引。不能只拿筛选命中的：
    # 溯源要靠 parent 链，父节点被筛掉就找不到源头了。
    _, all_rows = await asyncio.to_thread(
        store.query_tasks,
        since=since, until=until, user="", role="all",
        state="all", keyword="", limit=_GROUP_SCAN_CAP, offset=0,
    )
    truncated = len(all_rows) >= _GROUP_SCAN_CAP

    # 再取一次筛选结果，用来决定哪些组该出现
    if user or role != "all" or state != "all" or q:
        _, hit_rows = await asyncio.to_thread(
            store.query_tasks,
            since=since, until=until, user=user, role=role,
            state=state, keyword=q, limit=_GROUP_SCAN_CAP, offset=0,
        )
    else:
        hit_rows = all_rows

    index = {t.task_id: t for t in all_rows}

    # error / dur / depth / struct / pair 这五个维度 SQL 层没有（depth 还得靠 parent 链才能算），
    # 而 all_rows 已经全量在内存里了，在 Python 层再筛一道比改 SQL 更稳。
    if error or dur_min is not None or dur_max is not None or depth is not None or struct or pair:
        hit_rows = [r for r in hit_rows if _node_matches(
            r, index, error=error, dur_min=dur_min, dur_max=dur_max,
            depth=depth, struct=struct, pair=pair,
        )]

    # 按源头归组：组里装整条链，而不只是命中的节点
    members: dict[str, list[Any]] = {}
    for row in all_rows:
        members.setdefault(_resolve_root(row.task_id, index), []).append(row)

    keep = {_resolve_root(r.task_id, index) for r in hit_rows}

    groups = []
    for root_id in keep:
        nodes = members.get(root_id) or []
        if not nodes:
            continue
        root = index.get(root_id) or nodes[0]
        # 未终结的排前面，其余按链上最后一次动静倒序，跟任务列表口径一致
        groups.append({
            "root_id": root_id,
            "active": any(not n.terminal for n in nodes),
            "last_activity": max(float(n.last_activity or 0) for n in nodes),
            "created_at": float(root.created_at or root.first_seen or 0),
            "size": len(nodes),
            "nodes": nodes,
        })

    groups.sort(key=lambda g: (0 if g["active"] else 1, -g["last_activity"]))
    total = len(groups)
    active_total = sum(1 for g in groups if g["active"])
    page = groups[offset:offset + limit]

    return {
        "ts": now,
        "total": total,
        "active_total": active_total,
        "offset": offset,
        "limit": limit,
        "since": since,
        "until": until,
        "truncated": truncated,
        "groups": [
            {
                "root_id": g["root_id"],
                "active": g["active"],
                "size": g["size"],
                "last_activity": g["last_activity"],
                "created_at": g["created_at"],
                "nodes": [
                    dict(n.brief(now), depth=_depth_of(n.task_id, index))
                    for n in sorted(
                        g["nodes"],
                        key=lambda n: (float(n.created_at or n.first_seen or 0), n.task_id),
                    )
                ],
            }
            for g in page
        ],
    }


def _depth_of(task_id: str, index: dict[str, Any]) -> int:
    """距离源头几层，前端直接拿它做缩进。口径必须跟 _resolve_root 一致。"""
    depth, cur, seen = 0, task_id, set()
    while cur not in seen:
        seen.add(cur)
        row = index.get(cur)
        if row is None:
            break
        parent = str(row.parent_task_id or "")
        if not parent or parent not in index:
            break
        if not _parent_is_real(row, index[parent]):
            break
        depth += 1
        cur = parent
    return depth


def _node_seconds(row: Any) -> Optional[float]:
    """单条任务的真实耗时，口径必须跟 analytics 完全一致。

    notify 直接返回 None：单向通知投递即结，耗时恒为 0，
    按耗时筛的时候把它算进「<1s」会把那个桶泡得很难看。
    """
    if str(getattr(row, "kind", "") or "") == "notify":
        return None
    started = float(getattr(row, "created_at", 0) or 0)
    ended = float(getattr(row, "finished_at", 0) or 0)
    if not started or not ended or ended < started:
        return None
    return ended - started


def _node_matches(
    row: Any,
    index: dict[str, Any],
    *,
    error: str = "",
    dur_min: Optional[float] = None,
    dur_max: Optional[float] = None,
    depth: Optional[int] = None,
    struct: str = "",
    pair: str = "",
) -> bool:
    """单个节点是否命中那五个扩展维度。全部是与关系。"""
    if error:
        # error 筛选自带「必须是失败态」的约束。
        # 否则 error=unknown 会把所有 error_code 为空的记录（包括全部成功的）全捞进来。
        st = str(getattr(row, "state", "") or "")
        if st not in ("failed", "cancelled"):
            return False
        code = str(getattr(row, "error_code", "") or "")
        # 统计面板把没写 error_code 的失败归为 unknown，筛选得跟着同口径
        if (code or "unknown") != error:
            return False
    if dur_min is not None or dur_max is not None:
        sec = _node_seconds(row)
        if sec is None:
            return False
        if dur_min is not None and sec < dur_min:
            return False
        if dur_max is not None and sec >= dur_max:
            return False
    if depth is not None and _depth_of(row.task_id, index) != depth:
        return False
    if struct:
        rq = str(getattr(row, "requester", "") or "")
        hd = str(getattr(row, "handler", "") or "")
        is_cross = rq != hd
        if struct == "cross" and not is_cross:
            return False
        if struct == "self" and is_cross:
            return False
    if pair:
        want = [x.strip() for x in re.split(r"→|->", pair) if x.strip()]
        if len(want) == 2:
            a, b = want
            rq = str(getattr(row, "requester", "") or "")
            hd = str(getattr(row, "handler", "") or "")
            rs = str(getattr(row, "requester_session", "") or "")
            hs = str(getattr(row, "handler_session_real", "") or
                     getattr(row, "handler_session", "") or "")
            # 用户名或会话ID 都能匹配：统计面板给的是用户名对，
            # 拓扑图那边点过来的可能是会话ID 对
            if not ((a == rq or a == rs) and (b == hd or b == hs)):
                return False
    return True


@trace_router.get("/failure-issues", summary="失败问题（按 原因 × 会话对 聚合，按影响面排序）")
async def list_failure_issues(
    request: Request,
    user: str = Query("", description="按用户名过滤"),
    role: str = Query("all", pattern="^(all|sent|handled)$"),
    q: str = Query("", description="关键词"),
    days: Optional[int] = Query(None, ge=0, le=3650),
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
) -> dict[str, Any]:
    """把散在各条消息里的失败，按【失败原因 × 发起方 × 处理方】归成一个「问题」。

    为什么要聚合：流水列表里看到「找不到对方会话 5 次」是没法行动的，
    得知道「是谁发给谁」才能去掉那个失效的调用。同一个根因只占一行，
    配上首次/最近出现时间，还能分清「新冒出来的」和「一直在坏的」。

    排序按发生次数而不是时间：发生 12 次的比刚发生一次的更值得先修。
    """
    _guard(request)
    store = _store(request)
    now = time.time()
    since, until = _range(store, days, from_, to)

    _, rows = await asyncio.to_thread(
        store.query_tasks,
        since=since, until=until, user=user, role=role,
        state="all", keyword=q, limit=_ANALYTICS_CAP, offset=0,
    )

    def g(r: Any, k: str, d: Any = "") -> Any:
        return getattr(r, k, d)

    issues: dict[tuple, dict[str, Any]] = {}
    for r in rows:
        st = str(g(r, "state") or "")
        if st not in ("failed", "cancelled"):
            continue
        code = str(g(r, "error_code") or "") or ("cancelled" if st == "cancelled" else "unknown")
        rq = str(g(r, "requester") or "?")
        hd = str(g(r, "handler") or "?")
        key = (code, rq, hd)
        at = float(g(r, "created_at", 0) or g(r, "first_seen", 0) or 0)
        it = issues.get(key)
        if it is None:
            it = issues[key] = {
                "error_code": code,
                "label": _ERR_LABEL.get(code, code),
                "hint": _ERR_HINT.get(code, ""),
                "requester": rq,
                "handler": hd,
                "count": 0,
                "first_at": at,
                "last_at": at,
                "samples": [],
                "_sessions": set(),
            }
        it["count"] += 1
        if at:
            it["first_at"] = min(it["first_at"] or at, at)
            it["last_at"] = max(it["last_at"] or at, at)
        it["_sessions"].add(str(g(r, "requester_session") or ""))
        if len(it["samples"]) < 5:
            it["samples"].append({
                "task_id": str(g(r, "task_id") or ""),
                "summary": str(g(r, "summary") or ""),
                "created_at": at,
            })

    out = sorted(issues.values(), key=lambda x: (-x["count"], -(x["last_at"] or 0)))
    # 刚出现的问题单独标出来：这类最可能是刚改坏的，优先级比老问题高
    day_ago = now - 86400
    for it in out:
        it["sessions"] = len([s for s in it.pop("_sessions") if s])
        it["is_new"] = bool(it["first_at"] and it["first_at"] >= day_ago)

    return {
        "ts": now,
        "since": since,
        "until": until,
        "total": len(out),
        "total_failures": sum(x["count"] for x in out),
        "issues": out[:limit],
    }


@trace_router.get("/links", summary="链路列表（按会话对聚合的多轮往返）")
async def list_links(
    request: Request,
    user: str = Query("", description="按用户名过滤；语义是「包含此人的链路」"),
    role: str = Query("all", pattern="^(all|sent|handled)$"),
    state: str = Query("all", pattern="^(all|active|silent|failed|done)$", description="链路级状态"),
    q: str = Query("", description="关键词：摘要 / task_id / conv_id / 会话ID"),
    days: Optional[int] = Query(None, ge=0, le=3650),
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    recent: int = Query(3, ge=0, le=50, description="每条链路预览几个最近回合"),
) -> dict[str, Any]:
    """一对会话一行，把 A→B→A→B 的多轮往返串成一条链路。

    为什么不能用 conv_id 串：hub.ts 的 sendRequest 每次都 `CV-${randomUUID()}`，
    conv_id 实际上是「单次往返 ID」，只能盖住一次 request + 它的 response。
    所以链路靠的是会话对（无序），见 trace.make_link_key。

    过滤先作用在回合上再聚合，所以 `days=7` 的意思是
    「这七天内有过回合的链路」，而不是「七天内新建的链路」。
    """
    _guard(request)
    store = _store(request)
    now = time.time()
    since, until = _range(store, days, from_, to)
    data = await asyncio.to_thread(
        store.query_links,
        since=since, until=until, user=user, role=role,
        link_state=state, keyword=q, limit=limit, offset=offset, recent=recent,
    )
    data.update({"ts": now, "offset": offset, "limit": limit, "since": since, "until": until})
    return data


@trace_router.get("/links/{link_key}", summary="单条链路的完整回合时间轴")
async def link_detail(link_key: str, request: Request) -> dict[str, Any]:
    """不受时间筛选限制 —— 点开一条链路就是要看它的全部历史。"""
    _guard(request)
    store = _store(request)
    data = await asyncio.to_thread(store.get_link, link_key)
    if data is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"没有找到链路 {link_key}"},
        )
    return {"ts": time.time(), "link": data}


@trace_router.get("/tasks/{task_id}", summary="单个任务的完整时间线")
async def task_detail(task_id: str, request: Request) -> dict[str, Any]:
    _guard(request)
    store = _store(request)
    task = await asyncio.to_thread(store.get_task, task_id)
    if task is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"没有找到任务 {task_id}"},
        )
    detail = task.detail()
    detail["session_graph"] = await asyncio.to_thread(store.session_graph, task_id=task_id)
    detail["execution_graph"] = await asyncio.to_thread(store.execution_graph, task_id=task_id)
    return {"ts": time.time(), "task": detail}


@trace_router.get("/tasks/{task_id}/events", summary="任务的原始事件明细")
async def task_events(
    task_id: str,
    request: Request,
    limit: int = Query(2000, ge=1, le=20000),
) -> dict[str, Any]:
    """折叠前的原始事件，包含每一轮 probe。深度排查时用。"""
    _guard(request)
    store = _store(request)
    events = await asyncio.to_thread(store.task_events, task_id, limit)
    return {"ts": time.time(), "task_id": task_id, "count": len(events), "events": events}


#: 统计面板一次扫描的上限。统计必须和列表用同一套筛选口径，
#: 否则「统计说有 12 个失败、列表只列出 9 条」这种对不上的情况会让人不敢信数据。
_ANALYTICS_CAP = 20000

#: 错误码 → 给用户看的中文说法。
#: 看板是给人看的，直接抹 error_code 出去（PEER_OFFLINE / session_not_found）
#: 只有写代码的看得懂，所以这里给每个码配一个中文名，前端只显中文。
_ERR_LABEL = {
    "PEER_OFFLINE": "对方不在线",
    "session_not_found": "找不到对方会话",
    "peer_unreachable": "连上了但发不过去",
    "no_reply": "对方一直没回复",
    "rejected": "对方拒绝了请求",
    "cancelled": "发起方撤回了",
    "error": "对方执行报错",
    "timeout": "等超时了",
    "unknown": "原因不明",
}

#: 中文名后面的补充说明，告诉用户「为什么会这样」。
_ERR_HINT = {
    "PEER_OFFLINE": "对方没启动客户端、没登录，或没连上中继",
    "session_not_found": "会话已删除，或者地址填错了",
    "peer_unreachable": "网络或客户端异常，消息没能送达",
    "no_reply": "多次催办后对方仍未回复",
    "rejected": "对方主动拒绝，或会话正忙不接新任务",
    "cancelled": "在等待过程中被手动停掉了",
    "error": "对方处理时抛了异常",
    "timeout": "超出等待时限自动结束",
    "unknown": "没有记录具体原因",
}

#: 耗时分桶边界（秒）。
_DUR_BUCKETS = [
    (0, 1, "<1s"), (1, 5, "1-5s"), (5, 30, "5-30s"),
    (30, 120, "30s-2m"), (120, 600, "2-10m"), (600, None, ">10m"),
]


def _pct(values: list[float], q: float) -> Optional[float]:
    """线性插值分位数。values 必须已排序。"""
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    pos = (len(values) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (pos - lo)


@trace_router.get("/analytics", summary="统计面板：概览/趋势/失败/耗时/参与者/时段")
async def analytics(
    request: Request,
    user: str = Query("", description="按用户名过滤（client_id）"),
    role: str = Query("all", pattern="^(all|sent|handled)$"),
    state: str = Query("all", description="all/active/done/failed/cancelled/notified/silent"),
    q: str = Query("", description="关键词：摘要 / task_id / conv_id / 会话ID"),
    days: Optional[int] = Query(None, ge=0, le=3650),
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None),
) -> dict[str, Any]:
    """统计当前筛选范围内的这批通信。

    口径说明（界面上也要标出来，不然容易被误读）：
    - 成功率的分母**排除单向通知**：notify 投递成功即终结，没有「成功/失败」之分，
      把它算进分母会把成功率抬高；
    - 耗时按 finished_at - created_at 现算（库里没有 total_seconds 列），
      未终结的任务不计入；
    - 趋势按天补零，缺失的日子显示为 0，否则柱子间距会骗人。
    """
    _guard(request)
    store = _store(request)
    now = time.time()
    since, until = _range(store, days, from_, to)

    total, rows = await asyncio.to_thread(
        store.query_tasks,
        since=since, until=until, user=user, role=role,
        state=state, keyword=q, limit=_ANALYTICS_CAP, offset=0,
    )
    truncated = len(rows) >= _ANALYTICS_CAP

    def g(row: Any, name: str, default: Any = None) -> Any:
        return getattr(row, name, default)

    # ---------- 概览 ----------
    notify_total = sum(1 for r in rows if g(r, "kind") == "notify")
    req_total = len(rows) - notify_total
    by_state: dict[str, int] = {}
    for r in rows:
        st = str(g(r, "state") or "unknown")
        by_state[st] = by_state.get(st, 0) + 1
    done = by_state.get("done", 0)
    failed = by_state.get("failed", 0)
    cancelled = by_state.get("cancelled", 0)
    active = sum(1 for r in rows if not g(r, "terminal"))

    # 成功率：只在「有成败之分」的请求里算
    judged = done + failed + cancelled
    success_rate = (done / judged) if judged else None

    durations: list[float] = []
    for r in rows:
        # 单向通知投递即终结，耗时恒为 0，混进来会把 P50 压到失真。
        # 耗时只统计「真正等过对端」的请求。
        if g(r, "kind") == "notify":
            continue
        fin, crt = g(r, "finished_at"), g(r, "created_at")
        if fin and crt and fin >= crt:
            durations.append(float(fin) - float(crt))
    durations.sort()

    # ---------- 趋势（按天，补零）----------
    day_map: dict[str, dict[str, int]] = {}
    for r in rows:
        crt = g(r, "created_at")
        if not crt:
            continue
        key = datetime.fromtimestamp(float(crt)).strftime("%Y-%m-%d")
        slot = day_map.setdefault(key, {"total": 0, "done": 0, "failed": 0, "notify": 0})
        slot["total"] += 1
        st = g(r, "state")
        if g(r, "kind") == "notify":
            slot["notify"] += 1
        elif st == "failed":
            slot["failed"] += 1
        elif st == "done":
            slot["done"] += 1
    trend = []
    if day_map:
        first = datetime.strptime(min(day_map), "%Y-%m-%d")
        last = datetime.strptime(max(day_map), "%Y-%m-%d")
        span = (last - first).days
        # 跨度太大就不逐日补零了，否则几年历史会返回上千个空点
        if span <= 120:
            for i in range(span + 1):
                d = (first + timedelta(days=i)).strftime("%Y-%m-%d")
                v = day_map.get(d) or {"total": 0, "done": 0, "failed": 0, "notify": 0}
                trend.append({"date": d, **v})
        else:
            trend = [{"date": d, **day_map[d]} for d in sorted(day_map)]

    # ---------- 失败分析 ----------
    err_map: dict[str, int] = {}
    err_states: dict[str, set] = {}
    fail_user: dict[str, int] = {}
    fail_pair: dict[str, int] = {}
    for r in rows:
        if g(r, "state") not in ("failed", "cancelled"):
            continue
        code = str(g(r, "error_code") or "unknown")
        err_map[code] = err_map.get(code, 0) + 1
        # 记下每种原因到底出现在哪些状态里。
        # 前端点「发起方撤回了」时如果硬带 state=failed，
        # 而这批记录实际是 cancelled，两个条件互斥就会筛出 0 条（实测踩过）。
        err_states.setdefault(code, set()).add(str(g(r, "state") or ""))
        who = str(g(r, "requester") or "?")
        fail_user[who] = fail_user.get(who, 0) + 1
        pair = f'{who} → {g(r, "handler") or "?"}'
        fail_pair[pair] = fail_pair.get(pair, 0) + 1

    # ---------- 耗时分桶 ----------
    buckets = []
    for lo, hi, label in _DUR_BUCKETS:
        n = sum(1 for d in durations if d >= lo and (hi is None or d < hi))
        buckets.append({"label": label, "count": n, "lo": lo, "hi": hi})

    slowest = sorted(
        (
            {
                "task_id": g(r, "task_id"), "summary": g(r, "summary") or "",
                "requester": g(r, "requester"), "handler": g(r, "handler"),
                "seconds": float(g(r, "finished_at")) - float(g(r, "created_at")),
                "state": g(r, "state"),
            }
            for r in rows
            if g(r, "kind") != "notify"
            and g(r, "finished_at") and g(r, "created_at")
            and float(g(r, "finished_at")) >= float(g(r, "created_at"))
        ),
        key=lambda x: -x["seconds"],
    )[:10]

    # ---------- 参与者 / 会话对 ----------
    people: dict[str, dict[str, Any]] = {}
    pair_map: dict[str, int] = {}
    for r in rows:
        req, han = str(g(r, "requester") or "?"), str(g(r, "handler") or "?")
        st = g(r, "state")
        a = people.setdefault(req, {"user": req, "sent": 0, "handled": 0, "failed": 0, "secs": []})
        a["sent"] += 1
        if st in ("failed", "cancelled"):
            a["failed"] += 1
        b = people.setdefault(han, {"user": han, "sent": 0, "handled": 0, "failed": 0, "secs": []})
        b["handled"] += 1
        fin, crt = g(r, "finished_at"), g(r, "created_at")
        if g(r, "kind") != "notify" and fin and crt and float(fin) >= float(crt):
            a["secs"].append(float(fin) - float(crt))
        pair_map[f"{req} → {han}"] = pair_map.get(f"{req} → {han}", 0) + 1
    participants = []
    for v in people.values():
        secs = v.pop("secs")
        v["avg_seconds"] = (sum(secs) / len(secs)) if secs else None
        participants.append(v)
    participants.sort(key=lambda x: -(x["sent"] + x["handled"]))

    # ---------- 时段热力：7×24 ----------
    heat = [[0] * 24 for _ in range(7)]
    for r in rows:
        crt = g(r, "created_at")
        if not crt:
            continue
        dt = datetime.fromtimestamp(float(crt))
        heat[dt.weekday()][dt.hour] += 1

    # ---------- 结构：跨用户 / 同用户、深度 ----------
    cross = sum(1 for r in rows if g(r, "requester") != g(r, "handler"))
    index = {g(r, "task_id"): r for r in rows}
    depth_map: dict[int, int] = {}
    for r in rows:
        depth_map[_depth_of(str(g(r, "task_id")), index)] = (
            depth_map.get(_depth_of(str(g(r, "task_id")), index), 0) + 1
        )

    return {
        "ts": now,
        "since": since, "until": until,
        "days": days if days is not None else store.default_days,
        "filter": {"user": user, "role": role, "state": state, "q": q},
        "truncated": truncated,
        "scanned": len(rows),
        "matched_total": total,
        "overview": {
            "total": len(rows),
            "request_total": req_total,
            "notify_total": notify_total,
            "done": done, "failed": failed, "cancelled": cancelled,
            "active": active,
            "judged": judged,
            "success_rate": success_rate,
            "avg_seconds": (sum(durations) / len(durations)) if durations else None,
            "p50": _pct(durations, 0.50),
            "p90": _pct(durations, 0.90),
            "p95": _pct(durations, 0.95),
            "max_seconds": durations[-1] if durations else None,
            "timed": len(durations),
        },
        "by_state": [{"state": k, "count": v} for k, v in sorted(by_state.items(), key=lambda x: -x[1])],
        "trend": trend,
        "failures": {
            "by_code": [
                {
                    "code": k,
                    "label": _ERR_LABEL.get(k, k),
                    "count": v,
                    "hint": _ERR_HINT.get(k, ""),
                    "states": sorted(x for x in err_states.get(k, set()) if x),
                }
                for k, v in sorted(err_map.items(), key=lambda x: -x[1])
            ],
            "by_user": [{"user": k, "count": v} for k, v in sorted(fail_user.items(), key=lambda x: -x[1])[:8]],
            "by_pair": [{"pair": k, "count": v} for k, v in sorted(fail_pair.items(), key=lambda x: -x[1])[:8]],
        },
        "durations": {"buckets": buckets, "slowest": slowest},
        "participants": participants[:12],
        "pairs": [{"pair": k, "count": v} for k, v in sorted(pair_map.items(), key=lambda x: -x[1])[:10]],
        "heatmap": heat,
        "structure": {
            "cross_user": cross,
            "same_user": len(rows) - cross,
            "depth": [{"depth": k, "count": v} for k, v in sorted(depth_map.items())],
        },
    }


@trace_router.get("/stats", summary="追踪自身运行状况")
async def trace_stats(request: Request) -> dict[str, Any]:
    _guard(request)
    return _store(request).stats()


@trace_router.post("/maintain", summary="立即做一次维护（WAL 合并，备份前建议调一下）")
async def maintain(request: Request) -> dict[str, Any]:
    """合并 WAL 到主库；仅当 RELAY_TRACE_PRUNE_DAYS > 0 时才会顺带清理过期历史。

    备份场景：WAL 模式下直接拷 trace.db 可能拿不到最新数据（那些还在 -wal 里）。
    先调这个接口再拷单个文件就是完整的；或者三个文件（db/-wal/-shm）一起拷。
    """
    _guard(request)
    store = _store(request)
    result = await asyncio.to_thread(store.maintain)
    return {"ok": True, "pruned": result, "stats": store.stats()}


@trace_router.get("/stream", summary="SSE 实时推送任务变更")
async def stream(request: Request) -> StreamingResponse:
    _guard(request)
    store = _store(request)
    queue: asyncio.Queue = asyncio.Queue(maxsize=256)
    store.subscribe(queue)

    async def gen():
        try:
            yield f"event: hello\ndata: {json.dumps(store.stats(), ensure_ascii=False)}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=_SSE_HEARTBEAT)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                yield f"event: task\ndata: {json.dumps(message, ensure_ascii=False)}\n\n"
        except asyncio.CancelledError:  # 页面关掉就是这条路径，属正常
            raise
        finally:
            store.unsubscribe(queue)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # 防 Nginx 缓冲导致「实时」变成攒一批
        },
    )


__all__ = ["trace_router"]
