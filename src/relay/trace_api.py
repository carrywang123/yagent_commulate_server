"""跨端任务追踪的查询接口。

数据存在 SQLite 里（见 trace_db.py），**默认永久保留**；
这些接口只负责按条件取，不做任何删除。

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | /api/trace/overview | 顶部 KPI + 左侧用户列表（含在线状态） |
| GET | /api/trace/tasks | 任务列表，按用户 / 角色 / 状态 / 关键词 / 时间段过滤，可分页 |
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
import time
from datetime import datetime
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
    return {"ts": time.time(), "task": task.detail()}


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
