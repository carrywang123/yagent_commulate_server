"""REST 接口。

给「不方便维持长连接」的调用方使用，例如 shell 里的 curl、一次性 Python 脚本、
或者别的后端服务。功能与 WebSocket 侧完全对等。
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from .config import Settings, get_settings
from .protocol import Envelope, ErrCode, MsgType, new_id
from .registry import ClientRegistry
from .router import RelayError, Router
from .security import require_token

logger = logging.getLogger(__name__)

api_router = APIRouter(prefix="/api", tags=["relay"])


# ----------------------------------------------------------------------
# 请求体
# ----------------------------------------------------------------------


class SendRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(default="rest", alias="from", description="发送方标识")
    to: str = Field(description="目标 client_id")
    payload: Any = Field(default=None, description="业务数据，任意 JSON")
    expect_reply: bool = Field(default=False, description="是否等待对方回复")
    timeout: Optional[float] = Field(default=None, description="等待回复的超时秒数")


class BroadcastRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(default="rest", alias="from")
    group: Optional[str] = Field(default=None, description="留空则发给所有在线客户端")
    payload: Any = None
    exclude: Optional[str] = Field(default=None, description="排除某个 client_id")


# ----------------------------------------------------------------------
# 工具
# ----------------------------------------------------------------------


def _router(request: Request) -> Router:
    return request.app.state.router


def _registry(request: Request) -> ClientRegistry:
    return request.app.state.registry


def _settings(request: Request) -> Settings:
    return getattr(request.app.state, "settings", None) or get_settings()


def _trace_send(
    router: Router, env: Envelope, delivered: int, exc: RelayError | None = None
) -> None:
    """REST 发出的消息也进同一份跨端链路追踪（origin=rest）。"""
    tracer = getattr(router, "tracer", None)
    if tracer is None:
        return
    tracer.on_send(
        payload=env.payload,
        relay_from=env.from_ or "",
        relay_to=env.to or "",
        delivered=delivered,
        ts=env.ts,
        origin="rest",
        error_code=exc.code if exc is not None else None,
        error_message=exc.message if exc is not None else None,
    )


def _http_error(exc: RelayError) -> HTTPException:
    status_map = {
        ErrCode.PEER_OFFLINE: 404,
        ErrCode.BAD_REQUEST: 400,
        ErrCode.UNAUTHORIZED: 401,
        ErrCode.PAYLOAD_TOO_LARGE: 413,
        ErrCode.TIMEOUT: 504,
        ErrCode.UNKNOWN_REPLY: 409,
        ErrCode.REPLY_FORBIDDEN: 403,
    }
    return HTTPException(
        status_code=status_map.get(exc.code, 500),
        detail={"code": exc.code, "message": exc.message, "ref": exc.ref},
    )


# ----------------------------------------------------------------------
# 查询类
# ----------------------------------------------------------------------


@api_router.get("/metrics", summary="运行指标")
async def metrics(request: Request, _: None = Depends(require_token)) -> dict[str, Any]:
    return _router(request).metrics()


@api_router.get("/clients", summary="在线客户端列表")
async def list_clients(request: Request, _: None = Depends(require_token)) -> dict[str, Any]:
    registry = _registry(request)
    return {
        "count": len(registry.client_ids()),
        "clients": registry.snapshot(),
        "groups": registry.groups(),
    }


@api_router.get("/clients/{client_id}", summary="查询单个客户端是否在线")
async def get_client(
    client_id: str, request: Request, _: None = Depends(require_token)
) -> dict[str, Any]:
    registry = _registry(request)
    conns = registry.get_client(client_id)
    return {
        "client_id": client_id,
        "online": bool(conns),
        "connections": [c.info() for c in conns],
    }


@api_router.get("/groups", summary="分组列表")
async def list_groups(request: Request, _: None = Depends(require_token)) -> dict[str, Any]:
    return {"groups": _registry(request).groups()}


# ----------------------------------------------------------------------
# 发送类
# ----------------------------------------------------------------------


@api_router.post("/send", summary="定向发送（可选等待回复）")
async def send(
    body: SendRequest, request: Request, _: None = Depends(require_token)
) -> dict[str, Any]:
    router = _router(request)
    settings = _settings(request)

    env = Envelope(
        id=new_id(),
        type=MsgType.FORWARD,
        from_=body.from_,
        to=body.to,
        expect_reply=body.expect_reply,
        payload=body.payload,
    )
    router.stats.messages_in += 1

    if not body.expect_reply:
        try:
            result = router.deliver(env)
        except RelayError as exc:
            router.stats.errors += 1
            _trace_send(router, env, 0, exc)
            raise _http_error(exc) from exc
        _trace_send(router, env, result.delivered)
        router.stats.forwarded += result.delivered
        return {
            "ok": True,
            "msg_id": env.id,
            "delivered": result.delivered,
            "sessions": result.sessions,
        }

    # 需要等回复
    timeout = body.timeout or settings.request_timeout
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()
    pending = router.register_pending(env, future=future, timeout=timeout)

    try:
        result = router.deliver(env, single=True)
    except RelayError as exc:
        router.drop_pending(pending.msg_id)
        router.stats.errors += 1
        _trace_send(router, env, 0, exc)
        raise _http_error(exc) from exc

    _trace_send(router, env, result.delivered)
    router.stats.forwarded += result.delivered

    try:
        reply: Envelope = await asyncio.wait_for(future, timeout=timeout + 2.0)
    except asyncio.TimeoutError as exc:
        router.drop_pending(pending.msg_id)
        router.stats.errors += 1
        raise HTTPException(
            status_code=504,
            detail={
                "code": ErrCode.TIMEOUT,
                "message": f"等待 '{body.to}' 回复超时（{timeout}s）",
                "ref": env.id,
            },
        ) from exc

    if reply.error is not None:
        raise HTTPException(
            status_code=504 if reply.error.code == ErrCode.TIMEOUT else 400,
            detail={
                "code": reply.error.code,
                "message": reply.error.message,
                "ref": env.id,
            },
        )

    return {
        "ok": True,
        "msg_id": env.id,
        "delivered": result.delivered,
        "reply": {
            "from": reply.from_,
            "id": reply.id,
            "payload": reply.payload,
            "ts": reply.ts,
        },
    }


@api_router.post("/broadcast", summary="广播 / 分组发送")
async def broadcast(
    body: BroadcastRequest, request: Request, _: None = Depends(require_token)
) -> dict[str, Any]:
    router = _router(request)
    env = Envelope(
        id=new_id(),
        type=MsgType.BROADCAST,
        from_=body.from_,
        group=body.group,
        payload=body.payload,
    )
    router.stats.messages_in += 1
    result = router.broadcast(env, group=body.group, exclude_client=body.exclude)
    router.stats.forwarded += result.delivered
    return {
        "ok": True,
        "msg_id": env.id,
        "delivered": result.delivered,
        "group": body.group,
    }


@api_router.post("/kick/{client_id}", summary="强制断开某个客户端")
async def kick(
    client_id: str, request: Request, _: None = Depends(require_token)
) -> dict[str, Any]:
    registry = _registry(request)
    conns = registry.get_client(client_id)
    if not conns:
        raise HTTPException(
            status_code=404,
            detail={"code": ErrCode.PEER_OFFLINE, "message": f"'{client_id}' 不在线"},
        )
    for conn in conns:
        conn.mark_closing("kicked")
    return {"ok": True, "client_id": client_id, "closed": len(conns), "ts": time.time()}
