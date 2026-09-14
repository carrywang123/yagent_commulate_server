"""WebSocket 端点：注册、心跳、消息分发。"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from .config import Settings
from .protocol import Envelope, ErrCode, ErrorInfo, MsgType, new_id
from .registry import ClientRegistry, Connection
from .router import RelayError, Router
from .security import token_matches

logger = logging.getLogger(__name__)

ws_router = APIRouter()

HELLO_TIMEOUT = 10.0
WS_CLOSE_UNAUTHORIZED = 4401
WS_CLOSE_BAD_REQUEST = 4400
WS_CLOSE_SLOW = 4408
WS_CLOSE_DUPLICATE = 4409


def _as_bool(value: Any) -> bool:
    """宽容地解析开关量（查询参数只能是字符串）。"""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


@ws_router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    settings: Settings = websocket.app.state.settings
    router: Router = websocket.app.state.router
    registry: ClientRegistry = websocket.app.state.registry

    params = websocket.query_params
    token = params.get("token") or _header_token(websocket)
    if not token_matches(settings, token):
        await websocket.close(code=WS_CLOSE_UNAUTHORIZED, reason="unauthorized")
        return

    await websocket.accept()

    client_id: Optional[str] = params.get("client_id") or params.get("id")
    groups = _as_list(params.get("groups"))
    meta: dict[str, Any] = {}
    # 独占声明：同一个 client_id 只允许一条这样的连接。
    # 不传则保持原有的多连接语义，完全向后兼容。
    exclusive = _as_bool(params.get("exclusive"))

    # 没有在 URL 里带身份时，等待第一帧 hello
    if not client_id:
        try:
            raw = await asyncio.wait_for(websocket.receive_text(), timeout=HELLO_TIMEOUT)
        except asyncio.TimeoutError:
            await _close_with_error(
                websocket, ErrCode.BAD_REQUEST, "超时未收到 hello 消息", WS_CLOSE_BAD_REQUEST
            )
            return
        except (WebSocketDisconnect, RuntimeError):
            return

        try:
            hello = Envelope.model_validate_json(raw)
        except ValidationError as exc:
            await _close_with_error(
                websocket, ErrCode.BAD_REQUEST, f"hello 消息格式错误: {exc}", WS_CLOSE_BAD_REQUEST
            )
            return

        if hello.type != MsgType.HELLO:
            await _close_with_error(
                websocket,
                ErrCode.BAD_REQUEST,
                f"首帧必须是 hello，收到 '{hello.type}'",
                WS_CLOSE_BAD_REQUEST,
            )
            return

        payload = hello.payload if isinstance(hello.payload, dict) else {}
        client_id = hello.from_ or payload.get("client_id")
        groups = _as_list(payload.get("groups")) or groups
        meta = payload.get("meta") or {}
        exclusive = exclusive or _as_bool(payload.get("exclusive"))
        hello_token = payload.get("token")
        if settings.auth_enabled and not token_matches(settings, token or hello_token):
            await _close_with_error(
                websocket, ErrCode.UNAUTHORIZED, "token 无效或缺失", WS_CLOSE_UNAUTHORIZED
            )
            return

    if not client_id:
        await _close_with_error(
            websocket, ErrCode.BAD_REQUEST, "缺少 client_id", WS_CLOSE_BAD_REQUEST
        )
        return

    client_id = str(client_id)

    # 独占校验：先到先得。
    # 两种冲突都要拦：新连接要独占但已有人在；或者已有一条独占连接占着。
    # 拒绝新来者而不是踢走旧的，是因为旧连接可能正在处理任务，
    # 踢它会造成静默的任务丢失；而拒绝新来者能立即给出明确的错误提示。
    if exclusive and registry.is_online(client_id):
        await _close_with_error(
            websocket,
            ErrCode.DUPLICATE_CLIENT,
            f"client_id '{client_id}' 已在线（{registry.connection_count(client_id)} 条连接），"
            "本连接要求独占。请换一个标识，或先断开另一端。",
            WS_CLOSE_DUPLICATE,
        )
        logger.warning(
            "exclusive register rejected: client_id=%s existing=%d",
            client_id,
            registry.connection_count(client_id),
        )
        return
    if not exclusive and registry.has_exclusive(client_id):
        await _close_with_error(
            websocket,
            ErrCode.DUPLICATE_CLIENT,
            f"client_id '{client_id}' 已被一条独占连接占用，不允许再接入。",
            WS_CLOSE_DUPLICATE,
        )
        logger.warning("register rejected, exclusive holder exists: client_id=%s", client_id)
        return

    session_id = new_id()
    conn = Connection(
        session_id=session_id,
        client_id=client_id,
        ws=websocket,
        outbox_size=settings.outbox_size,
        groups=groups,
        meta=meta if isinstance(meta, dict) else {},
        exclusive=exclusive,
    )
    registry.add(conn)
    conn.start_writer()
    router.stats.connections_total += 1

    logger.info(
        "client online: client_id=%s session=%s groups=%s exclusive=%s",
        conn.client_id,
        session_id,
        groups,
        exclusive,
    )

    conn.send(
        Envelope(
            type=MsgType.WELCOME,
            to=conn.client_id,
            payload={
                "client_id": conn.client_id,
                "session_id": session_id,
                "groups": sorted(conn.groups),
                "server_time": conn.connected_at,
                "heartbeat_interval": settings.heartbeat_interval,
                "request_timeout": settings.request_timeout,
                "online_clients": registry.client_ids(),
            },
        )
    )
    router.announce_presence(conn, online=True)

    try:
        await _read_loop(websocket, conn, router, settings)
    except WebSocketDisconnect as exc:
        logger.info(
            "client offline: client_id=%s session=%s code=%s",
            conn.client_id,
            session_id,
            exc.code,
        )
    except Exception:
        logger.exception("websocket loop crashed: client_id=%s", conn.client_id)
    finally:
        registry.remove(session_id)
        await conn.stop_writer()
        router.announce_presence(conn, online=False)
        try:
            await websocket.close()
        except Exception:
            pass


def _header_token(websocket: WebSocket) -> Optional[str]:
    auth = websocket.headers.get("authorization")
    if auth:
        auth = auth.strip()
        if auth.lower().startswith("bearer "):
            return auth[7:].strip()
        return auth
    return websocket.headers.get("x-auth-token")


async def _close_with_error(
    websocket: WebSocket, code: str, message: str, ws_code: int
) -> None:
    try:
        await websocket.send_json(
            Envelope(type=MsgType.ERROR, error=ErrorInfo(code=code, message=message)).dump()
        )
    except Exception:
        pass
    try:
        await websocket.close(code=ws_code, reason=code)
    except Exception:
        pass


async def _read_loop(
    websocket: WebSocket, conn: Connection, router: Router, settings: Settings
) -> None:
    """同时监听「收消息」和「该关闭了」两个事件。"""
    close_waiter = asyncio.ensure_future(conn.should_close.wait())
    try:
        while True:
            receiver = asyncio.ensure_future(websocket.receive_text())
            done, _ = await asyncio.wait(
                {receiver, close_waiter}, return_when=asyncio.FIRST_COMPLETED
            )

            if close_waiter in done:
                receiver.cancel()
                reason = conn.close_reason or "server_closing"
                logger.info("closing connection: client=%s reason=%s", conn.client_id, reason)
                await conn.drain(0.5)
                code = WS_CLOSE_SLOW if reason == "SLOW_CONSUMER" else 1000
                try:
                    await websocket.close(code=code, reason=reason)
                except Exception:
                    pass
                return

            raw = receiver.result()
            conn.touch()
            conn.recv_count += 1
            router.stats.messages_in += 1

            if len(raw) > settings.max_message_bytes:
                _send_error(
                    conn,
                    router,
                    ErrCode.PAYLOAD_TOO_LARGE,
                    f"消息超过 {settings.max_message_bytes} 字节上限",
                )
                continue

            try:
                env = Envelope.model_validate_json(raw)
            except ValidationError as exc:
                _send_error(conn, router, ErrCode.BAD_REQUEST, f"消息格式错误: {exc}")
                continue
            except json.JSONDecodeError as exc:
                _send_error(conn, router, ErrCode.BAD_REQUEST, f"不是合法 JSON: {exc}")
                continue

            try:
                await _dispatch(conn, env, router, settings)
            except RelayError as exc:
                _send_error(conn, router, exc.code, exc.message, ref=exc.ref or env.id)
            except Exception as exc:  # noqa: BLE001
                logger.exception("dispatch failed: client=%s type=%s", conn.client_id, env.type)
                _send_error(conn, router, ErrCode.INTERNAL, str(exc), ref=env.id)
    finally:
        close_waiter.cancel()


def _send_error(
    conn: Connection, router: Router, code: str, message: str, ref: str | None = None
) -> None:
    router.stats.errors += 1
    conn.send(
        Envelope(
            type=MsgType.ERROR,
            to=conn.client_id,
            reply_to=ref,
            error=ErrorInfo(code=code, message=message, ref=ref),
        )
    )


def _trace_send(
    router: Router, env: Envelope, delivered: int, exc: RelayError | None = None
) -> None:
    """把一条 send 的投递结果告知追踪器。

    追踪器内部已经全程 try/except，这里再包一层是为了它根本没被注入的情况。
    非 crossnode 的业务消息会在 on_send 里被直接忽略。
    """
    tracer = getattr(router, "tracer", None)
    if tracer is None:
        return
    tracer.on_send(
        payload=env.payload,
        relay_from=env.from_ or "",
        relay_to=env.to or "",
        delivered=delivered,
        ts=env.ts,
        origin="ws",
        error_code=exc.code if exc is not None else None,
        error_message=exc.message if exc is not None else None,
    )


async def _dispatch(
    conn: Connection, env: Envelope, router: Router, settings: Settings
) -> None:
    registry = router.registry
    # 发送方身份一律以连接身份为准，禁止伪造
    env.from_ = conn.client_id
    mtype = env.type

    if mtype == MsgType.PING:
        conn.send(Envelope(type=MsgType.PONG, to=conn.client_id, reply_to=env.id))
        return

    if mtype == MsgType.PONG:
        return

    if mtype == MsgType.HELLO:
        # 允许重复 hello 用于更新分组 / 元信息
        payload = env.payload if isinstance(env.payload, dict) else {}
        for group in _as_list(payload.get("groups")):
            registry.join_group(conn.session_id, group)
        if isinstance(payload.get("meta"), dict):
            conn.meta.update(payload["meta"])
        conn.send(
            Envelope(
                type=MsgType.WELCOME,
                to=conn.client_id,
                reply_to=env.id,
                payload={
                    "client_id": conn.client_id,
                    "session_id": conn.session_id,
                    "groups": sorted(conn.groups),
                },
            )
        )
        return

    if mtype == MsgType.SEND:
        if not env.to:
            raise RelayError(ErrCode.BAD_REQUEST, "send 必须指定 to", env.id)

        forward = Envelope(
            id=env.id,
            type=MsgType.FORWARD,
            from_=conn.client_id,
            to=env.to,
            expect_reply=env.expect_reply,
            reply_to=env.reply_to,
            ts=env.ts,
            payload=env.payload,
        )

        pending = None
        if env.expect_reply:
            pending = router.register_pending(
                forward, origin_session=conn.session_id, timeout=env.timeout
            )
        try:
            result = router.deliver(forward, single=env.expect_reply)
        except RelayError as exc:
            if pending is not None:
                router.drop_pending(pending.msg_id)
            # 投递失败也要进追踪：对跨端任务而言 PEER_OFFLINE 就是一种结局
            _trace_send(router, forward, 0, exc)
            raise

        _trace_send(router, forward, result.delivered)
        router.stats.forwarded += result.delivered
        if env.need_ack:
            conn.send(
                Envelope(
                    type=MsgType.ACK,
                    to=conn.client_id,
                    reply_to=env.id,
                    payload={"delivered": result.delivered, "to": env.to},
                )
            )
        return

    if mtype == MsgType.REPLY:
        router.resolve_reply(
            Envelope(
                id=env.id,
                type=MsgType.REPLY,
                from_=conn.client_id,
                reply_to=env.reply_to,
                ts=env.ts,
                payload=env.payload,
                error=env.error,
            )
        )
        return

    if mtype == MsgType.BROADCAST:
        out = Envelope(
            id=env.id,
            type=MsgType.BROADCAST,
            from_=conn.client_id,
            group=env.group,
            ts=env.ts,
            payload=env.payload,
        )
        result = router.broadcast(out, group=env.group, exclude_session=conn.session_id)
        router.stats.forwarded += result.delivered
        if env.need_ack:
            conn.send(
                Envelope(
                    type=MsgType.ACK,
                    to=conn.client_id,
                    reply_to=env.id,
                    payload={"delivered": result.delivered, "group": env.group},
                )
            )
        return

    if mtype in (MsgType.SUBSCRIBE, MsgType.UNSUBSCRIBE):
        payload = env.payload if isinstance(env.payload, dict) else {}
        groups = _as_list(payload.get("groups")) or _as_list(env.group)
        if not groups:
            raise RelayError(ErrCode.BAD_REQUEST, "缺少 groups", env.id)
        for group in groups:
            if mtype == MsgType.SUBSCRIBE:
                registry.join_group(conn.session_id, group)
            else:
                registry.leave_group(conn.session_id, group)
        conn.send(
            Envelope(
                type=MsgType.ACK,
                to=conn.client_id,
                reply_to=env.id,
                payload={"groups": sorted(conn.groups)},
            )
        )
        return

    if mtype == MsgType.LIST:
        conn.send(
            Envelope(
                type=MsgType.LIST_RESULT,
                to=conn.client_id,
                reply_to=env.id,
                payload={
                    "clients": registry.snapshot(),
                    "groups": registry.groups(),
                },
            )
        )
        return

    raise RelayError(ErrCode.UNKNOWN_TYPE, f"不支持的消息类型 '{mtype}'", env.id)
