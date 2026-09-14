"""消息路由核心：定向转发、广播、请求-响应关联。"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from .config import Settings
from .protocol import Envelope, ErrCode, ErrorInfo, MsgType, new_id
from .registry import ClientRegistry, Connection

logger = logging.getLogger(__name__)

REST_ORIGIN = "__rest__"


class RelayError(Exception):
    """业务错误，会被转换成 error 信封或 HTTP 错误响应。"""

    def __init__(self, code: str, message: str = "", ref: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code
        self.message = message or code
        self.ref = ref


@dataclass
class DeliveryResult:
    msg_id: str
    delivered: int = 0
    sessions: list[str] = field(default_factory=list)
    dropped: list[str] = field(default_factory=list)  # 队列满被丢弃的 session

    @property
    def ok(self) -> bool:
        return self.delivered > 0


@dataclass
class Pending:
    """一条正在等待回复的请求。"""

    msg_id: str
    from_client: str
    to_client: str
    expire_at: float
    origin_session: Optional[str] = None       # WS 发起方的连接
    future: Optional[asyncio.Future] = None    # REST 发起方等待的 future


@dataclass
class Stats:
    started_at: float = field(default_factory=time.time)
    messages_in: int = 0
    messages_out: int = 0
    forwarded: int = 0
    broadcast: int = 0
    replies: int = 0
    errors: int = 0
    peer_offline: int = 0
    reply_timeouts: int = 0
    connections_total: int = 0


class Router:
    def __init__(self, registry: ClientRegistry, settings: Settings) -> None:
        self.registry = registry
        self.settings = settings
        self.stats = Stats()
        self._pending: dict[str, Pending] = {}
        #: 跨端任务链路追踪器（trace.TraceStore）。由 create_app 注入，
        #: 为 None 时所有埋点都是空操作 —— 转发逻辑不依赖它。
        self.tracer: Any = None

    # ------------------------------------------------------------------
    # 定向转发
    # ------------------------------------------------------------------

    def deliver(
        self,
        env: Envelope,
        *,
        exclude_session: str | None = None,
        single: bool = False,
    ) -> DeliveryResult:
        """把信封投递给 env.to 指向的连接。

        single=False：投给该 client_id 的全部连接（通知语义，多端都该收到）。
        single=True ：只投给一条连接（任务语义，同一个请求不能被执行多次），
                      取最近活跃的那条。
        """
        if not env.to:
            raise RelayError(ErrCode.BAD_REQUEST, "缺少 to 字段", env.id)

        targets = self.registry.get_client(env.to)
        if exclude_session:
            targets = [c for c in targets if c.session_id != exclude_session]

        if not targets:
            self.stats.peer_offline += 1
            raise RelayError(
                ErrCode.PEER_OFFLINE,
                f"目标客户端 '{env.to}' 不在线",
                env.id,
            )

        if single and len(targets) > 1:
            targets = [max(targets, key=lambda c: c.last_seen)]

        return self._fanout(env, targets)

    def deliver_to_session(self, session_id: str, env: Envelope) -> bool:
        conn = self.registry.get_session(session_id)
        if conn is None:
            return False
        ok = conn.send(env)
        if ok:
            self.stats.messages_out += 1
        return ok

    def _fanout(self, env: Envelope, targets: list[Connection]) -> DeliveryResult:
        result = DeliveryResult(msg_id=env.id)
        for conn in targets:
            if conn.send(env):
                result.delivered += 1
                result.sessions.append(conn.session_id)
                self.stats.messages_out += 1
            else:
                result.dropped.append(conn.session_id)
        return result

    # ------------------------------------------------------------------
    # 广播 / 分组
    # ------------------------------------------------------------------

    def broadcast(
        self,
        env: Envelope,
        *,
        group: str | None = None,
        exclude_session: str | None = None,
        exclude_client: str | None = None,
    ) -> DeliveryResult:
        if group:
            targets = self.registry.group_members(group)
        else:
            targets = self.registry.all_connections()

        if exclude_session:
            targets = [c for c in targets if c.session_id != exclude_session]
        if exclude_client:
            targets = [c for c in targets if c.client_id != exclude_client]

        self.stats.broadcast += 1
        return self._fanout(env, targets)

    # ------------------------------------------------------------------
    # 请求-响应
    # ------------------------------------------------------------------

    def register_pending(
        self,
        env: Envelope,
        *,
        origin_session: str | None = None,
        future: asyncio.Future | None = None,
        timeout: float | None = None,
    ) -> Pending:
        ttl = timeout or env.timeout or self.settings.request_timeout
        pending = Pending(
            msg_id=env.id,
            from_client=env.from_ or "",
            to_client=env.to or "",
            expire_at=time.time() + ttl,
            origin_session=origin_session,
            future=future,
        )
        self._pending[env.id] = pending
        return pending

    def drop_pending(self, msg_id: str) -> Optional[Pending]:
        return self._pending.pop(msg_id, None)

    def pending_count(self) -> int:
        return len(self._pending)

    def resolve_reply(self, env: Envelope) -> None:
        """处理一条 reply：找到原请求，把回复送回发起方。"""
        ref = env.reply_to
        if not ref:
            raise RelayError(ErrCode.BAD_REQUEST, "reply 必须携带 reply_to", env.id)

        pending = self._pending.pop(ref, None)
        if pending is None:
            raise RelayError(
                ErrCode.UNKNOWN_REPLY,
                f"没有找到待回复的请求 '{ref}'（可能已超时）",
                env.id,
            )

        # 只有当初的目标方才能回复，防止第三方冒充
        if env.from_ and pending.to_client and env.from_ != pending.to_client:
            self._pending[ref] = pending  # 放回去，别误伤真正的回复
            raise RelayError(
                ErrCode.REPLY_FORBIDDEN,
                f"'{env.from_}' 无权回复本应由 '{pending.to_client}' 处理的请求",
                env.id,
            )

        env.to = pending.from_client
        self.stats.replies += 1

        if pending.future is not None:
            if not pending.future.done():
                pending.future.set_result(env)
            return

        if pending.origin_session:
            if not self.deliver_to_session(pending.origin_session, env):
                logger.info(
                    "回复无法送达发起方（连接已断开）: session=%s ref=%s",
                    pending.origin_session,
                    ref,
                )

    def expire_pending(self, now: float | None = None) -> int:
        """清理超时的请求，并通知发起方。返回清理条数。"""
        now = now or time.time()
        expired = [p for p in self._pending.values() if p.expire_at <= now]
        for pending in expired:
            self._pending.pop(pending.msg_id, None)
            self.stats.reply_timeouts += 1
            err = Envelope(
                type=MsgType.ERROR,
                to=pending.from_client,
                reply_to=pending.msg_id,
                error=ErrorInfo(
                    code=ErrCode.TIMEOUT,
                    message=f"等待 '{pending.to_client}' 回复超时",
                    ref=pending.msg_id,
                ),
            )
            if pending.future is not None:
                if not pending.future.done():
                    pending.future.set_result(err)
            elif pending.origin_session:
                self.deliver_to_session(pending.origin_session, err)
        return len(expired)

    # ------------------------------------------------------------------
    # 上下线通知
    # ------------------------------------------------------------------

    def announce_presence(self, conn: Connection, online: bool) -> None:
        if not self.settings.presence_broadcast:
            return
        env = Envelope(
            id=new_id(),
            type=MsgType.PRESENCE,
            from_=conn.client_id,
            payload={
                "client_id": conn.client_id,
                "online": online,
                "groups": sorted(conn.groups),
                "meta": conn.meta,
                "connections": len(self.registry.get_client(conn.client_id)),
            },
        )
        self.broadcast(env, exclude_client=conn.client_id)

    # ------------------------------------------------------------------
    # 统计快照
    # ------------------------------------------------------------------

    def metrics(self) -> dict[str, Any]:
        s = self.stats
        return {
            "uptime_seconds": round(time.time() - s.started_at, 3),
            "online_clients": len(self.registry.client_ids()),
            "online_connections": len(self.registry.all_connections()),
            "groups": self.registry.groups(),
            "pending_requests": self.pending_count(),
            "messages_in": s.messages_in,
            "messages_out": s.messages_out,
            "forwarded": s.forwarded,
            "broadcast": s.broadcast,
            "replies": s.replies,
            "errors": s.errors,
            "peer_offline": s.peer_offline,
            "reply_timeouts": s.reply_timeouts,
            "connections_total": s.connections_total,
        }
