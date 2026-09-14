"""在线客户端注册表。

设计要点：
1. 一个 client_id 允许有多条连接（同一个人开了多个网页 / 多个进程），
   定向消息会投递给该 client_id 的全部连接。
2. 每条连接有独立的发送队列 + writer 协程，避免多处并发写同一个 websocket。
3. 队列满 = 客户端消费太慢，直接标记断开，防止服务端内存被拖垮。
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Iterable, Optional

from fastapi import WebSocket

from .protocol import Envelope

logger = logging.getLogger(__name__)


class Connection:
    """一条 WebSocket 连接。"""

    def __init__(
        self,
        session_id: str,
        client_id: str,
        ws: WebSocket,
        *,
        outbox_size: int = 256,
        groups: Optional[Iterable[str]] = None,
        meta: Optional[dict[str, Any]] = None,
        exclusive: bool = False,
    ) -> None:
        self.session_id = session_id
        self.client_id = client_id
        self.ws = ws
        self.groups: set[str] = set(groups or ())
        self.meta: dict[str, Any] = dict(meta or {})
        # 声明独占：同一个 client_id 只允许一条这样的连接。
        # 用于“一个账号 = 一台机器”的跨端场景：若同名多开，
        # 定向消息会投给全部连接，导致同一任务被多台机器重复执行。
        # 不传此参数的客户端（ui / py_worker 多开）行为完全不变。
        self.exclusive = bool(exclusive)

        self.connected_at = time.time()
        self.last_seen = self.connected_at

        self.outbox: asyncio.Queue[Envelope] = asyncio.Queue(maxsize=outbox_size)
        self._writer_task: Optional[asyncio.Task] = None
        self._closing = False
        self.close_reason: str = ""
        # 供读循环感知“该收摊了”（写失败 / 慢消费 / 心跳超时）
        self.should_close: asyncio.Event = asyncio.Event()

        self.sent_count = 0
        self.recv_count = 0

    # ---------- 生命周期 ----------

    def start_writer(self) -> None:
        if self._writer_task is None:
            self._writer_task = asyncio.create_task(
                self._writer_loop(), name=f"writer-{self.session_id}"
            )

    async def _writer_loop(self) -> None:
        try:
            while True:
                env = await self.outbox.get()
                try:
                    await self.ws.send_json(env.dump())
                    self.sent_count += 1
                except Exception as exc:  # 连接已断
                    logger.debug(
                        "send failed session=%s client=%s: %s",
                        self.session_id,
                        self.client_id,
                        exc,
                    )
                    self.mark_closing("SEND_FAILED")
                    return
        except asyncio.CancelledError:
            raise

    async def stop_writer(self) -> None:
        task = self._writer_task
        self._writer_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    # ---------- 收发 ----------

    def send(self, env: Envelope) -> bool:
        """把消息塞进发送队列。返回 False 表示队列已满/连接正在关闭。"""
        if self._closing:
            return False
        try:
            self.outbox.put_nowait(env)
            return True
        except asyncio.QueueFull:
            logger.warning(
                "outbox full, marking slow consumer: client=%s session=%s",
                self.client_id,
                self.session_id,
            )
            self.mark_closing("SLOW_CONSUMER")
            return False

    def touch(self) -> None:
        self.last_seen = time.time()

    @property
    def closing(self) -> bool:
        return self._closing

    def mark_closing(self, reason: str = "") -> None:
        self._closing = True
        if reason and not self.close_reason:
            self.close_reason = reason
        self.should_close.set()

    async def drain(self, timeout: float = 1.0) -> None:
        """尽力把队列里剩余的消息发完（用于关闭前把 error 送出去）。"""
        deadline = asyncio.get_running_loop().time() + timeout
        while not self.outbox.empty():
            if asyncio.get_running_loop().time() > deadline:
                return
            await asyncio.sleep(0.01)

    def info(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "client_id": self.client_id,
            "groups": sorted(self.groups),
            "meta": self.meta,
            "exclusive": self.exclusive,
            "connected_at": self.connected_at,
            "last_seen": self.last_seen,
            "sent": self.sent_count,
            "recv": self.recv_count,
        }


class ClientRegistry:
    """内存注册表。

    单实例够用；未来要多节点时，只需在这一层背后接 Redis，
    上层 Router 的调用方式不变。
    """

    def __init__(self) -> None:
        self._by_session: dict[str, Connection] = {}
        self._by_client: dict[str, dict[str, Connection]] = {}
        self._groups: dict[str, set[str]] = {}  # group -> {session_id}

    # ---------- 增删 ----------

    def add(self, conn: Connection) -> None:
        self._by_session[conn.session_id] = conn
        self._by_client.setdefault(conn.client_id, {})[conn.session_id] = conn
        for g in conn.groups:
            self._groups.setdefault(g, set()).add(conn.session_id)

    def remove(self, session_id: str) -> Optional[Connection]:
        conn = self._by_session.pop(session_id, None)
        if conn is None:
            return None
        bucket = self._by_client.get(conn.client_id)
        if bucket is not None:
            bucket.pop(session_id, None)
            if not bucket:
                self._by_client.pop(conn.client_id, None)
        for g in list(conn.groups):
            members = self._groups.get(g)
            if members is not None:
                members.discard(session_id)
                if not members:
                    self._groups.pop(g, None)
        return conn

    # ---------- 查询 ----------

    def get_session(self, session_id: str) -> Optional[Connection]:
        return self._by_session.get(session_id)

    def get_client(self, client_id: str) -> list[Connection]:
        return list(self._by_client.get(client_id, {}).values())

    def is_online(self, client_id: str) -> bool:
        return bool(self._by_client.get(client_id))

    def connection_count(self, client_id: str) -> int:
        """某个 client_id 当前的连接数。独占校验用。"""
        return len(self._by_client.get(client_id, {}))

    def has_exclusive(self, client_id: str) -> bool:
        """该 client_id 是否已有一条声明独占的连接。

        独占是**连接**级属性而不是注册表级策略：谁声明谁独占，
        没声明的客户端（原有的 ui / py_worker 多开场景）行为完全不变。
        """
        return any(
            conn.exclusive for conn in self._by_client.get(client_id, {}).values()
        )

    def all_connections(self) -> list[Connection]:
        return list(self._by_session.values())

    def group_members(self, group: str) -> list[Connection]:
        return [
            conn
            for sid in self._groups.get(group, set())
            if (conn := self._by_session.get(sid)) is not None
        ]

    def client_ids(self) -> list[str]:
        return sorted(self._by_client.keys())

    def groups(self) -> dict[str, int]:
        return {g: len(members) for g, members in sorted(self._groups.items())}

    # ---------- 分组 ----------

    def join_group(self, session_id: str, group: str) -> bool:
        conn = self._by_session.get(session_id)
        if conn is None:
            return False
        conn.groups.add(group)
        self._groups.setdefault(group, set()).add(session_id)
        return True

    def leave_group(self, session_id: str, group: str) -> bool:
        conn = self._by_session.get(session_id)
        if conn is None:
            return False
        conn.groups.discard(group)
        members = self._groups.get(group)
        if members is not None:
            members.discard(session_id)
            if not members:
                self._groups.pop(group, None)
        return True

    # ---------- 统计 ----------

    def snapshot(self) -> list[dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for conn in self._by_session.values():
            item = result.setdefault(
                conn.client_id,
                {
                    "client_id": conn.client_id,
                    "connections": 0,
                    "groups": set(),
                    "meta": {},
                    "exclusive": False,
                    "connected_at": conn.connected_at,
                    "last_seen": conn.last_seen,
                },
            )
            item["connections"] += 1
            item["groups"].update(conn.groups)
            item["meta"].update(conn.meta)
            item["exclusive"] = item["exclusive"] or conn.exclusive
            item["connected_at"] = min(item["connected_at"], conn.connected_at)
            item["last_seen"] = max(item["last_seen"], conn.last_seen)
        out = []
        for item in result.values():
            item["groups"] = sorted(item["groups"])
            out.append(item)
        out.sort(key=lambda x: x["client_id"])
        return out
