"""YAgent Commulate Server —— Python 客户端 SDK

包含两个类：

RelayClient（异步，长连接）
    适合常驻进程：能收消息、能等回复、掉线自动重连。
        client = RelayClient("ws://127.0.0.1:8770/ws", client_id="py-worker", groups=["worker"])

        @client.on_request
        async def handle(env):          # 别人 call 我时触发，返回值自动回给对方
            return {"echo": env["payload"]}

        await client.connect()
        result = await client.call("ui-1", {"cmd": "render"}, timeout=15)

RelayRestClient（同步，零依赖）
    适合一次性脚本 / shell 调用，只用标准库 urllib：
        RelayRestClient("http://127.0.0.1:8770").send("ui-1", {"hello": 1})

依赖：RelayClient 需要 `websockets`；RelayRestClient 不需要任何第三方库。
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Awaitable, Callable, Iterable, Optional

logger = logging.getLogger(__name__)

Handler = Callable[[dict], Any]


class RelayError(Exception):
    def __init__(self, code: str, message: str = "", ref: str | None = None) -> None:
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message
        self.ref = ref


class PeerOffline(RelayError):
    pass


def _new_id() -> str:
    return uuid.uuid4().hex


# ======================================================================
# 异步长连接客户端
# ======================================================================


class RelayClient:
    def __init__(
        self,
        url: str,
        client_id: str,
        *,
        token: str | None = None,
        groups: Iterable[str] | None = None,
        meta: dict | None = None,
        auto_reconnect: bool = True,
        reconnect_max_delay: float = 30.0,
        request_timeout: float = 30.0,
        ping_interval: float = 20.0,
    ) -> None:
        self.url = url
        self.client_id = client_id
        self.token = token
        self.groups = list(groups or [])
        self.meta = dict(meta or {})
        self.auto_reconnect = auto_reconnect
        self.reconnect_max_delay = reconnect_max_delay
        self.request_timeout = request_timeout
        self.ping_interval = ping_interval

        self._ws: Any = None
        self._connected = asyncio.Event()
        self._closed = False
        self._tasks: list[asyncio.Task] = []
        self._pending: dict[str, asyncio.Future] = {}

        self._request_handler: Optional[Handler] = None
        self._message_handler: Optional[Handler] = None
        self._broadcast_handler: Optional[Handler] = None
        self._presence_handler: Optional[Handler] = None
        self._raw_handlers: dict[str, list[Handler]] = {}

        self.session_id: str | None = None
        self.welcome: dict | None = None

    # ---------------- 处理器注册（可当装饰器用） ----------------

    def on_request(self, fn: Handler) -> Handler:
        """处理别人的 call（expect_reply=True），返回值会自动作为回复发回。"""
        self._request_handler = fn
        return fn

    def on_message(self, fn: Handler) -> Handler:
        """处理别人的单向 send。"""
        self._message_handler = fn
        return fn

    def on_broadcast(self, fn: Handler) -> Handler:
        self._broadcast_handler = fn
        return fn

    def on_presence(self, fn: Handler) -> Handler:
        """别的客户端上下线时触发。"""
        self._presence_handler = fn
        return fn

    def on(self, msg_type: str) -> Callable[[Handler], Handler]:
        """按原始消息类型注册处理器。"""

        def deco(fn: Handler) -> Handler:
            self._raw_handlers.setdefault(msg_type, []).append(fn)
            return fn

        return deco

    # ---------------- 连接管理 ----------------

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    def _build_url(self) -> str:
        query = {"client_id": self.client_id}
        if self.groups:
            query["groups"] = ",".join(self.groups)
        if self.token:
            query["token"] = self.token
        sep = "&" if "?" in self.url else "?"
        return f"{self.url}{sep}{urllib.parse.urlencode(query)}"

    async def connect(self, wait: bool = True, timeout: float = 10.0) -> None:
        """启动连接（后台维持）。wait=True 时等到握手完成。"""
        if self._tasks:
            return
        self._closed = False
        self._tasks.append(asyncio.create_task(self._run(), name=f"relay-{self.client_id}"))
        if wait:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)

    async def close(self) -> None:
        self._closed = True
        self._connected.clear()
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(RelayError("CLOSED", "客户端已关闭"))
        self._pending.clear()

    async def __aenter__(self) -> "RelayClient":
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def _run(self) -> None:
        import websockets  # 延迟导入，方便只用 REST 的场景

        delay = 1.0
        while not self._closed:
            try:
                async with websockets.connect(self._build_url(), max_size=8 * 1024 * 1024) as ws:
                    self._ws = ws
                    delay = 1.0
                    logger.info("relay connected: %s", self.client_id)
                    if self.meta:
                        await self._raw_send(
                            {
                                "type": "hello",
                                "from": self.client_id,
                                "payload": {"groups": self.groups, "meta": self.meta},
                            }
                        )
                    pinger = asyncio.create_task(self._ping_loop())
                    try:
                        async for raw in ws:
                            await self._on_raw(raw)
                    finally:
                        pinger.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("relay connection error (%s): %s", self.client_id, exc)
            finally:
                self._ws = None
                self._connected.clear()

            if self._closed or not self.auto_reconnect:
                return
            logger.info("reconnecting in %.1fs ...", delay)
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                return
            delay = min(delay * 2, self.reconnect_max_delay)

    async def _ping_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.ping_interval)
                await self._raw_send({"type": "ping", "id": _new_id()})
        except asyncio.CancelledError:
            return
        except Exception:
            return

    # ---------------- 收消息 ----------------

    async def _on_raw(self, raw: str | bytes) -> None:
        try:
            env = json.loads(raw)
        except Exception:
            logger.warning("收到非法 JSON: %r", raw[:200])
            return

        mtype = env.get("type")

        for fn in self._raw_handlers.get(mtype, []):
            await self._maybe_await(fn(env))

        if mtype == "welcome":
            self.welcome = env.get("payload") or {}
            self.session_id = self.welcome.get("session_id")
            self._connected.set()
            return

        if mtype == "ping":
            await self._raw_send({"type": "pong", "reply_to": env.get("id")})
            return

        if mtype in ("pong", "ack", "list_result"):
            self._resolve(env.get("reply_to"), env)
            return

        if mtype == "reply":
            self._resolve(env.get("reply_to"), env)
            return

        if mtype == "error":
            info = env.get("error") or {}
            ref = env.get("reply_to") or info.get("ref")
            code = info.get("code", "ERROR")
            exc_cls = PeerOffline if code == "PEER_OFFLINE" else RelayError
            exc = exc_cls(code, info.get("message", ""), ref)
            if ref and ref in self._pending:
                fut = self._pending.pop(ref)
                if not fut.done():
                    fut.set_exception(exc)
            else:
                logger.warning("relay error: %s", exc)
            return

        if mtype == "forward":
            await self._handle_forward(env)
            return

        if mtype == "broadcast":
            if self._broadcast_handler:
                await self._maybe_await(self._broadcast_handler(env))
            return

        if mtype == "presence":
            if self._presence_handler:
                await self._maybe_await(self._presence_handler(env))
            return

        logger.debug("未处理的消息类型: %s", mtype)

    async def _handle_forward(self, env: dict) -> None:
        if env.get("expect_reply"):
            handler = self._request_handler
            if handler is None:
                await self._raw_send(
                    {
                        "type": "reply",
                        "id": _new_id(),
                        "reply_to": env.get("id"),
                        "error": {
                            "code": "NO_HANDLER",
                            "message": f"'{self.client_id}' 未注册请求处理器",
                        },
                    }
                )
                return
            try:
                result = await self._maybe_await(handler(env))
                await self._raw_send(
                    {
                        "type": "reply",
                        "id": _new_id(),
                        "reply_to": env.get("id"),
                        "payload": result,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("请求处理器抛出异常")
                await self._raw_send(
                    {
                        "type": "reply",
                        "id": _new_id(),
                        "reply_to": env.get("id"),
                        "error": {"code": "HANDLER_ERROR", "message": str(exc)},
                    }
                )
            return

        if self._message_handler:
            await self._maybe_await(self._message_handler(env))

    def _resolve(self, ref: str | None, env: dict) -> None:
        if not ref:
            return
        fut = self._pending.pop(ref, None)
        if fut is not None and not fut.done():
            fut.set_result(env)

    @staticmethod
    async def _maybe_await(value: Any) -> Any:
        if inspect.isawaitable(value):
            return await value
        return value

    # ---------------- 发消息 ----------------

    async def _raw_send(self, obj: dict) -> None:
        ws = self._ws
        if ws is None:
            raise RelayError("NOT_CONNECTED", "尚未连接到中转服务")
        await ws.send(json.dumps(obj, ensure_ascii=False))

    async def send(self, to: str, payload: Any, *, need_ack: bool = False) -> Optional[dict]:
        """单向发送。need_ack=True 时等服务端确认，返回 {'delivered': n, 'to': ...}。"""
        msg_id = _new_id()
        fut: asyncio.Future | None = None
        if need_ack:
            fut = asyncio.get_running_loop().create_future()
            self._pending[msg_id] = fut
        await self._raw_send(
            {
                "type": "send",
                "id": msg_id,
                "from": self.client_id,
                "to": to,
                "payload": payload,
                "need_ack": need_ack,
            }
        )
        if fut is None:
            return None
        try:
            env = await asyncio.wait_for(fut, timeout=self.request_timeout)
            return env.get("payload")
        finally:
            self._pending.pop(msg_id, None)

    async def call(self, to: str, payload: Any, *, timeout: float | None = None) -> Any:
        """请求-响应：发给 to 并等它回复，返回对方回复的 payload。"""
        timeout = timeout or self.request_timeout
        msg_id = _new_id()
        fut = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = fut
        await self._raw_send(
            {
                "type": "send",
                "id": msg_id,
                "from": self.client_id,
                "to": to,
                "payload": payload,
                "expect_reply": True,
                "timeout": timeout,
            }
        )
        try:
            env = await asyncio.wait_for(fut, timeout=timeout + 2.0)
        except asyncio.TimeoutError as exc:
            raise RelayError("TIMEOUT", f"等待 '{to}' 回复超时（{timeout}s）", msg_id) from exc
        finally:
            self._pending.pop(msg_id, None)

        if env.get("error"):
            info = env["error"]
            raise RelayError(info.get("code", "ERROR"), info.get("message", ""), msg_id)
        return env.get("payload")

    async def broadcast(self, payload: Any, *, group: str | None = None) -> None:
        await self._raw_send(
            {
                "type": "broadcast",
                "id": _new_id(),
                "from": self.client_id,
                "group": group,
                "payload": payload,
            }
        )

    async def subscribe(self, *groups: str) -> dict:
        env = await self._control("subscribe", {"groups": list(groups)})
        return env.get("payload") or {}

    async def unsubscribe(self, *groups: str) -> dict:
        env = await self._control("unsubscribe", {"groups": list(groups)})
        return env.get("payload") or {}

    async def list_clients(self) -> dict:
        env = await self._control("list", None)
        return env.get("payload") or {}

    async def _control(self, mtype: str, payload: Any) -> dict:
        msg_id = _new_id()
        fut = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = fut
        await self._raw_send(
            {"type": mtype, "id": msg_id, "from": self.client_id, "payload": payload}
        )
        try:
            return await asyncio.wait_for(fut, timeout=self.request_timeout)
        finally:
            self._pending.pop(msg_id, None)

    async def run_forever(self) -> None:
        """阻塞直到连接被关闭，适合纯接收端。"""
        while not self._closed:
            await asyncio.sleep(0.5)


# ======================================================================
# 同步 REST 客户端（零依赖，适合 shell / 一次性脚本）
# ======================================================================


class RelayRestClient:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8770",
        *,
        token: str | None = None,
        from_id: str = "rest",
        timeout: float = 35.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.from_id = from_id
        self.timeout = timeout

    def _request(self, method: str, path: str, body: dict | None = None) -> Any:
        url = f"{self.base_url}{path}"
        data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("X-Auth-Token", self.token)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8") or "{}")
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", "replace")
            try:
                detail = json.loads(raw).get("detail") or {}
            except Exception:
                detail = {"message": raw}
            code = detail.get("code", f"HTTP_{exc.code}")
            cls = PeerOffline if code == "PEER_OFFLINE" else RelayError
            raise cls(code, detail.get("message", raw), detail.get("ref")) from exc

    def health(self) -> dict:
        return self._request("GET", "/health")

    def metrics(self) -> dict:
        return self._request("GET", "/api/metrics")

    def clients(self) -> dict:
        return self._request("GET", "/api/clients")

    def is_online(self, client_id: str) -> bool:
        return bool(self._request("GET", f"/api/clients/{client_id}").get("online"))

    def send(self, to: str, payload: Any) -> dict:
        return self._request(
            "POST", "/api/send", {"from": self.from_id, "to": to, "payload": payload}
        )

    def call(self, to: str, payload: Any, timeout: float = 30.0) -> Any:
        result = self._request(
            "POST",
            "/api/send",
            {
                "from": self.from_id,
                "to": to,
                "payload": payload,
                "expect_reply": True,
                "timeout": timeout,
            },
        )
        return (result.get("reply") or {}).get("payload")

    def broadcast(self, payload: Any, group: str | None = None) -> dict:
        return self._request(
            "POST", "/api/broadcast", {"from": self.from_id, "group": group, "payload": payload}
        )


__all__ = ["RelayClient", "RelayRestClient", "RelayError", "PeerOffline"]
