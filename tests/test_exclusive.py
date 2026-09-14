"""独占接入（exclusive）的测试。

背景：一个 client_id 默认允许多条连接（同一个人开多个网页 / 多个 py_worker 进程），
定向消息会投给**全部**连接。这个语义对 UI 和 worker 是对的，但对
"一个账号 = 一台机器" 的跨端场景是灾难：同一条任务会被多台机器各执行一次。

因此增加 exclusive 声明，由客户端自己决定要不要独占。
不传该参数的客户端行为必须**完全不变** —— 下面专门有测试锁住这条。
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from relay.config import Settings
from relay.main import create_app
from relay.protocol import ErrCode
from relay.ws import WS_CLOSE_DUPLICATE


def make_client(**overrides) -> TestClient:
    settings = Settings(
        auth_token=overrides.pop("auth_token", ""),
        heartbeat_interval=overrides.pop("heartbeat_interval", 999.0),
        client_timeout=overrides.pop("client_timeout", 999.0),
        request_timeout=overrides.pop("request_timeout", 5.0),
        presence_broadcast=overrides.pop("presence_broadcast", False),
        **overrides,
    )
    return TestClient(create_app(settings))


def recv_until(ws, *types, skip=("presence", "ping"), limit=10):
    for _ in range(limit):
        env = ws.receive_json()
        if env["type"] in types:
            return env
        if env["type"] not in skip:
            return env
    raise AssertionError(f"没有收到 {types}")


# ---------------------------------------------------------------- 向后兼容


def test_default_still_allows_multiple_connections():
    """不传 exclusive 时必须保持原有多连接语义，这是向后兼容的底线。"""
    client = make_client()
    with client.websocket_connect("/ws?client_id=ui-1") as first:
        recv_until(first, "welcome")
        with client.websocket_connect("/ws?client_id=ui-1") as second:
            welcome = recv_until(second, "welcome")
            assert welcome["type"] == "welcome", welcome
            listed = client.get("/api/clients").json()
            entry = next(x for x in listed["clients"] if x["client_id"] == "ui-1")
            assert entry["connections"] == 2, entry
            assert entry["exclusive"] is False, entry


def test_default_multi_connection_receives_on_all():
    """多连接下单向消息投给全部连接——原有语义，不能被改坏。"""
    client = make_client()
    with client.websocket_connect("/ws?client_id=ui-1") as first:
        recv_until(first, "welcome")
        with client.websocket_connect("/ws?client_id=ui-1") as second:
            recv_until(second, "welcome")
            with client.websocket_connect("/ws?client_id=sender") as sender:
                recv_until(sender, "welcome")
                sender.send_json({"type": "send", "to": "ui-1", "payload": {"n": 1}})
                assert recv_until(first, "forward")["payload"] == {"n": 1}
                assert recv_until(second, "forward")["payload"] == {"n": 1}


# ---------------------------------------------------------------- 独占语义


def test_exclusive_rejects_second_connection():
    client = make_client()
    with client.websocket_connect("/ws?client_id=liupeiyu&exclusive=1") as first:
        recv_until(first, "welcome")
        with pytest.raises(Exception):
            with client.websocket_connect("/ws?client_id=liupeiyu&exclusive=1") as second:
                # 服务端会先回一帧 error 再关闭
                frame = second.receive_json()
                assert frame["type"] == "error", frame
                assert frame["error"]["code"] == ErrCode.DUPLICATE_CLIENT, frame
                second.receive_json()  # 触发关闭异常


def test_exclusive_blocks_non_exclusive_newcomer():
    """已有独占连接时，普通连接也不能再接入，否则消息还是会被投两份。"""
    client = make_client()
    with client.websocket_connect("/ws?client_id=liupeiyu&exclusive=1") as first:
        recv_until(first, "welcome")
        with pytest.raises(Exception):
            with client.websocket_connect("/ws?client_id=liupeiyu") as second:
                frame = second.receive_json()
                assert frame["type"] == "error", frame
                assert frame["error"]["code"] == ErrCode.DUPLICATE_CLIENT, frame
                second.receive_json()


def test_exclusive_rejected_when_plain_holder_exists():
    """反向：已有普通连接时，要求独占的新连接也应被拒绝。"""
    client = make_client()
    with client.websocket_connect("/ws?client_id=liupeiyu") as first:
        recv_until(first, "welcome")
        with pytest.raises(Exception):
            with client.websocket_connect("/ws?client_id=liupeiyu&exclusive=1") as second:
                frame = second.receive_json()
                assert frame["type"] == "error", frame
                assert frame["error"]["code"] == ErrCode.DUPLICATE_CLIENT, frame
                second.receive_json()


def test_exclusive_released_after_disconnect():
    """独占连接断开后，标识必须能被重新占用，否则重启客户端就永久连不上了。"""
    client = make_client()
    with client.websocket_connect("/ws?client_id=liupeiyu&exclusive=1") as first:
        recv_until(first, "welcome")
    # 退出 with 即断开
    with client.websocket_connect("/ws?client_id=liupeiyu&exclusive=1") as again:
        welcome = recv_until(again, "welcome")
        assert welcome["payload"]["client_id"] == "liupeiyu", welcome


def test_exclusive_does_not_affect_other_client_ids():
    client = make_client()
    with client.websocket_connect("/ws?client_id=liupeiyu&exclusive=1") as first:
        recv_until(first, "welcome")
        with client.websocket_connect("/ws?client_id=zhangsan&exclusive=1") as second:
            welcome = recv_until(second, "welcome")
            assert welcome["payload"]["client_id"] == "zhangsan", welcome


def test_exclusive_flag_visible_in_rest():
    client = make_client()
    with client.websocket_connect("/ws?client_id=liupeiyu&exclusive=1") as conn:
        recv_until(conn, "welcome")
        entry = next(
            x for x in client.get("/api/clients").json()["clients"]
            if x["client_id"] == "liupeiyu"
        )
        assert entry["exclusive"] is True, entry
        assert entry["connections"] == 1, entry


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_exclusive_accepts_truthy_spellings(value):
    client = make_client()
    with client.websocket_connect(f"/ws?client_id=u&exclusive={value}") as conn:
        recv_until(conn, "welcome")
        entry = next(
            x for x in client.get("/api/clients").json()["clients"] if x["client_id"] == "u"
        )
        assert entry["exclusive"] is True, (value, entry)


@pytest.mark.parametrize("value", ["0", "false", "no", "off", ""])
def test_exclusive_ignores_falsy_spellings(value):
    client = make_client()
    with client.websocket_connect(f"/ws?client_id=u&exclusive={value}") as conn:
        recv_until(conn, "welcome")
        entry = next(
            x for x in client.get("/api/clients").json()["clients"] if x["client_id"] == "u"
        )
        assert entry["exclusive"] is False, (value, entry)


def test_exclusive_via_hello_payload():
    """不带查询参数、走 hello 帧注册时也要能声明独占。"""
    client = make_client()
    with client.websocket_connect("/ws") as first:
        first.send_json(
            {"type": "hello", "from": "liupeiyu", "payload": {"exclusive": True}}
        )
        recv_until(first, "welcome")
        entry = next(
            x for x in client.get("/api/clients").json()["clients"]
            if x["client_id"] == "liupeiyu"
        )
        assert entry["exclusive"] is True, entry


def test_close_code_is_duplicate():
    """关闭码要能区分出"重名"，客户端据此决定不要无脑重连。"""
    client = make_client()
    with client.websocket_connect("/ws?client_id=liupeiyu&exclusive=1") as first:
        recv_until(first, "welcome")
        try:
            with client.websocket_connect("/ws?client_id=liupeiyu&exclusive=1") as second:
                second.receive_json()
                second.receive_json()
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "code", None)
            if code is not None:
                assert code == WS_CLOSE_DUPLICATE, code
