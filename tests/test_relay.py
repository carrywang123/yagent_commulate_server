"""端到端测试：WebSocket 转发、请求-响应、广播、REST、鉴权。"""

from __future__ import annotations

import json
import threading
import time

import pytest
from fastapi.testclient import TestClient

from relay.config import Settings
from relay.main import create_app


def make_client(**overrides) -> TestClient:
    settings = Settings(
        auth_token=overrides.pop("auth_token", ""),
        heartbeat_interval=overrides.pop("heartbeat_interval", 999.0),
        client_timeout=overrides.pop("client_timeout", 999.0),
        request_timeout=overrides.pop("request_timeout", 5.0),
        presence_broadcast=overrides.pop("presence_broadcast", True),
        **overrides,
    )
    return TestClient(create_app(settings))


def recv_until(ws, *types, skip=("presence", "ping"), limit=10):
    """跳过噪音消息，读到指定类型为止。"""
    for _ in range(limit):
        env = ws.receive_json()
        if env["type"] in types:
            return env
        if env["type"] not in skip:
            return env
    raise AssertionError(f"没有收到 {types}")


# ----------------------------------------------------------------------


def test_health_and_metrics():
    with make_client() as client:
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

        r = client.get("/api/metrics")
        assert r.status_code == 200
        assert r.json()["online_clients"] == 0


def test_welcome_on_connect():
    with make_client() as client:
        with client.websocket_connect("/ws?client_id=a&groups=ui,worker") as ws:
            env = ws.receive_json()
            assert env["type"] == "welcome"
            assert env["payload"]["client_id"] == "a"
            assert env["payload"]["session_id"]
            assert set(env["payload"]["groups"]) == {"ui", "worker"}


def test_hello_handshake_without_query():
    with make_client() as client:
        with client.websocket_connect("/ws") as ws:
            ws.send_json(
                {"type": "hello", "from": "b", "payload": {"groups": ["ui"], "meta": {"k": 1}}}
            )
            env = ws.receive_json()
            assert env["type"] == "welcome"
            assert env["payload"]["client_id"] == "b"


def test_missing_client_id_rejected():
    with make_client() as client:
        with client.websocket_connect("/ws") as ws:
            ws.send_json({"type": "send", "to": "x"})
            env = ws.receive_json()
            assert env["type"] == "error"
            assert env["error"]["code"] == "BAD_REQUEST"


def test_direct_forward():
    with make_client() as client:
        with client.websocket_connect("/ws?client_id=alice") as a, client.websocket_connect(
            "/ws?client_id=bob"
        ) as b:
            assert a.receive_json()["type"] == "welcome"
            assert b.receive_json()["type"] == "welcome"

            a.send_json(
                {"type": "send", "id": "m1", "to": "bob", "payload": {"hi": 1}, "need_ack": True}
            )

            got = recv_until(b, "forward")
            assert got["type"] == "forward"
            assert got["from"] == "alice"
            assert got["to"] == "bob"
            assert got["payload"] == {"hi": 1}
            assert got["id"] == "m1"

            ack = recv_until(a, "ack")
            assert ack["payload"]["delivered"] == 1


def test_sender_identity_cannot_be_forged():
    with make_client() as client:
        with client.websocket_connect("/ws?client_id=alice") as a, client.websocket_connect(
            "/ws?client_id=bob"
        ) as b:
            a.receive_json()
            b.receive_json()
            a.send_json({"type": "send", "to": "bob", "from": "root", "payload": 1})
            got = recv_until(b, "forward")
            assert got["from"] == "alice"


def test_request_reply():
    with make_client() as client:
        with client.websocket_connect("/ws?client_id=caller") as a, client.websocket_connect(
            "/ws?client_id=worker"
        ) as b:
            a.receive_json()
            b.receive_json()

            a.send_json(
                {
                    "type": "send",
                    "id": "req-1",
                    "to": "worker",
                    "payload": {"n": 21},
                    "expect_reply": True,
                }
            )

            req = recv_until(b, "forward")
            assert req["expect_reply"] is True
            b.send_json({"type": "reply", "reply_to": req["id"], "payload": {"n": 42}})

            reply = recv_until(a, "reply")
            assert reply["type"] == "reply"
            assert reply["from"] == "worker"
            assert reply["payload"] == {"n": 42}
            assert reply["reply_to"] == "req-1"


def test_reply_from_third_party_rejected():
    with make_client() as client:
        with client.websocket_connect("/ws?client_id=caller") as a, client.websocket_connect(
            "/ws?client_id=worker"
        ) as b, client.websocket_connect("/ws?client_id=evil") as c:
            for ws in (a, b, c):
                ws.receive_json()

            a.send_json(
                {"type": "send", "id": "req-2", "to": "worker", "payload": 1, "expect_reply": True}
            )
            req = recv_until(b, "forward")

            c.send_json({"type": "reply", "reply_to": req["id"], "payload": "hacked"})
            err = recv_until(c, "error")
            assert err["error"]["code"] == "REPLY_FORBIDDEN"


def test_peer_offline():
    with make_client() as client:
        with client.websocket_connect("/ws?client_id=alice") as a:
            a.receive_json()
            a.send_json({"type": "send", "id": "m9", "to": "ghost", "payload": {}})
            err = recv_until(a, "error")
            assert err["error"]["code"] == "PEER_OFFLINE"
            assert err["error"]["ref"] == "m9"


def test_broadcast_and_group():
    with make_client(presence_broadcast=False) as client:
        with client.websocket_connect("/ws?client_id=a&groups=ui") as a, client.websocket_connect(
            "/ws?client_id=b&groups=ui"
        ) as b, client.websocket_connect("/ws?client_id=c&groups=worker") as c:
            for ws in (a, b, c):
                ws.receive_json()

            a.send_json({"type": "broadcast", "group": "ui", "payload": "to-ui"})
            got = recv_until(b, "broadcast")
            assert got["payload"] == "to-ui"
            assert got["group"] == "ui"

            # c 不在 ui 组，不该收到；再发一条全局广播验证 c 的队列头是全局那条
            a.send_json({"type": "broadcast", "payload": "to-all"})
            got_c = recv_until(c, "broadcast")
            assert got_c["payload"] == "to-all"


def test_subscribe_unsubscribe():
    with make_client(presence_broadcast=False) as client:
        with client.websocket_connect("/ws?client_id=a") as a, client.websocket_connect(
            "/ws?client_id=b"
        ) as b:
            a.receive_json()
            b.receive_json()

            b.send_json({"type": "subscribe", "id": "s1", "payload": {"groups": ["room1"]}})
            ack = recv_until(b, "ack")
            assert "room1" in ack["payload"]["groups"]

            a.send_json({"type": "broadcast", "group": "room1", "payload": "hi"})
            assert recv_until(b, "broadcast")["payload"] == "hi"


def test_multi_connection_same_client_id():
    with make_client(presence_broadcast=False) as client:
        with client.websocket_connect("/ws?client_id=ui") as u1, client.websocket_connect(
            "/ws?client_id=ui"
        ) as u2, client.websocket_connect("/ws?client_id=py") as p:
            for ws in (u1, u2, p):
                ws.receive_json()
            p.send_json({"type": "send", "to": "ui", "payload": "both"})
            assert recv_until(u1, "forward")["payload"] == "both"
            assert recv_until(u2, "forward")["payload"] == "both"


def test_list_clients():
    with make_client(presence_broadcast=False) as client:
        with client.websocket_connect("/ws?client_id=a&groups=ui") as a:
            a.receive_json()
            a.send_json({"type": "list", "id": "l1"})
            res = recv_until(a, "list_result")
            ids = [c["client_id"] for c in res["payload"]["clients"]]
            assert "a" in ids
            assert res["payload"]["groups"]["ui"] == 1


def test_unknown_type():
    with make_client() as client:
        with client.websocket_connect("/ws?client_id=a") as a:
            a.receive_json()
            a.send_json({"type": "nonsense"})
            err = recv_until(a, "error")
            assert err["error"]["code"] == "UNKNOWN_TYPE"


def test_bad_json():
    with make_client() as client:
        with client.websocket_connect("/ws?client_id=a") as a:
            a.receive_json()
            a.send_text("{ not json")
            err = recv_until(a, "error")
            assert err["error"]["code"] == "BAD_REQUEST"


# ---------------------------- REST ----------------------------


def test_rest_send_to_ws_client():
    with make_client(presence_broadcast=False) as client:
        with client.websocket_connect("/ws?client_id=ui") as ws:
            ws.receive_json()
            r = client.post("/api/send", json={"from": "shell", "to": "ui", "payload": {"a": 1}})
            assert r.status_code == 200, r.text
            assert r.json()["delivered"] == 1

            got = recv_until(ws, "forward")
            assert got["from"] == "shell"
            assert got["payload"] == {"a": 1}


def test_rest_send_offline_404():
    with make_client() as client:
        r = client.post("/api/send", json={"to": "ghost", "payload": 1})
        assert r.status_code == 404
        assert r.json()["detail"]["code"] == "PEER_OFFLINE"


def test_rest_call_with_reply():
    with make_client(presence_broadcast=False) as client:
        with client.websocket_connect("/ws?client_id=worker") as ws:
            ws.receive_json()

            result: dict = {}

            def do_call():
                r = client.post(
                    "/api/send",
                    json={
                        "from": "shell",
                        "to": "worker",
                        "payload": {"n": 2},
                        "expect_reply": True,
                        "timeout": 5,
                    },
                )
                result["status"] = r.status_code
                result["body"] = r.json()

            th = threading.Thread(target=do_call)
            th.start()

            req = recv_until(ws, "forward")
            assert req["expect_reply"] is True
            ws.send_json({"type": "reply", "reply_to": req["id"], "payload": {"n": 4}})

            th.join(timeout=15)
            assert result.get("status") == 200, result
            assert result["body"]["reply"]["payload"] == {"n": 4}


def test_rest_broadcast():
    with make_client(presence_broadcast=False) as client:
        with client.websocket_connect("/ws?client_id=a&groups=ui") as a:
            a.receive_json()
            r = client.post("/api/broadcast", json={"group": "ui", "payload": "hello"})
            assert r.status_code == 200
            assert r.json()["delivered"] == 1
            assert recv_until(a, "broadcast")["payload"] == "hello"


def test_rest_clients_and_kick():
    with make_client(presence_broadcast=False) as client:
        with client.websocket_connect("/ws?client_id=a") as a:
            a.receive_json()
            r = client.get("/api/clients")
            assert r.json()["count"] == 1

            r = client.get("/api/clients/a")
            assert r.json()["online"] is True

            r = client.post("/api/kick/a")
            assert r.status_code == 200
            assert r.json()["closed"] == 1

        r = client.get("/api/clients/a")
        assert r.json()["online"] is False


# ---------------------------- 鉴权 ----------------------------


def test_auth_rest():
    with make_client(auth_token="s3cr3t") as client:
        assert client.get("/api/clients").status_code == 401
        assert client.get("/api/clients", headers={"X-Auth-Token": "s3cr3t"}).status_code == 200
        assert (
            client.get("/api/clients", headers={"Authorization": "Bearer s3cr3t"}).status_code
            == 200
        )
        # health 不需要鉴权，方便探活
        assert client.get("/health").status_code == 200


def test_auth_ws_rejected():
    from starlette.websockets import WebSocketDisconnect

    with make_client(auth_token="s3cr3t") as client:
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect("/ws?client_id=a") as ws:
                ws.receive_json()
        assert exc.value.code == 4401


def test_auth_ws_accepted():
    with make_client(auth_token="s3cr3t") as client:
        with client.websocket_connect("/ws?client_id=a&token=s3cr3t") as ws:
            assert ws.receive_json()["type"] == "welcome"


# ---------------------------- 其他 ----------------------------


def test_ping_pong():
    with make_client() as client:
        with client.websocket_connect("/ws?client_id=a") as ws:
            ws.receive_json()
            ws.send_json({"type": "ping", "id": "p1"})
            env = recv_until(ws, "pong")
            assert env["reply_to"] == "p1"


def test_presence_broadcast():
    with make_client(presence_broadcast=True) as client:
        with client.websocket_connect("/ws?client_id=a") as a:
            a.receive_json()  # welcome
            with client.websocket_connect("/ws?client_id=b") as b:
                b.receive_json()
                env = a.receive_json()
                assert env["type"] == "presence"
                assert env["payload"]["client_id"] == "b"
                assert env["payload"]["online"] is True
            env = a.receive_json()
            assert env["type"] == "presence"
            assert env["payload"]["online"] is False


def test_payload_too_large():
    with make_client(max_message_bytes=200) as client:
        with client.websocket_connect("/ws?client_id=a") as ws:
            ws.receive_json()
            ws.send_json({"type": "send", "to": "b", "payload": "x" * 500})
            err = recv_until(ws, "error")
            assert err["error"]["code"] == "PAYLOAD_TOO_LARGE"


def test_reply_timeout_notifies_caller():
    with make_client(request_timeout=1.0) as client:
        with client.websocket_connect("/ws?client_id=caller") as a, client.websocket_connect(
            "/ws?client_id=worker"
        ) as b:
            a.receive_json()
            b.receive_json()
            a.send_json(
                {"type": "send", "id": "t1", "to": "worker", "payload": 1, "expect_reply": True}
            )
            recv_until(b, "forward")
            # 不回复，等服务端超时清理
            env = recv_until(a, "error", limit=20)
            assert env["error"]["code"] == "TIMEOUT"
            assert env["error"]["ref"] == "t1"
