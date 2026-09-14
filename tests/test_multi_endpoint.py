"""多端连接下的投递语义：

- 单向消息 -> 该 client_id 的所有连接都收到（通知）
- 需要回复的请求 -> 只有一条连接收到（任务只能被执行一次）

判断"某个连接没收到某条消息"用栅栏法：随后再发一条已知消息，
如果它读到的第一条就是栅栏消息，说明前一条确实没投给它。
"""

from __future__ import annotations

from tests.test_relay import make_client, recv_until


def test_oneway_fanout():
    with make_client(presence_broadcast=False) as client:
        with client.websocket_connect("/ws?client_id=caller") as caller, client.websocket_connect(
            "/ws?client_id=ui"
        ) as ui1, client.websocket_connect("/ws?client_id=ui") as ui2:
            for ws in (caller, ui1, ui2):
                ws.receive_json()

            caller.send_json({"type": "send", "id": "n1", "to": "ui", "payload": "notice"})
            assert recv_until(ui1, "forward")["payload"] == "notice"
            assert recv_until(ui2, "forward")["payload"] == "notice"


def test_request_goes_to_single_connection():
    with make_client(presence_broadcast=False) as client:
        with client.websocket_connect("/ws?client_id=caller") as caller, client.websocket_connect(
            "/ws?client_id=ui"
        ) as ui1, client.websocket_connect("/ws?client_id=ui") as ui2:
            for ws in (caller, ui1, ui2):
                ws.receive_json()

            # 让 ui1 成为"最近活跃"的连接（服务端按 last_seen 选择投递目标）
            ui1.send_json({"type": "ping", "id": "p1"})
            assert recv_until(ui1, "pong")["reply_to"] == "p1"

            caller.send_json(
                {"type": "send", "id": "r1", "to": "ui", "payload": "task", "expect_reply": True}
            )

            task = recv_until(ui1, "forward")
            assert task["payload"] == "task"
            assert task["expect_reply"] is True

            # 栅栏：ui2 若也收到了任务，它读到的第一条就不会是 fence
            caller.send_json({"type": "send", "id": "f1", "to": "ui", "payload": "fence"})
            first_for_ui2 = recv_until(ui2, "forward")
            assert first_for_ui2["payload"] == "fence", "任务被重复投给了第二个连接"

            # ui1 也会收到栅栏消息（单向 -> 全部连接）
            assert recv_until(ui1, "forward")["payload"] == "fence"

            # 拿到任务的一方回复，发起方收到
            ui1.send_json({"type": "reply", "reply_to": task["id"], "payload": "done"})
            reply = recv_until(caller, "reply")
            assert reply["payload"] == "done"
            assert reply["reply_to"] == "r1"


def test_rest_oneway_fanout_all_connections():
    with make_client(presence_broadcast=False) as client:
        with client.websocket_connect("/ws?client_id=ui") as ui1, client.websocket_connect(
            "/ws?client_id=ui"
        ) as ui2:
            ui1.receive_json()
            ui2.receive_json()

            r = client.post("/api/send", json={"to": "ui", "payload": 1})
            assert r.status_code == 200
            assert r.json()["delivered"] == 2
            assert recv_until(ui1, "forward")["payload"] == 1
            assert recv_until(ui2, "forward")["payload"] == 1


def test_rest_request_single_delivery():
    with make_client(presence_broadcast=False) as client:
        with client.websocket_connect("/ws?client_id=ui") as ui1, client.websocket_connect(
            "/ws?client_id=ui"
        ) as ui2:
            ui1.receive_json()
            ui2.receive_json()

            ui2.send_json({"type": "ping", "id": "p2"})
            assert recv_until(ui2, "pong")["reply_to"] == "p2"

            import threading

            box: dict = {}

            def do_call():
                r = client.post(
                    "/api/send",
                    json={"to": "ui", "payload": "job", "expect_reply": True, "timeout": 5},
                )
                box["status"] = r.status_code
                box["body"] = r.json()

            th = threading.Thread(target=do_call)
            th.start()

            job = recv_until(ui2, "forward")
            assert job["payload"] == "job"
            assert box.get("body") is None  # 还在等回复

            ui2.send_json({"type": "reply", "reply_to": job["id"], "payload": "ok"})
            th.join(timeout=15)
            assert box["status"] == 200, box
            assert box["body"]["delivered"] == 1
            assert box["body"]["reply"]["payload"] == "ok"
