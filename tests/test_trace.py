"""跨端任务链路追踪测试。

覆盖重点是「会让看板说谎的路径」，不是接口清单：
  * 完整链路的状态推导（发出 → 投递 → 处理中 → 完成）与耗时口径
  * 各种失败结局都必须落到终态：PEER_OFFLINE / 对端不认识 / 业务 error / 撤回
  * probe 每 60s 一轮，时间线必须折叠，否则长任务会刷出几十行噪音
  * 非 crossnode 的业务消息绝不能被算成跨端任务（同一 Relay 上还挂着别人的客户端）
  * payload 只留摘要，不留全文（隐私）
  * 落盘 + 重启回放后，看板数据不能丢
  * 追踪失效（脏数据/异常）绝不能影响转发
"""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from relay.config import Settings
from relay.main import create_app
from relay.trace import PROBE_ID_PREFIX, TaskAggregator, parse_probe_task, summarize_payload

REQUESTER = "liupeiyu"
HANDLER = "张洪俊"


def make_client(tmp_path, **overrides) -> TestClient:
    settings = Settings(
        auth_token=overrides.pop("auth_token", ""),
        heartbeat_interval=overrides.pop("heartbeat_interval", 999.0),
        client_timeout=overrides.pop("client_timeout", 999.0),
        request_timeout=overrides.pop("request_timeout", 1.0),
        log_to_file=False,
        presence_broadcast=False,
        trace_dir=str(tmp_path / "trace"),
        **overrides,
    )
    return TestClient(create_app(settings))


def envelope(
    etype: str,
    msg_id: str,
    *,
    conv_id: str = "C-1",
    reply_to: str | None = None,
    from_client: str = REQUESTER,
    from_session: str = "sess-A",
    to_client: str = HANDLER,
    to_session: str = "sess-B",
    payload=None,
    code: str | None = None,
    message: str | None = None,
) -> dict:
    """构造一条 Hub 侧的跨端信封（字段与 Yagent_Desk/src/main/hub/protocol.ts 对齐）。"""
    env = {
        "v": 1,
        "msg_id": msg_id,
        "conv_id": conv_id,
        "type": etype,
        "from": {"client": from_client, "session": from_session},
        "to": {"client": to_client, "session": to_session},
        "chain": [],
        "hop": 1,
        "ttl": 86400,
        "created_at": int(time.time()),
        "payload": payload,
    }
    if reply_to:
        env["reply_to"] = reply_to
    if code:
        env["code"] = code
    if message:
        env["message"] = message
    return {"__crossnode__": 1, "envelope": env}


def send(ws, to: str, payload: dict, msg_id: str | None = None) -> None:
    frame = {"type": "send", "to": to, "payload": payload}
    if msg_id:
        frame["id"] = msg_id
    ws.send_json(frame)


def drain(ws, limit: int = 6) -> list[dict]:
    """把已到达的帧读掉，避免影响后续断言。"""
    out = []
    for _ in range(limit):
        try:
            out.append(ws.receive_json())
        except Exception:
            break
    return out


def tasks_of(client: TestClient, **params) -> list[dict]:
    resp = client.get("/api/trace/tasks", params=params or None)
    assert resp.status_code == 200, resp.text
    return resp.json()["tasks"]


def test_trace_dir_layout(tmp_path):
    """默认就是一个 SQLite 文件，不再有按天的 NDJSON。"""
    with make_client(tmp_path) as client:
        assert (tmp_path / "trace" / "trace.db").exists()
        assert list((tmp_path / "trace").glob("*.ndjson")) == []
        assert client.get("/api/trace/stats").json()["tasks"] == 0


# ----------------------------------------------------------------------
# 纯函数
# ----------------------------------------------------------------------


def test_parse_probe_task_id():
    assert parse_probe_task(f"{PROBE_ID_PREFIX}M-abc:3") == "M-abc"
    # 任务 id 自身含冒号也要能切对（取最后一个冒号）
    assert parse_probe_task(f"{PROBE_ID_PREFIX}M:x:9") == "M:x"
    assert parse_probe_task("M-abc") is None


def test_summary_prefers_message_and_truncates():
    assert summarize_payload({"message": "  改一下   登录页  "}, 200) == "改一下 登录页"
    assert summarize_payload({"message": "x" * 500}, 200) == "x" * 200
    # 没有已知字段时退化成 JSON 摘要，仍然受长度限制
    assert len(summarize_payload({"weird": "y" * 500}, 50)) == 50
    assert summarize_payload(None, 200) == ""


def test_aggregator_folds_probe_noise():
    agg = TaskAggregator()
    now = time.time()
    agg.apply({"ts": now, "ev": "request", "msg_id": "M-1", "conv_id": "C", "delivered": 1,
               "from_client": REQUESTER, "to_client": HANDLER, "summary": "活儿"})
    for i in range(1, 21):
        agg.apply({"ts": now + i * 60, "ev": "probe", "msg_id": f"{PROBE_ID_PREFIX}M-1:{i}",
                   "reply_to": "M-1", "delivered": 1})
        agg.apply({"ts": now + i * 60 + 1, "ev": "probe_ack", "msg_id": f"A{i}",
                   "reply_to": "M-1", "peer_state": "working", "delivered": 1})
    task = agg.tasks["M-1"]
    assert task.probe_count == 20 and task.probe_ack_count == 20
    # 20 轮 probe + 20 条 ack 折叠后，时间线不该超过几行
    assert len(task.timeline) <= 3, task.timeline
    folded = [it for it in task.timeline if it["ev"] == "heartbeat"][0]
    assert folded["probe_count"] == 20 and folded["ack_count"] == 20
    assert folded["peer_state"] == "working"


def test_aggregator_splits_timeline_on_state_change():
    """状态拐点（排队→开干）必须单独成行，否则看不出任务什么时候真正开始跑。"""
    agg = TaskAggregator()
    now = time.time()
    agg.apply({"ts": now, "ev": "request", "msg_id": "M-2", "delivered": 1,
               "from_client": REQUESTER, "to_client": HANDLER})
    for i, state in enumerate(["queued", "queued", "working", "working"], start=1):
        agg.apply({"ts": now + i * 60, "ev": "probe_ack", "msg_id": f"A{i}",
                   "reply_to": "M-2", "peer_state": state, "delivered": 1})
    task = agg.tasks["M-2"]
    kinds = [(it["ev"], it.get("peer_state")) for it in task.timeline]
    assert kinds == [("request", None), ("heartbeat", "queued"), ("heartbeat", "working")]
    assert task.state == "working"
    assert task.queued_at is not None and task.started_at is not None


# ----------------------------------------------------------------------
# 完整链路
# ----------------------------------------------------------------------


def test_full_lifecycle_request_to_done(tmp_path):
    with make_client(tmp_path) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json()  # welcome
            b.receive_json()

            send(a, HANDLER, envelope("request", "M-1", payload={"message": "帮我改一下登录页"}), "M-1")
            drain(b, 1)

            rows = tasks_of(client)
            assert len(rows) == 1
            task = rows[0]
            assert task["task_id"] == "M-1"
            assert task["requester"] == REQUESTER and task["handler"] == HANDLER
            assert task["handler_session"] == "sess-B"
            assert task["summary"] == "帮我改一下登录页"
            assert task["state"] == "delivered" and task["delivered"] == 1
            assert task["terminal"] is False

            # 发起方对账 → 对端回「正在处理」
            send(a, HANDLER, envelope("probe", f"{PROBE_ID_PREFIX}M-1:1", reply_to="M-1"))
            drain(b, 1)
            send(b, REQUESTER, envelope(
                "probe_ack", "A-1", reply_to="M-1",
                from_client=HANDLER, from_session="sess-B",
                to_client=REQUESTER, to_session="sess-A",
                payload={"state": "working"},
            ))
            drain(a, 1)

            task = tasks_of(client)[0]
            assert task["state"] == "working"
            assert task["started_at"] is not None
            assert task["probe_count"] == 1 and task["probe_ack_count"] == 1

            # 对端交结果
            send(b, REQUESTER, envelope(
                "response", "R-1", reply_to="M-1",
                from_client=HANDLER, from_session="sess-B",
                to_client=REQUESTER, to_session="sess-A",
                payload={"message": "改完了，见 commit abc123"},
            ))
            drain(a, 1)

            task = tasks_of(client)[0]
            assert task["state"] == "done" and task["terminal"] is True
            assert task["finished_at"] is not None
            assert task["work_seconds"] is not None
            assert task["bytes_in"] > 0 and task["bytes_out"] > 0

            detail = client.get("/api/trace/tasks/M-1").json()["task"]
            evs = [it["ev"] for it in detail["timeline"]]
            assert evs == ["request", "heartbeat", "response"]
            hb = detail["timeline"][1]
            assert hb["probe_count"] == 1 and hb["ack_count"] == 1
            assert hb["peer_state"] == "working"


def test_peer_offline_marks_failed(tmp_path):
    """对端不在线：Relay 立刻回 PEER_OFFLINE，这对跨端任务就是一种结局。"""
    with make_client(tmp_path) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a:
            a.receive_json()
            send(a, HANDLER, envelope("request", "M-2", payload={"message": "对方没开机"}), "M-2")
            err = a.receive_json()
            assert err["type"] == "error" and err["error"]["code"] == "PEER_OFFLINE"

            task = tasks_of(client)[0]
            assert task["state"] == "failed"
            assert task["error_code"] == "PEER_OFFLINE"
            assert task["delivered"] == 0
            assert task["handler"] == HANDLER  # 目标仍然要能看出是谁


def test_probe_ack_unknown_marks_failed(tmp_path):
    """对端不认识这个请求 —— Hub 侧据此判 no_reply，看板必须同步判失败。"""
    with make_client(tmp_path) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-3", payload={"message": "丢了的任务"}), "M-3")
            drain(b, 1)
            send(b, REQUESTER, envelope(
                "probe_ack", "A-3", reply_to="M-3",
                from_client=HANDLER, to_client=REQUESTER,
                payload={"state": "unknown"},
            ))
            drain(a, 1)
            task = tasks_of(client)[0]
            assert task["state"] == "failed" and task["error_code"] == "no_reply"


def test_business_error_and_cancel(tmp_path):
    with make_client(tmp_path) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()

            send(a, HANDLER, envelope("request", "M-4", payload={"message": "会被拒的活"}), "M-4")
            drain(b, 1)
            send(b, REQUESTER, envelope(
                "error", "E-4", reply_to="M-4", from_client=HANDLER, to_client=REQUESTER,
                code="rejected", message="用户拒接",
            ))
            drain(a, 1)

            send(a, HANDLER, envelope("request", "M-5", payload={"message": "我要撤回的活"}), "M-5")
            drain(b, 1)
            send(a, HANDLER, envelope("cancel", "X-5", reply_to="M-5"))
            drain(b, 1)

            by_id = {t["task_id"]: t for t in tasks_of(client)}
            assert by_id["M-4"]["state"] == "failed"
            assert by_id["M-4"]["error_code"] == "rejected"
            assert by_id["M-4"]["error_message"] == "用户拒接"
            assert by_id["M-5"]["state"] == "cancelled"


def test_notify_is_terminal_on_arrival(tmp_path):
    """notify 按协议没有回执，收下即终态，不能永远挂在「进行中」。"""
    with make_client(tmp_path) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("notify", "N-1", payload={"message": "同步一下进度"}), "N-1")
            drain(b, 1)
            task = tasks_of(client)[0]
            assert task["kind"] == "notify" and task["state"] == "notified"
            assert task["terminal"] is True


def test_non_crossnode_traffic_is_ignored(tmp_path):
    """同一个 Relay 上还挂着别人的 ui / py_worker，它们的消息不是跨端任务。"""
    with make_client(tmp_path) as client:
        with client.websocket_connect("/ws?client_id=ui-1") as a, \
             client.websocket_connect("/ws?client_id=py-worker") as b:
            a.receive_json(); b.receive_json()
            send(a, "py-worker", {"cmd": "add", "args": [1, 2]})
            drain(b, 1)
            # 连拼错 key 的都不该被当成任务（宁可漏记，不可乱记）
            send(a, "py-worker", {"__crossnode": 1, "envelope": {"msg_id": "x", "type": "request"}})
            drain(b, 1)
            assert tasks_of(client) == []
            assert client.get("/api/trace/stats").json()["events_total"] == 0


# ----------------------------------------------------------------------
# 按用户定位（看板左栏的核心需求）
# ----------------------------------------------------------------------


def test_overview_and_filter_by_user_role(tmp_path):
    with make_client(tmp_path) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            # liupeiyu 发 2 条给张洪俊
            send(a, HANDLER, envelope("request", "M-6", payload={"message": "活一"}), "M-6")
            send(a, HANDLER, envelope("request", "M-7", payload={"message": "活二"}), "M-7")
            drain(b, 2)
            # 张洪俊 回敬 1 条给 liupeiyu
            send(b, REQUESTER, envelope(
                "request", "M-8", conv_id="C-2",
                from_client=HANDLER, from_session="sess-B",
                to_client=REQUESTER, to_session="sess-A",
                payload={"message": "反向的活"},
            ), "M-8")
            drain(a, 1)

            data = client.get("/api/trace/overview").json()
            users = {u["client_id"]: u for u in data["users"]}
            assert users[REQUESTER]["sent_total"] == 2
            assert users[REQUESTER]["handled_total"] == 1
            assert users[HANDLER]["sent_total"] == 1
            assert users[HANDLER]["handled_total"] == 2
            assert users[HANDLER]["online"] is True
            assert data["kpi"]["active"] == 3

            sent = tasks_of(client, user=REQUESTER, role="sent")
            assert {t["task_id"] for t in sent} == {"M-6", "M-7"}
            handled = tasks_of(client, user=REQUESTER, role="handled")
            assert {t["task_id"] for t in handled} == {"M-8"}
            both = tasks_of(client, user=REQUESTER, role="all")
            assert len(both) == 3

            # 关键词命中摘要
            assert {t["task_id"] for t in tasks_of(client, q="反向")} == {"M-8"}
            # 关键词命中会话 ID（排查时最常用）
            assert len(tasks_of(client, q="sess-B")) == 3
            # 状态过滤
            assert len(tasks_of(client, state="active")) == 3
            assert tasks_of(client, state="done") == []


def test_task_detail_404_and_stats(tmp_path):
    with make_client(tmp_path) as client:
        assert client.get("/api/trace/tasks/nope").status_code == 404
        stats = client.get("/api/trace/stats").json()
        assert stats["enabled"] is True
        # 默认永不删除，看板默认只看 14 天
        assert stats["retention"] == "forever" and stats["prune_days"] == 0
        assert stats["default_days"] == 14
        assert stats["db_path"].endswith("trace.db")
        assert client.get("/trace").status_code == 200


# ----------------------------------------------------------------------
# 持久化（这是“不希望数据丢失”的核心保障）
# ----------------------------------------------------------------------


def test_data_survives_restart_and_is_written_immediately(tmp_path):
    """写入是同步的：不调任何 flush、直接用另一个连接读库就能看到。"""
    with make_client(tmp_path) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-9", payload={"message": "重启也要在"}), "M-9")
            drain(b, 1)

            # 不经过服务，直接查 sqlite 文件
            db_path = tmp_path / "trace" / "trace.db"
            assert db_path.exists()
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            try:
                rows = conn.execute(
                    "SELECT task_id, requester, handler, summary, state FROM tasks"
                ).fetchall()
                events = conn.execute(
                    "SELECT ev, msg_id, summary FROM events ORDER BY id"
                ).fetchall()
            finally:
                conn.close()
            assert rows == [("M-9", REQUESTER, HANDLER, "重启也要在", "delivered")]
            assert events[0] == ("request", "M-9", "重启也要在")

    # 新进程（同一个库）应该能直接查到，而不靠任何回放文件
    with make_client(tmp_path) as client2:
        rows = tasks_of(client2)
        assert len(rows) == 1 and rows[0]["task_id"] == "M-9"
        assert rows[0]["state"] == "delivered"
        assert rows[0]["summary"] == "重启也要在"


def test_reply_after_restart_does_not_lose_history(tmp_path):
    """重启后才收到 response：必须接在原任务上，而不是新建一个残缺任务。

    这是真实会发生的场景：跨端任务动辄几十分钟，中心服期间重启过。
    """
    with make_client(tmp_path) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-R", payload={"message": "长任务"}), "M-R")
            drain(b, 1)

    with make_client(tmp_path) as client2:
        with client2.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client2.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(b, REQUESTER, envelope(
                "response", "R-R", reply_to="M-R",
                from_client=HANDLER, to_client=REQUESTER,
                payload={"message": "重启后交的活"},
            ))
            drain(a, 1)
            task = tasks_of(client2)[0]
            assert task["task_id"] == "M-R"
            assert task["state"] == "done"
            # 关键：发起方与摘要没有因为重启而丢
            assert task["requester"] == REQUESTER
            assert task["summary"] == "长任务"
            assert task["partial"] is False


def test_evicted_task_is_reloaded_from_db(tmp_path):
    """内存只缓存 1 条时，老任务的回复仍不得覆盖库里的完整记录。"""
    with make_client(tmp_path, trace_max_tasks=1) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-old", payload={"message": "老任务"}), "M-old")
            drain(b, 1)
            # 再发一条把它从内存里挤掉
            send(a, HANDLER, envelope("request", "M-new", payload={"message": "新任务"}), "M-new")
            drain(b, 1)
            send(b, REQUESTER, envelope(
                "response", "R-old", reply_to="M-old",
                from_client=HANDLER, to_client=REQUESTER, payload={"message": "还是干完了"},
            ))
            drain(a, 1)

            detail = client.get("/api/trace/tasks/M-old").json()["task"]
            assert detail["state"] == "done"
            assert detail["requester"] == REQUESTER and detail["summary"] == "老任务"
            assert detail["partial"] is False


def test_raw_events_include_every_probe(tmp_path):
    """时间线是折叠的，但原始明细里每一轮 probe 都在（不丢数据）。"""
    with make_client(tmp_path) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-P", payload={"message": "多轮对账"}), "M-P")
            drain(b, 1)
            for index in range(1, 6):
                send(a, HANDLER, envelope(
                    "probe", f"{PROBE_ID_PREFIX}M-P:{index}", reply_to="M-P"))
                drain(b, 1)
                send(b, REQUESTER, envelope(
                    "probe_ack", f"A-{index}", reply_to="M-P",
                    from_client=HANDLER, to_client=REQUESTER, payload={"state": "working"},
                ))
                drain(a, 1)

            detail = client.get("/api/trace/tasks/M-P").json()["task"]
            assert [it["ev"] for it in detail["timeline"]] == ["request", "heartbeat"]
            assert detail["probe_count"] == 5 and detail["probe_ack_count"] == 5

            raw = client.get("/api/trace/tasks/M-P/events").json()
            kinds = [e["ev"] for e in raw["events"]]
            assert kinds.count("probe") == 5 and kinds.count("probe_ack") == 5
            assert raw["count"] == 11  # 1 request + 5 probe + 5 ack


def test_probe_events_can_be_skipped_to_save_space(tmp_path):
    """关掉 probe 明细后，events 表不再存心跳，但轮数统计仍准。"""
    with make_client(tmp_path, trace_store_probe_events=False) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-S", payload={"message": "省空间"}), "M-S")
            drain(b, 1)
            send(a, HANDLER, envelope("probe", f"{PROBE_ID_PREFIX}M-S:1", reply_to="M-S"))
            drain(b, 1)
            send(b, REQUESTER, envelope(
                "probe_ack", "A-S", reply_to="M-S",
                from_client=HANDLER, to_client=REQUESTER, payload={"state": "working"},
            ))
            drain(a, 1)

            raw = client.get("/api/trace/tasks/M-S/events").json()
            assert [e["ev"] for e in raw["events"]] == ["request"]
            task = tasks_of(client)[0]
            assert task["probe_count"] == 1 and task["probe_ack_count"] == 1
            assert task["state"] == "working"


def test_default_config_never_prunes(tmp_path):
    with make_client(tmp_path) as client:
        tracer = client.app.state.tracer
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-K", payload={"message": "不该被删"}), "M-K")
            drain(b, 1)
        # 维护任务跑一百次也不会动数据
        for _ in range(3):
            assert tracer.maintain() == {"events": 0, "tasks": 0}
        assert len(tasks_of(client)) == 1


def test_prune_only_when_explicitly_configured(tmp_path):
    with make_client(tmp_path, trace_prune_days=1) as client:
        tracer = client.app.state.tracer
        old = time.time() - 86400 * 30
        # 直接造一条三十天前的任务
        tracer.record({
            "ts": old, "ev": "request", "msg_id": "M-OLD", "conv_id": "C",
            "from_client": REQUESTER, "to_client": HANDLER, "delivered": 1,
            "created_at": old, "summary": "老得该清了",
        })
        tracer.record({
            "ts": old + 1, "ev": "response", "msg_id": "R-OLD", "reply_to": "M-OLD",
            "from_client": HANDLER, "to_client": REQUESTER, "delivered": 1,
        })
        assert len(tasks_of(client, days=0)) == 1
        removed = tracer.maintain()
        assert removed["tasks"] == 1 and removed["events"] == 2


# ----------------------------------------------------------------------
# 时间范围筛选与分页
# ----------------------------------------------------------------------


def test_time_range_filters(tmp_path):
    with make_client(tmp_path) as client:
        tracer = client.app.state.tracer
        now = time.time()
        # 造三条：今天 / 10 天前 / 100 天前
        for name, age in (("M-today", 0), ("M-10d", 10), ("M-100d", 100)):
            ts = now - 86400 * age
            tracer.record({
                "ts": ts, "ev": "request", "msg_id": name, "conv_id": "C",
                "from_client": REQUESTER, "to_client": HANDLER, "delivered": 1,
                "created_at": ts, "summary": f"任务 {name}",
            })

        assert {t["task_id"] for t in tasks_of(client, days=1)} == {"M-today"}
        assert {t["task_id"] for t in tasks_of(client, days=14)} == {"M-today", "M-10d"}
        # days 不传 = 看板默认（14 天）
        assert {t["task_id"] for t in tasks_of(client)} == {"M-today", "M-10d"}
        # days=0 = 全部历史，一条也不能漏
        assert len(tasks_of(client, days=0)) == 3

        # 自定义区间：只要 100 天前那一天
        day = datetime.fromtimestamp(now - 86400 * 100).strftime("%Y-%m-%d")
        picked = tasks_of(client, **{"from": day, "to": f"{day} 23:59:59"})
        assert {t["task_id"] for t in picked} == {"M-100d"}

        # overview 的统计口径跟着范围走
        assert client.get("/api/trace/overview", params={"days": 1}).json()["kpi"]["tasks_total"] == 1
        assert client.get("/api/trace/overview", params={"days": 0}).json()["kpi"]["tasks_total"] == 3


def test_pagination(tmp_path):
    with make_client(tmp_path) as client:
        tracer = client.app.state.tracer
        now = time.time()
        for index in range(25):
            ts = now - index * 60
            tracer.record({
                "ts": ts, "ev": "request", "msg_id": f"M-{index:02d}", "conv_id": "C",
                "from_client": REQUESTER, "to_client": HANDLER, "delivered": 1,
                "created_at": ts, "summary": f"第 {index} 条",
            })
        first = client.get("/api/trace/tasks", params={"limit": 10, "offset": 0}).json()
        second = client.get("/api/trace/tasks", params={"limit": 10, "offset": 10}).json()
        third = client.get("/api/trace/tasks", params={"limit": 10, "offset": 20}).json()
        assert first["total"] == second["total"] == third["total"] == 25
        assert len(first["tasks"]) == 10 and len(third["tasks"]) == 5
        ids = {t["task_id"] for t in first["tasks"] + second["tasks"] + third["tasks"]}
        assert len(ids) == 25  # 三页无重叠、无遗漏


def test_bad_time_input_is_rejected(tmp_path):
    with make_client(tmp_path) as client:
        resp = client.get("/api/trace/tasks", params={"from": "上周二"})
        assert resp.status_code == 400


def test_keyword_with_sql_wildcards_is_safe(tmp_path):
    """关键词走参数化查询，引号与通配符不能把 SQL 弄坏。"""
    with make_client(tmp_path) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-Q", payload={"message": "100% 完成"}), "M-Q")
            drain(b, 1)
        assert len(tasks_of(client, q="100%")) == 1
        assert tasks_of(client, q="'; DROP TABLE tasks; --") == []
        # 表还在
        assert len(tasks_of(client)) == 1


def test_rebuild_tasks_from_events(tmp_path):
    """tasks 是派生数据，必须能从 events 完整重建。"""
    with make_client(tmp_path) as client:
        tracer = client.app.state.tracer
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-B", payload={"message": "重建验证"}), "M-B")
            drain(b, 1)
            send(b, REQUESTER, envelope(
                "response", "R-B", reply_to="M-B",
                from_client=HANDLER, to_client=REQUESTER, payload={"message": "ok"},
            ))
            drain(a, 1)

        before = client.get("/api/trace/tasks/M-B").json()["task"]
        # 把 tasks 表清空（模拟聚合口径变更 / 快照损坏）
        tracer.db.conn.execute("DELETE FROM tasks")
        tracer.db.conn.commit()
        tracer.agg.tasks.clear()
        assert tasks_of(client, days=0) == []

        assert tracer.rebuild() == 1
        after = client.get("/api/trace/tasks/M-B").json()["task"]
        for key in ("task_id", "requester", "handler", "summary", "state", "bytes_in"):
            assert after[key] == before[key], key


# ----------------------------------------------------------------------
# 落盘 / 回放 / 隐私
# ----------------------------------------------------------------------


def test_persist_and_replay_after_restart(tmp_path):
    """旧名保留：现在验证的是 SQLite 持久化（不再有 NDJSON 回放）。"""
    with make_client(tmp_path) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-P9", payload={"message": "持久化"}), "M-P9")
            drain(b, 1)

    with make_client(tmp_path) as client2:
        rows = tasks_of(client2)
        assert len(rows) == 1 and rows[0]["task_id"] == "M-P9"


def test_summary_length_capped_by_config(tmp_path):
    with make_client(tmp_path, trace_summary_chars=20) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-10", payload={"message": "长" * 100}), "M-10")
            drain(b, 1)
            assert tasks_of(client)[0]["summary"] == "长" * 20


def test_trace_disabled_keeps_relay_working(tmp_path):
    with make_client(tmp_path, trace_enabled=False) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-11", payload={"message": "照常转发"}), "M-11")
            forwarded = b.receive_json()
            assert forwarded["type"] == "forward"
            assert forwarded["payload"]["envelope"]["msg_id"] == "M-11"
        assert client.get("/api/trace/tasks").status_code == 503
        # 关掉时连数据库文件都不建
        assert not (tmp_path / "trace" / "trace.db").exists()


def test_broken_tracer_never_breaks_forwarding(tmp_path):
    """追踪器炸了也只能自己烂掉，绝不允许影响转发。"""
    with make_client(tmp_path) as client:
        class Boom:
            enabled = True

            def on_send(self, **kwargs):
                raise RuntimeError("boom")

            def flush(self):
                return 0

            def maintain(self):
                return {"events": 0, "tasks": 0}

            def close(self):
                pass

        client.app.state.router.tracer = Boom()
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-12", payload={"message": "转发必须成功"}), "M-12")
            forwarded = b.receive_json()
            assert forwarded["type"] == "forward"


def test_rest_send_is_traced(tmp_path):
    """shell / 脚本走 REST 发出的跨端消息同样要能追踪到。"""
    with make_client(tmp_path) as client:
        with client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            b.receive_json()
            resp = client.post("/api/send", json={
                "from": REQUESTER,
                "to": HANDLER,
                "payload": envelope("request", "M-13", payload={"message": "来自脚本"}),
            })
            assert resp.status_code == 200
            drain(b, 1)
            task = tasks_of(client)[0]
            assert task["task_id"] == "M-13" and task["state"] == "delivered"
            detail = client.get("/api/trace/tasks/M-13").json()["task"]
            assert detail["timeline"][0]["origin"] == "rest"


@pytest.mark.parametrize("state", ["working", "queued", "deferred", "pending_retry"])
def test_all_peer_states_are_mapped(tmp_path, state):
    with make_client(tmp_path) as client:
        with client.websocket_connect(f"/ws?client_id={REQUESTER}") as a, \
             client.websocket_connect(f"/ws?client_id={HANDLER}") as b:
            a.receive_json(); b.receive_json()
            send(a, HANDLER, envelope("request", "M-14", payload={"message": "状态映射"}), "M-14")
            drain(b, 1)
            send(b, REQUESTER, envelope(
                "probe_ack", "A-14", reply_to="M-14",
                from_client=HANDLER, to_client=REQUESTER, payload={"state": state},
            ))
            drain(a, 1)
            assert tasks_of(client)[0]["state"] == state
