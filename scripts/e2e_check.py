"""端到端联调检查：对真实运行中的服务（默认 127.0.0.1:8770）跑一遍完整场景。

使用：
    1. 先启动服务：run.bat  （或 python scripts/start_background.py）
    2. 运行：.venv/Scripts/python scripts/e2e_check.py

覆盖：WS 连接/welcome、presence、在线列表、单向转发+ack、请求-响应、
异常回传、目标离线、分组广播、REST 各接口、多端同身份、回复超时、自动重连。
"""

import asyncio
import json
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "sdk" / "python"))

from yagent_relay_client import PeerOffline, RelayClient, RelayError, RelayRestClient

WS = "ws://127.0.0.1:8770/ws"
HTTP = "http://127.0.0.1:8770"

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print(("  OK   " if ok else "  FAIL ") + name + (f"  -> {detail}" if detail else ""))


async def main():
    rest = RelayRestClient(HTTP, from_id="shell")
    check("REST /health", rest.health().get("status") == "ok")

    # ---- worker 端（模拟 Vue3 / python 接收方）----
    worker = RelayClient(WS, client_id="e2e-worker", groups=["worker", "ui"])
    got_oneway = asyncio.Event()
    oneway_box = {}
    bcast_box = {}
    bcast_evt = asyncio.Event()

    @worker.on_request
    async def on_req(env):
        p = env["payload"]
        if p.get("cmd") == "add":
            return {"sum": sum(p["args"])}
        if p.get("cmd") == "fail":
            raise RuntimeError("boom")
        if p.get("cmd") == "slow":
            await asyncio.sleep(p.get("seconds", 3))
            return {"ok": True}
        return {"echo": p}

    @worker.on_message
    async def on_msg(env):
        oneway_box["env"] = env
        got_oneway.set()

    @worker.on_broadcast
    async def on_bc(env):
        bcast_box["env"] = env
        bcast_evt.set()

    presence_box = []

    @worker.on_presence
    async def on_pres(env):
        presence_box.append(env["payload"])

    await worker.connect(timeout=10)
    check("WS 连接 + welcome", worker.connected, f"session={worker.session_id}")

    # ---- caller 端（模拟 python 发起方）----
    caller = RelayClient(WS, client_id="e2e-caller", groups=["caller"])
    await caller.connect(timeout=10)
    check("第二个客户端连接", caller.connected)

    await asyncio.sleep(0.3)
    check("上下线通知(presence)", any(p["client_id"] == "e2e-caller" for p in presence_box),
          str(presence_box))

    # 1. 在线列表
    listing = await caller.list_clients()
    ids = {c["client_id"] for c in listing["clients"]}
    check("WS 查询在线列表", {"e2e-worker", "e2e-caller"} <= ids, str(sorted(ids)))

    # 2. 单向转发
    ack = await caller.send("e2e-worker", {"text": "one-way"}, need_ack=True)
    await asyncio.wait_for(got_oneway.wait(), timeout=5)
    check("WS 单向转发 + ack", oneway_box["env"]["payload"]["text"] == "one-way" and ack["delivered"] == 1,
          json.dumps(ack, ensure_ascii=False))

    # 3. 请求-响应
    r = await caller.call("e2e-worker", {"cmd": "add", "args": [1, 2, 3, 4]}, timeout=10)
    check("WS 请求-响应 call", r == {"sum": 10}, str(r))

    # 4. 对方处理异常
    try:
        await caller.call("e2e-worker", {"cmd": "fail"}, timeout=10)
        check("对方异常回传", False, "没有抛错")
    except RelayError as e:
        check("对方异常回传", e.code == "HANDLER_ERROR", str(e))

    # 5. 目标不在线
    try:
        await caller.call("ghost-client", {"x": 1}, timeout=5)
        check("目标不在线报错", False, "没有抛错")
    except PeerOffline as e:
        check("目标不在线报错", True, e.code)
    except RelayError as e:
        check("目标不在线报错", e.code == "PEER_OFFLINE", str(e))

    # 6. 分组广播
    await caller.broadcast({"event": "reload"}, group="worker")
    await asyncio.wait_for(bcast_evt.wait(), timeout=5)
    check("分组广播", bcast_box["env"]["payload"]["event"] == "reload")

    # 7. REST -> WS 单向
    got_oneway.clear()
    res = rest.send("e2e-worker", {"text": "from-rest"})
    await asyncio.wait_for(got_oneway.wait(), timeout=5)
    check("REST 单向发送 -> WS 收到", oneway_box["env"]["payload"]["text"] == "from-rest"
          and res["delivered"] == 1)

    # 8. REST 请求-响应（在线程里发，避免阻塞事件循环）
    box = {}

    def do_rest_call():
        try:
            box["r"] = rest.call("e2e-worker", {"cmd": "add", "args": [10, 20]}, timeout=10)
        except Exception as e:  # noqa: BLE001
            box["err"] = repr(e)

    th = threading.Thread(target=do_rest_call)
    th.start()
    while th.is_alive():
        await asyncio.sleep(0.05)
    check("REST 请求-响应（shell 场景）", box.get("r") == {"sum": 30}, str(box))

    # 9. REST 查询 / 广播
    check("REST 在线查询", rest.is_online("e2e-worker") and not rest.is_online("nobody"))
    bcast_evt.clear()
    rest.broadcast({"event": "rest-bc"}, group="ui")
    await asyncio.wait_for(bcast_evt.wait(), timeout=5)
    check("REST 分组广播", bcast_box["env"]["payload"]["event"] == "rest-bc")

    # 10. 同一 client_id 多连接：单向消息两端都收到
    ui2 = RelayClient(WS, client_id="e2e-worker", groups=["worker"])
    dual = asyncio.Event()

    @ui2.on_message
    async def on_msg2(env):
        dual.set()

    await ui2.connect(timeout=10)
    got_oneway.clear()
    await caller.send("e2e-worker", {"text": "fanout"})
    await asyncio.wait_for(asyncio.gather(got_oneway.wait(), dual.wait()), timeout=5)
    check("同一 client_id 多端同时收到单向消息", True)

    # 11. 断开多余连接，验证该身份的连接数回落（只看自己，不受其他客户端干扰）
    before = len(rest._request("GET", "/api/clients/e2e-worker")["connections"])
    await ui2.close()
    await asyncio.sleep(0.8)
    after = len(rest._request("GET", "/api/clients/e2e-worker")["connections"])
    check("断开后连接数回落", before == 2 and after == 1, f"{before} -> {after}")

    # 12. 等待回复超时（worker 故意慢 4s，call 只等 1s）
    try:
        await caller.call("e2e-worker", {"cmd": "slow", "seconds": 4}, timeout=1)
        check("等待回复超时", False, "没有超时")
    except RelayError as e:
        check("等待回复超时", e.code == "TIMEOUT", str(e))

    # 13. 自动重连
    reconn = RelayClient(WS, client_id="e2e-recon", auto_reconnect=True)
    await reconn.connect(timeout=10)
    ws = reconn._ws
    await ws.close()  # 模拟网络断开
    await asyncio.sleep(2.5)
    check("掉线自动重连", reconn.connected, f"connected={reconn.connected}")
    await reconn.close()

    await caller.close()
    await worker.close()
    await asyncio.sleep(0.3)
    print("\n最终 metrics:", json.dumps(rest.metrics(), ensure_ascii=False, indent=2))

    failed = [n for n, ok, _ in results if not ok]
    print(f"\n===== {len(results) - len(failed)}/{len(results)} 通过 =====")
    if failed:
        print("失败项:", failed)
        sys.exit(1)


asyncio.run(main())
