"""往运行中的 Relay 灌入一批仿真的跨端任务，用来看 /trace 看板的实际效果。

这不是测试，是**联调/演示工具**：真实跨端任务要等两台机器协作才出现，
调看板样式时不可能每次都去跑一遍真链路。

它严格按 Hub 的真实帧序列发送（request → probe → probe_ack → response/error/cancel），
所以看板上呈现的一切与真实链路完全同构。

用法：
    .venv/Scripts/python scripts/trace_demo.py --url ws://127.0.0.1:8779/ws
    .venv/Scripts/python scripts/trace_demo.py --url ws://127.0.0.1:8779/ws --token xxx

注意：默认连的是本地测试实例端口。往生产 Relay(8770) 灌演示数据会污染真实看板，
需要显式指定 --url 才会那么干。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
import uuid
from typing import Any

try:
    import websockets
except ImportError:  # pragma: no cover
    raise SystemExit("需要 websockets：uv pip install websockets")

PROBE_PREFIX = "PB:"

# 仿真用的人名与任务，贴近真实使用场景
PEOPLE = ["liupeiyu", "张洪俊", "han", "qirui", "peiyuceshi"]
JOBS = [
    "帮我把登录页的验证码倒计时改成 60 秒，改完跑一遍回归",
    "这个分支合并冲突了，帮我看下 crossnode-service.ts 那段该保留谁的",
    "跑一遍 hub 的自测，把失败的断言贴给我",
    "打个 0.1.60 的包，传到 blobstore 后把 blob_id 给我",
    "帮我查下昨天 18:07 那次 peer_offline 是什么原因",
    "生成一份这周的跨端协作发布说明",
    "把 trace.html 的配色调成浅色主题看看效果",
    "帮我复现一下「排队消息暂停后消失」那个 bug",
]


def now() -> int:
    return int(time.time())


def mid(prefix: str = "M") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def wrap(envelope: dict[str, Any]) -> dict[str, Any]:
    return {"__crossnode__": 1, "envelope": envelope}


def make_envelope(
    etype: str,
    *,
    msg_id: str,
    conv_id: str,
    from_client: str,
    from_session: str,
    to_client: str,
    to_session: str,
    reply_to: str | None = None,
    payload: Any = None,
    code: str | None = None,
    message: str | None = None,
    created_offset: int = 0,
) -> dict[str, Any]:
    env: dict[str, Any] = {
        "v": 1,
        "msg_id": msg_id,
        "conv_id": conv_id,
        "type": etype,
        "from": {"client": from_client, "session": from_session},
        "to": {"client": to_client, "session": to_session},
        "chain": [],
        "hop": 1,
        "ttl": 86400,
        # 演示数据把发起时间往前挪，否则所有任务耗时都是 1s，看不出耗时对比
        "created_at": now() - max(0, created_offset),
        "payload": payload,
    }
    if reply_to:
        env["reply_to"] = reply_to
    if code:
        env["code"] = code
    if message:
        env["message"] = message
    return env


class Peer:
    """一个仿真的 Hub 连接。"""

    def __init__(self, url: str, client_id: str, token: str = "") -> None:
        self.client_id = client_id
        sep = "&" if "?" in url else "?"
        self.url = f"{url}{sep}client_id={client_id}" + (f"&token={token}" if token else "")
        self.ws: Any = None

    async def __aenter__(self) -> "Peer":
        self.ws = await websockets.connect(self.url, max_size=2 * 1024 * 1024)
        await self.ws.recv()  # welcome
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self.ws is not None:
            await self.ws.close()

    async def send(self, to: str, envelope: dict[str, Any]) -> None:
        await self.ws.send(
            json.dumps(
                {"type": "send", "id": envelope["msg_id"], "to": to, "payload": wrap(envelope)},
                ensure_ascii=False,
            )
        )
        await asyncio.sleep(0.02)

    async def drain(self) -> None:
        """把入站帧读掉，避免 outbox 积压触发慢消费保护。"""
        while True:
            try:
                await asyncio.wait_for(self.ws.recv(), timeout=0.05)
            except (asyncio.TimeoutError, Exception):
                return


async def one_task(a: Peer, b: Peer, *, outcome: str, rounds: int = 2) -> None:
    """跑一条完整链路。outcome: done | failed | cancelled | running | notify"""
    conv_id = mid("C")
    task_id = mid("M")
    sess_a, sess_b = mid("S"), mid("S")
    job = random.choice(JOBS)
    #  模拟“几分钟前发起”，让看板上的耗时有层次
    age = random.choice([45, 90, 200, 380, 700, 1500, 2600, 5400])

    if outcome == "notify":
        await a.send(b.client_id, make_envelope(
            "notify", msg_id=task_id, conv_id=conv_id,
            from_client=a.client_id, from_session=sess_a,
            to_client=b.client_id, to_session=sess_b,
            payload={"message": "顺手同步一下：" + job},
            created_offset=age,
        ))
        await b.drain()
        return

    await a.send(b.client_id, make_envelope(
        "request", msg_id=task_id, conv_id=conv_id,
        from_client=a.client_id, from_session=sess_a,
        to_client=b.client_id, to_session=sess_b,
        payload={"message": job},
        created_offset=age,
    ))
    await b.drain()

    # 前几轮排队，之后开干 —— 与真实 Hub 的 probe 语义一致
    states = ["queued"] + ["working"] * max(1, rounds - 1)
    for index, state in enumerate(states, start=1):
        await a.send(b.client_id, make_envelope(
            "probe", msg_id=f"{PROBE_PREFIX}{task_id}:{index}", conv_id=conv_id,
            from_client=a.client_id, from_session=sess_a,
            to_client=b.client_id, to_session=sess_b, reply_to=task_id,
        ))
        await b.drain()
        await b.send(a.client_id, make_envelope(
            "probe_ack", msg_id=mid("A"), conv_id=conv_id,
            from_client=b.client_id, from_session=sess_b,
            to_client=a.client_id, to_session=sess_a,
            reply_to=task_id, payload={"state": state},
        ))
        await a.drain()

    if outcome == "running":
        return

    if outcome == "done":
        await b.send(a.client_id, make_envelope(
            "response", msg_id=mid("R"), conv_id=conv_id,
            from_client=b.client_id, from_session=sess_b,
            to_client=a.client_id, to_session=sess_a, reply_to=task_id,
            payload={"message": "搞定了，改了 3 个文件，回归 12/12 通过", "ok": True},
        ))
        await a.drain()
    elif outcome == "failed":
        await b.send(a.client_id, make_envelope(
            "error", msg_id=mid("E"), conv_id=conv_id,
            from_client=b.client_id, from_session=sess_b,
            to_client=a.client_id, to_session=sess_a, reply_to=task_id,
            code="rejected", message="这台机器上没有这个工程，接不了",
        ))
        await a.drain()
    elif outcome == "cancelled":
        await a.send(b.client_id, make_envelope(
            "cancel", msg_id=mid("X"), conv_id=conv_id,
            from_client=a.client_id, from_session=sess_a,
            to_client=b.client_id, to_session=sess_b, reply_to=task_id,
        ))
        await b.drain()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="ws://127.0.0.1:8779/ws")
    parser.add_argument("--token", default="")
    parser.add_argument("--tasks", type=int, default=14)
    parser.add_argument("--offline-target", default="离线的机器")
    args = parser.parse_args()

    plan = ["done"] * 6 + ["running"] * 3 + ["failed"] * 2 + ["cancelled"] + ["notify"] * 2
    random.shuffle(plan)
    plan = plan[: args.tasks]

    peers: dict[str, Peer] = {}
    try:
        for name in PEOPLE:
            peer = Peer(args.url, name, args.token)
            await peer.__aenter__()
            peers[name] = peer

        for outcome in plan:
            a, b = random.sample(PEOPLE, 2)
            await one_task(peers[a], peers[b], outcome=outcome, rounds=random.randint(1, 4))

        # 再来一条打给不在线的人：应该在看板上显示 PEER_OFFLINE 失败
        a = peers[PEOPLE[0]]
        await a.send(args.offline_target, make_envelope(
            "request", msg_id=mid("M"), conv_id=mid("C"),
            from_client=a.client_id, from_session=mid("S"),
            to_client=args.offline_target, to_session=mid("S"),
            payload={"message": "这条会因为对方不在线而失败"},
            created_offset=120,
        ))
        await a.drain()

        print(f"已灌入 {len(plan) + 1} 条仿真任务，打开 /trace 查看")
    finally:
        for peer in peers.values():
            await peer.__aexit__()


if __name__ == "__main__":
    asyncio.run(main())
