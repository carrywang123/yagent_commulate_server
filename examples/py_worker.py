"""示例：Python 常驻工作端（接收任务并回复）。

运行：
    python examples/py_worker.py                 # 默认 client_id = py-worker
    python examples/py_worker.py my-worker

它会：
1. 连到中转服务并加入 worker 分组
2. 别人 call 它时，执行"任务"并把结果回复回去
3. 别人单向 send 时，只打印
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk" / "python"))

from yagent_relay_client import RelayClient  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

URL = os.getenv("RELAY_URL", "ws://127.0.0.1:8770/ws")
TOKEN = os.getenv("RELAY_AUTH_TOKEN") or None


async def main() -> None:
    client_id = sys.argv[1] if len(sys.argv) > 1 else "py-worker"
    client = RelayClient(
        URL,
        client_id=client_id,
        token=TOKEN,
        groups=["worker"],
        meta={"kind": "python", "pid": os.getpid()},
    )

    @client.on_request
    async def handle_request(env: dict):
        """别人 call 我 —— 返回值会自动作为回复发回去。"""
        payload = env.get("payload") or {}
        print(f"[任务] 来自 {env.get('from')}: {payload}")

        cmd = payload.get("cmd") if isinstance(payload, dict) else None
        if cmd == "add":
            return {"ok": True, "result": sum(payload.get("args", []))}
        if cmd == "slow":
            await asyncio.sleep(float(payload.get("seconds", 2)))
            return {"ok": True, "result": "done"}
        if cmd == "fail":
            raise RuntimeError("这是故意抛出的错误")
        return {"ok": True, "echo": payload, "worker": client_id}

    @client.on_message
    async def handle_message(env: dict):
        print(f"[单向] 来自 {env.get('from')}: {env.get('payload')}")

    @client.on_broadcast
    async def handle_broadcast(env: dict):
        print(f"[广播] 来自 {env.get('from')} @{env.get('group') or '全部'}: {env.get('payload')}")

    @client.on_presence
    async def handle_presence(env: dict):
        p = env.get("payload") or {}
        print(f"[状态] {p.get('client_id')} {'上线' if p.get('online') else '下线'}")

    await client.connect()
    print(f"worker '{client_id}' 已上线，等待任务... (Ctrl+C 退出)")
    try:
        await client.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        await client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nbye")
