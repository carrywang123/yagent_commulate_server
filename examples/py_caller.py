"""示例：Python 发起端（找某个客户端干活并拿结果）。

运行（先启动服务和 py_worker.py）：
    python examples/py_caller.py                    # 目标默认 py-worker
    python examples/py_caller.py ui-1               # 目标是某个 Vue3 页面
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk" / "python"))

from yagent_relay_client import PeerOffline, RelayClient, RelayError  # noqa: E402

URL = os.getenv("RELAY_URL", "ws://127.0.0.1:8770/ws")
TOKEN = os.getenv("RELAY_AUTH_TOKEN") or None


async def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else "py-worker"

    async with RelayClient(URL, client_id="py-caller", token=TOKEN, groups=["caller"]) as client:
        print("在线客户端:", (await client.list_clients()).get("clients"))

        # 1) 单向发送（不关心结果）
        await client.send(target, {"cmd": "notify", "text": "只是通知一下"}, need_ack=True)
        print("单向消息已投递")

        # 2) 请求-响应：等对方把结果回传
        try:
            result = await client.call(target, {"cmd": "add", "args": [1, 2, 3, 4]}, timeout=10)
            print("call 结果:", result)
        except PeerOffline as exc:
            print("目标不在线:", exc)
        except RelayError as exc:
            print("调用失败:", exc)

        # 3) 对方处理时抛错 -> 错误会原样带回
        try:
            await client.call(target, {"cmd": "fail"}, timeout=10)
        except RelayError as exc:
            print("预期内的失败:", exc)

        # 4) 广播给一个分组
        await client.broadcast({"event": "config_changed", "version": 3}, group="worker")
        print("广播已发出")


if __name__ == "__main__":
    asyncio.run(main())
