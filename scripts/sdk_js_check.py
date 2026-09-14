"""在真实浏览器里验证 JS SDK 本体（src/relay/static/relay-client.js）。

做法：打开服务首页（保证同源），在页面里 import('/sdk/relay-client.js')，
用真实 WebSocket 跑一遍 连接 / onRequest 自动回复 / call / broadcast / 断线重连。

前置：服务已在 127.0.0.1:8770 运行。
运行： "$YAGENT_PYTHON_RUNNER" run-python-script scripts/sdk_js_check.py
"""

import json
import sys

from playwright.sync_api import sync_playwright

BASE = "http://127.0.0.1:8770"

JS = r"""
async () => {
  const mod = await import('/sdk/relay-client.js');
  const { RelayClient } = mod;
  const out = {};

  const a = new RelayClient({ url: 'ws://127.0.0.1:8770/ws', clientId: 'sdk-a', groups: ['g1'] });
  const b = new RelayClient({ url: 'ws://127.0.0.1:8770/ws', clientId: 'sdk-b', groups: ['g1'] });

  const bGotOneway = new Promise((res) => b.onMessage((env) => res(env.payload)));
  const aGotBroadcast = new Promise((res) => a.onBroadcast((env) => res(env.payload)));
  b.onRequest(async (env) => {
    if (env.payload.cmd === 'boom') throw new Error('业务失败');
    return { doubled: env.payload.n * 2, by: 'sdk-b' };
  });

  await a.connect();
  await b.connect();
  out.connected = a.connected && b.connected;
  out.welcome = !!a.welcome && !!a.sessionId;

  // 1. 请求-响应
  out.call = await a.call('sdk-b', { cmd: 'double', n: 21 }, 10000);

  // 2. 对方业务抛错 -> 变成异常
  try {
    await a.call('sdk-b', { cmd: 'boom' }, 10000);
    out.errorPropagated = 'NO_ERROR';
  } catch (e) {
    out.errorPropagated = e.code;
  }

  // 3. 目标不在线
  try {
    await a.call('nobody-here', { x: 1 }, 5000);
    out.offline = 'NO_ERROR';
  } catch (e) {
    out.offline = e.code;
  }

  // 4. 单向 + ack
  out.ack = await a.send('sdk-b', { hello: 'one-way' }, { needAck: true });
  out.oneway = await bGotOneway;

  // 5. 分组广播（b 发给 g1，a 收到）
  b.broadcast({ event: 'ping-all' }, 'g1');
  out.broadcast = await aGotBroadcast;

  // 6. 在线列表
  const listing = await a.listClients();
  out.clients = listing.clients.map((c) => c.client_id).filter((id) => id.startsWith('sdk-'));

  // 7. 订阅新分组后能收到该组广播
  await a.subscribe('room9');
  const roomMsg = new Promise((res) => a.onBroadcast((env) => res(env.payload)));
  b.broadcast({ event: 'room9-only' }, 'room9');
  out.subscribed = 'timeout';
  out.subscribed = await Promise.race([
    roomMsg,
    new Promise((res) => setTimeout(() => res('timeout'), 4000)),
  ]);

  // 8. 断线自动重连
  a.ws.close();
  await new Promise((res) => setTimeout(res, 2500));
  out.reconnected = a.connected;

  a.close();
  b.close();
  return out;
}
"""

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))

    page.goto(BASE, wait_until="domcontentloaded")
    res = page.evaluate(JS)
    browser.close()

print(json.dumps(res, ensure_ascii=False, indent=2))

checks = [
    ("两个 SDK 客户端连接成功", res.get("connected") is True),
    ("welcome / sessionId 已就绪", res.get("welcome") is True),
    ("call 拿到回复", res.get("call") == {"doubled": 42, "by": "sdk-b"}),
    ("对方业务异常变成异常抛出", res.get("errorPropagated") == "HANDLER_ERROR"),
    ("目标不在线报 PEER_OFFLINE", res.get("offline") == "PEER_OFFLINE"),
    ("单向发送 + ack", (res.get("ack") or {}).get("delivered") == 1),
    ("对方收到单向消息", res.get("oneway") == {"hello": "one-way"}),
    ("分组广播", res.get("broadcast") == {"event": "ping-all"}),
    ("在线列表包含两个客户端", sorted(res.get("clients") or []) == ["sdk-a", "sdk-b"]),
    ("subscribe 后收到新分组广播", res.get("subscribed") == {"event": "room9-only"}),
    ("断线自动重连", res.get("reconnected") is True),
    ("无页面 JS 异常", not errors),
]

failed = [name for name, ok in checks if not ok]
for name, ok in checks:
    print(("  OK   " if ok else "  FAIL ") + name)
if errors:
    print("页面异常:", errors[:3])
print(f"\n===== {len(checks) - len(failed)}/{len(checks)} 通过 =====")
if failed:
    print("失败项:", failed)
    sys.exit(1)
