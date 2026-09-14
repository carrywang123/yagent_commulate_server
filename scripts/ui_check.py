"""用 Playwright 验证浏览器调试台：
1. 页面能连上中转服务
2. 页面 -> Python worker 的 call 能拿到回复
3. shell(REST) -> 页面 的 call 能被页面自动应答并把结果带回
"""

import json
import sys
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = "http://127.0.0.1:8770"
SHOT = ROOT / "temp" / "console.png"

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print(("  OK   " if ok else "  FAIL ") + name + (f"  -> {detail}" if detail else ""))


def rest_call(to, payload, timeout=20):
    body = json.dumps(
        {"from": "shell", "to": to, "payload": payload, "expect_reply": True, "timeout": timeout}
    ).encode()
    req = urllib.request.Request(f"{BASE}/api/send", data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout + 5) as r:
        return json.loads(r.read().decode())


from playwright.sync_api import sync_playwright  # noqa: E402

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page(viewport={"width": 1500, "height": 880})
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)

    page.goto(BASE, wait_until="domcontentloaded")
    check("调试台页面可访问", "调试台" in page.title(), page.title())

    page.fill("#cid", "ui-1")
    page.fill("#groups", "ui")
    page.click("#btnConn")
    page.wait_for_function("() => document.getElementById('status').textContent === '已连接'", timeout=15000)
    check("页面 WebSocket 连接成功", True)

    page.wait_for_timeout(1200)
    clients_text = page.inner_text("#clients")
    check("页面显示在线客户端", "py-worker" in clients_text, clients_text.replace("\n", " / "))

    # 页面 -> python worker 的 call
    page.select_option("#mode", "call")
    page.fill("#to", "py-worker")
    page.fill("#body", json.dumps({"cmd": "add", "args": [5, 6]}))
    page.click("#btnSend")
    page.wait_for_function(
        "() => document.getElementById('log').innerText.includes('reply ← py-worker')",
        timeout=15000,
    )
    log_text = page.inner_text("#log")
    check("页面 call Python worker 拿到回复", '"result": 11' in log_text,
          [l for l in log_text.splitlines() if "result" in l][:2])

    # shell(REST) -> 页面，页面自动应答
    box = {}

    def do_call():
        try:
            box["r"] = rest_call("ui-1", {"cmd": "render", "id": 7})
        except Exception as e:  # noqa: BLE001
            box["err"] = repr(e)

    th = threading.Thread(target=do_call)
    th.start()
    deadline = time.time() + 25
    while th.is_alive() and time.time() < deadline:
        page.wait_for_timeout(200)
    th.join(timeout=5)

    reply = (box.get("r") or {}).get("reply", {}).get("payload")
    check("shell(REST) call 页面并收到页面回复", reply == {"ok": True, "from": "console"},
          json.dumps(box, ensure_ascii=False))

    log_text = page.inner_text("#log")
    check("页面日志记录了收到的请求", "call ← shell" in log_text)

    page.screenshot(path=str(SHOT), full_page=False)
    check("截图已保存", SHOT.exists(), str(SHOT))

    check("页面无 JS 报错", not errors, str(errors[:3]))

    browser.close()

failed = [n for n, ok, _ in results if not ok]
print(f"\n===== {len(results) - len(failed)}/{len(results)} 通过 =====")
if failed:
    print("失败项:", failed)
    sys.exit(1)
