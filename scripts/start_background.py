"""后台启动进程（不占用当前终端），立即输出子进程 PID。

主要供自动化联调使用；日常开发直接用 run.bat / run.sh 即可。

用法：
    python scripts/start_background.py            # 启动中转服务，日志 temp/logs/server.log
    python scripts/start_background.py worker     # 启动示例 worker，日志 temp/logs/worker.log
    python scripts/start_background.py server 8880

停止：taskkill /F /PID <pid>（Windows）或 kill <pid>
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

target = sys.argv[1] if len(sys.argv) > 1 else "server"
port = sys.argv[2] if len(sys.argv) > 2 else "8770"

py = ROOT / ".venv" / "Scripts" / "python.exe"
if not py.exists():
    py = ROOT / ".venv" / "bin" / "python"
if not py.exists():
    py = Path(sys.executable)

if target == "server":
    cmd = [str(py), "-m", "relay", "--port", port]
    log_name = "server.log"
elif target == "worker":
    cmd = [str(py), str(ROOT / "examples" / "py_worker.py")]
    log_name = "worker.log"
else:
    raise SystemExit(f"未知目标 '{target}'，可选：server / worker")

log_path = ROOT / "temp" / "logs" / log_name
log_path.parent.mkdir(parents=True, exist_ok=True)

env = dict(os.environ)
env["PYTHONPATH"] = str(ROOT / "src")
env["PYTHONUNBUFFERED"] = "1"

flags = 0
if sys.platform == "win32":
    flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP

with open(log_path, "ab") as f:
    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        env=env,
        stdout=f,
        stderr=f,
        stdin=subprocess.DEVNULL,
        creationflags=flags,
        close_fds=True,
    )

print(proc.pid)
