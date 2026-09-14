#!/usr/bin/env bash
# 启动中转服务（Linux / macOS / Git Bash）
#   bash run.sh            # 正常启动
#   bash run.sh --reload   # 开发模式，改代码自动重启
set -euo pipefail
cd "$(dirname "$0")"

PY=".venv/Scripts/python.exe"
[ -x "$PY" ] || PY=".venv/bin/python"
[ -x "$PY" ] || PY="python"

PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}" "$PY" -m relay "$@"
