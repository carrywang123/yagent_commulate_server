#!/usr/bin/env bash
# 启动跨端文件中转服务（Blob Store）
#   bash run_blob.sh                  正常启动（8771）
#   bash run_blob.sh --reload         开发模式
#   bash run_blob.sh --port 8899      换端口
#
# 与 Relay(8770) 是两个独立进程：Relay 挂了重启即可恢复（纯内存），
# 而文件服务有磁盘状态，且大流量会与 Relay 的心跳争抢事件循环。
set -euo pipefail

cd "$(dirname "$0")"
export PYTHONPATH="src:${PYTHONPATH:-}"
export PYTHONIOENCODING=utf-8

if [ -x ".venv/Scripts/python.exe" ]; then
    PY=".venv/Scripts/python.exe"
elif [ -x ".venv/bin/python" ]; then
    PY=".venv/bin/python"
else
    PY="python"
fi

exec "$PY" -m blobstore "$@"
