#!/usr/bin/env bash
# 示例：从 shell 里发消息（无需长连接，纯 curl）
#
#   bash examples/shell_send.sh
#
# 若服务端配置了鉴权，先 export RELAY_AUTH_TOKEN=xxx

set -euo pipefail

BASE="${RELAY_BASE:-http://127.0.0.1:8770}"
TARGET="${1:-ui-1}"
AUTH_HEADER=()
if [ -n "${RELAY_AUTH_TOKEN:-}" ]; then
  AUTH_HEADER=(-H "X-Auth-Token: ${RELAY_AUTH_TOKEN}")
fi

echo "== 1. 服务是否健康 =="
curl -s "${BASE}/health"; echo

echo "== 2. 谁在线 =="
curl -s "${AUTH_HEADER[@]}" "${BASE}/api/clients"; echo

echo "== 3. 单向发一条给 ${TARGET} =="
curl -s -X POST "${BASE}/api/send" \
  -H "Content-Type: application/json" "${AUTH_HEADER[@]}" \
  -d "{\"from\":\"shell\",\"to\":\"${TARGET}\",\"payload\":{\"cmd\":\"notify\",\"text\":\"来自 shell\"}}"
echo

echo "== 4. 发给 ${TARGET} 并等它回复（最多 15 秒）=="
curl -s -X POST "${BASE}/api/send" \
  -H "Content-Type: application/json" "${AUTH_HEADER[@]}" \
  -d "{\"from\":\"shell\",\"to\":\"${TARGET}\",\"payload\":{\"cmd\":\"add\",\"args\":[1,2,3]},\"expect_reply\":true,\"timeout\":15}"
echo

echo "== 5. 广播给 worker 分组 =="
curl -s -X POST "${BASE}/api/broadcast" \
  -H "Content-Type: application/json" "${AUTH_HEADER[@]}" \
  -d '{"from":"shell","group":"worker","payload":{"event":"reload"}}'
echo
