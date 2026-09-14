# Blob Store —— 跨端文件中转

与 Relay（8770）同仓库、**独立进程**，默认端口 **8771**。

## 为什么需要它

跨端消息通道（Relay）传不了文件，且这不只是"体积超限"那么简单：

| 约束 | 出处 | 后果 |
|---|---|---|
| `RELAY_MAX_MESSAGE_BYTES = 1MB` | `relay/config.py` | base64 膨胀 1.33 倍，1MB 的文件都过不去 |
| `RELAY_CLIENT_TIMEOUT = 60s`、心跳 20s | `relay/main.py:_maintenance_loop` | 大流量拖慢心跳往返 → Relay 主动 `mark_closing("HEARTBEAT_TIMEOUT")` |
| `RELAY_OUTBOX_SIZE = 256` 溢出即断连 | `relay/registry.py` | 慢消费保护是"直接断开"，不是排队 |

第二条是最危险的：Hub 收到 presence offline 后会给正在等待的跨端任务注入
`peer_disconnected`（见 `Yagent_Desk/src/main/hub/README.md` 失败路径表）。
**传文件会把消息通道搞挂，让一个正在干活的协作任务被判死。**

所以文件走独立链路：`上传 → 把地址写进消息 → 对端下载`。

## 为什么与 Relay 同仓库但独立进程

- **同仓库**：共用部署与运维习惯；`BLOB_AUTH_TOKEN` 留空时自动回落到
  `RELAY_AUTH_TOKEN`，避免"消息通道开了鉴权、文件通道忘了开"的错配。
- **独立进程**：Relay 是单进程 asyncio + 纯内存注册表，挂了重启就好；
  文件服务有磁盘状态，且大流量会与心跳争抢事件循环（见上表）。
  两者故障域必须分开 —— **磁盘满绝不能连带转发一起挂**。

## 内容寻址

`blob_id` 就是文件内容的 **sha256**。三个好处都直接对应实际问题：

1. **去重**：同一份文件重复上传不占额外空间；
2. **秒传**：上传前 HEAD 一下就知道服务端有没有，命中则零流量；
3. **校验**：下载方自己算一遍就能确认没被截断。
   缺这一条的话，半个文件会被当成成功交付，故障要到下游（解压失败、
   图片打不开）才暴露，极难定位。

原始文件名只作元数据保存，下载时用 `Content-Disposition` 还原。
**不用原始文件名做存储路径**：既消掉路径穿越，也避免可执行文件按原名落地。

## 启动

```bash
run_blob.bat                      # Windows，8771
bash run_blob.sh                  # Git Bash / Linux / macOS
run_blob.bat --reload             # 开发模式
run_blob.bat --port 8899          # 换端口
run_blob.bat --data-dir D:\blobs  # 换存储目录
```

- 健康检查：<http://127.0.0.1:8771/health>（不鉴权，便于探活）
- 接口文档：<http://127.0.0.1:8771/docs>

## 配置（环境变量前缀 `BLOB_`）

| 项 | 默认 | 说明 |
|---|---|---|
| `BLOB_PORT` | 8771 | 刻意与 Relay 相邻，便于记忆与防火墙放行 |
| `BLOB_AUTH_TOKEN` | 空 | 留空则回落到 `RELAY_AUTH_TOKEN`；两者都空 = 不校验 |
| `BLOB_DATA_DIR` | `blobs/` | 存储目录 |
| `BLOB_MAX_FILE_BYTES` | 200MB | 跨端场景是传工程/素材包，不是网盘 |
| `BLOB_MAX_TOTAL_BYTES` | 20GB | 超限报 `quota_exceeded`（会先跑一次过期清理再判定） |
| `BLOB_TTL_DAYS` | 7 | 临时中转物，到期自动删。`0` = 永不过期 |
| `BLOB_CLEANUP_INTERVAL` | 600s | 后台清理间隔 |

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查（不鉴权） |
| POST/PUT | `/api/blob` | 上传，**请求体为原始字节流** |
| GET | `/api/blob/{id}` | 下载 |
| HEAD | `/api/blob/{id}` | 探测是否已存在（秒传用） |
| GET | `/api/blob/{id}/meta` | 元数据（含原始文件名） |
| DELETE | `/api/blob/{id}` | 删除 |
| GET | `/api/blobs` | 列表（调试） |
| GET | `/api/stats` | 存储统计 |
| POST | `/api/cleanup` | 立即执行一次过期清理 |

鉴权：`X-Auth-Token`、`Authorization: Bearer`，或 **`?token=`**。
支持 query 是刻意的：下载 URL 会被写进跨端消息、交给 curl 或浏览器，塞不进请求头。

### 上传请求头

| 头 | 必填 | 说明 |
|---|---|---|
| `X-Filename` | 建议 | 原始文件名。**非 ASCII 必须 percent-encode**（HTTP 头只能放 latin-1） |
| `X-Sha256` | 建议 | 服务端核对，不符则拒收（422），坏文件不会进库 |
| `X-Uploader` | 可选 | 上传者标识，排查用 |
| `X-Content-Type` | 可选 | 显式指定 MIME；不给时会忽略 curl 自作主张的 `x-www-form-urlencoded` |

### 为什么上传用原始字节流而不是 multipart

`UploadFile` 需要 `python-multipart`，本仓库 .venv 里没有
（relay 只依赖 fastapi/uvicorn/pydantic/pydantic-settings）。**本服务零新增依赖**。
此外原始流更省内存（`request.stream()` 逐块处理，不落整份到内存），
客户端也更简单（`curl --data-binary @file` 即可）。

## agent 侧怎么用

**agent 侧无需任何代码改动**，纯 curl 就能跑通（已实测，含中文名与 50MB 二进制）。
使用说明写在技能文档里：

    skill-root/Agents/create-crossnode-skill/SKILL.md → 「跨端传文件」一节

它与协作入口管理合在同一个技能里：跨端协作和传文件本就是一件事的两面，
拆成两个技能会出现"已经在聊协作了却没加载传文件那份"的情况。

## 用法

### curl（agent 侧主路径，无需任何代码改动）

**地址不要写死。** agent 环境里从环境变量取中继主机，端口 = 中继端口 + 1：

```bash
BLOB="http://${YAGENT_RELAY_HOST}:8771"
```

写 `127.0.0.1` 会造成两类故障：中继通常不在本机（连不上）；
即使本机能连，发给对端的 URL 在对端机器上指向的是它自己。

```bash
# 上传
SHA=$(sha256sum pack.zip | cut -d' ' -f1)
curl -s -X POST "$BLOB/api/blob" \
  -H "X-Filename: pack.zip" -H "X-Sha256: $SHA" \
  --data-binary @pack.zip

# 下载：先取原始文件名，再 -o
NAME=$(curl -s "$BLOB/api/blob/$SHA/meta" \
  | python -c "import sys,json;print(json.load(sys.stdin)['filename'])")
curl -s -o "$NAME" "$BLOB/api/blob/$SHA"
sha256sum "$NAME"   # 应等于 $SHA
```

**不要用 `curl -OJ` 下中文名文件**：curl 不支持 RFC5987 的 `filename*`，
只认 ASCII 回退名，中文会变成 `____.txt`。实测确认，必须用 `/meta` + `-o`。

`--data-binary @` 里的 `@` 不能漏，也不要用 `-d`：后者会把二进制当文本处理并吃掉换行。

### Python SDK（零第三方依赖，只用标准库）

`sdk/python/yagent_blob_client.py`

```python
from yagent_blob_client import BlobClient

client = BlobClient("http://10.1.42.2:8771", token="")
info = client.upload("D:/work/pack.zip", uploader="alice")
print(info.url, info.sha256, info.deduplicated)

# 对端
client.download(info.blob_id, "D:/dest/pack.zip")   # 默认校验 sha256
```

`upload()` 默认先算 sha256 并 HEAD 探测，命中则秒传（零流量）。
`download()` 先写 `.part` 再 rename，中断不会留下"看起来完整"的坏文件。

## 测试

```bash
# 单元测试（ASGI 内存传输，不需要起服务）
.venv/Scripts/python -m pytest tests/test_blobstore.py -q

# 端到端（需先起服务，走真实 socket）
run_blob.bat
.venv/Scripts/python scripts/blob_e2e_check.py --big-mb 32
```

实测结果（2026-08-26，本机）：

| 检查 | 结果 | 覆盖 |
|---|---|---|
| `pytest tests/test_blobstore.py` | 20/20 | 内容寻址、去重、校验和拒收、超限（含 chunked）、路径穿越、中文名、鉴权与 token 回落、TTL 清理、配额 |
| 全仓 `pytest` | 71/71 | 新增 20 项，原 relay 51 项无回归 |
| `scripts/blob_e2e_check.py` | 26/26 | 真实 HTTP：大文件、进度回调、秒传、完整 URL 下载、错误路径 |
| 吞吐 | 32MB 上传 166MB/s | 环回，仅供参考 |

## 已知限制

- **单节点**：存储在本地磁盘，没有多副本。中转服务所在机器挂了，
  未下载完的文件就没了 —— 但这是临时中转物，重传即可。
- **没有分片续传**：单次 HTTP 传完。200MB 上限内、局域网环境下够用；
  跨公网传大文件如果频繁中断，需要另做分片。
- **没有按 client 的独立配额**：只有全局上限。多人共用时某个人可能占满，
  靠 TTL 与人工 `/api/cleanup` 兜底。
- **`?token=` 会出现在 URL 里**，可能被日志记录。安全边界是"能拿到这条跨端消息的人
  才能下载"，与消息本身同级；不适合放需要更强隔离的内容。
