# YAgent Commulate Server

一个**带身份注册的消息转发（中转）服务**。

每个客户端启动后先告诉服务端"我上线了，我叫 X"；之后要通信时告诉服务端"我要找 Y"，
服务端负责把消息投递给 Y，需要的话再把 Y 的回复带回来。

典型场景（本项目的实际目标）：

```
Python / shell 脚本 ──发起任务──► 中转服务 ──转发──► Vue3 页面
                    ◄──回传结果──          ◄──回复──
```

- **发起方**：Python 常驻进程（WebSocket）或 shell 脚本（一条 curl 即可，无需长连接）
- **接收方**：Vue3 页面（浏览器 WebSocket）或 Python 工作进程
- **服务端**：只做身份注册 + 路由转发，不理解业务内容

---

## 1. 技术选型

| 项 | 选择 | 原因 |
|---|---|---|
| 语言 | Python 3.11+ | 与 YAgent 生态一致，客户端底层也是 Python |
| Web 框架 | FastAPI + uvicorn | 同时提供 WebSocket 与 REST，自带 OpenAPI 文档 |
| 传输 | WebSocket（长连接）+ HTTP REST（一次性） | 网页/常驻进程用 WS；shell 用 curl 更省事 |
| 并发 | asyncio 单进程 | 转发是纯 IO 密集，几百到数千长连接毫无压力 |
| 状态 | 内存注册表 | 单实例够用；路由层已抽象，将来可换 Redis 做多节点 |

已实现的能力：定向转发、请求-响应、广播/分组、多端同身份、心跳探活、
token 鉴权、慢消费保护、上下线通知、在线查询、运行指标、Web 调试台、
**跨端任务链路追踪看板**（见 §7）。

---

## 2. 目录结构

```
├── src/relay/                  服务端
│   ├── main.py                 FastAPI 组装 + 后台维护任务（心跳/超时清理/追踪落盘）
│   ├── config.py               配置（环境变量 RELAY_*）
│   ├── protocol.py             消息信封与类型定义
│   ├── registry.py             在线连接注册表（含发送队列、慢消费保护）
│   ├── router.py               路由核心：转发 / 广播 / 请求-响应关联
│   ├── ws.py                   WebSocket 端点与消息分发
│   ├── api.py                  REST 接口
│   ├── trace.py                跨端任务链路追踪：事件抽取 + 任务聚合
│   ├── trace_db.py             追踪的 SQLite 持久层（events / tasks 两张表）
│   ├── trace_api.py            追踪查询接口 + SSE 实时推送
│   ├── security.py             token 鉴权
│   └── static/
│       ├── console.html        Web 调试台（零依赖，浏览器直接打开）
│       ├── trace.html          跨端任务追踪看板（零依赖单文件）
│       └── relay-client.js     浏览器 / Vue3 SDK（服务端会直接对外提供）
├── sdk/python/yagent_relay_client.py   Python SDK（异步长连接 + 同步 REST）
├── examples/
│   ├── py_worker.py            接收任务并回复
│   ├── py_caller.py            发起任务并等结果
│   ├── shell_send.sh           纯 curl 用法
│   └── vue3/useRelay.js        Vue3 组合式函数
├── scripts/
│   ├── start_background.py     后台启动服务 / 示例 worker
│   ├── e2e_check.py            对运行中的服务做端到端联调检查
│   ├── trace_demo.py           往运行中的服务灌入仿真跨端任务，用于看 /trace 效果
│   ├── sdk_js_check.py         用 Playwright 在真实浏览器里验证 JS SDK
│   └── ui_check.py             用 Playwright 检查 Web 调试台
├── tests/                      pytest 自动化测试（31 项）
├── docs/PROTOCOL.md            协议详细说明
├── docs/BLOBSTORE.md           跨端文件中转（Blob Store）说明
├── src/blobstore/              跨端文件中转服务（独立进程，8771）
│   ├── main.py                 FastAPI 组装 + 后台过期清理
│   ├── config.py               配置（环境变量 BLOB_*）
│   ├── storage.py              内容寻址存储（blob_id = sha256）
│   └── security.py             token 鉴权（留空则回落 RELAY_AUTH_TOKEN）
├── sdk/python/yagent_blob_client.py    文件中转客户端（零第三方依赖）
├── run.sh / run.bat            启动脚本
├── run_blob.sh / run_blob.bat  启动文件中转服务（8771）
└── .env.example                配置模板
```

→ **跨端传文件看 [docs/BLOBSTORE.md](docs/BLOBSTORE.md)**。消息通道有 1MB 上限，
且大流量会拖垮心跳导致 Relay 主动断连，**文件必须走 8771 那条独立链路**。

---

## 3. 快速开始

### 3.1 安装

```bash
uv venv .venv
uv pip install -e ".[dev]"
```

（没有 uv 就用 `python -m venv .venv` + `pip install -e ".[dev]"`）

### 3.2 启动服务

```bash
run.bat              # Windows
bash run.sh          # Git Bash / Linux / macOS
run.bat --reload     # 开发模式，改代码自动重启
```

默认监听 `0.0.0.0:8770`。启动后：

- Web 调试台：<http://127.0.0.1:8770/>
- **跨端任务追踪看板：<http://127.0.0.1:8770/trace>**
- 接口文档：<http://127.0.0.1:8770/docs>
- 健康检查：<http://127.0.0.1:8770/health>

配置方式：复制 `.env.example` 为 `.env` 后修改，或直接设环境变量（前缀 `RELAY_`）。

### 3.3 三分钟跑通一条链路

```bash
# 终端 1：启动服务
run.bat

# 终端 2：启动一个 Python 工作端（会注册为 py-worker）
.venv/Scripts/python examples/py_worker.py

# 终端 3：从 shell 发一个任务给它并等结果
curl -X POST http://127.0.0.1:8770/api/send \
  -H "Content-Type: application/json" \
  -d '{"from":"shell","to":"py-worker","payload":{"cmd":"add","args":[1,2,3]},"expect_reply":true}'
# => {"ok":true,...,"reply":{"from":"py-worker","payload":{"ok":true,"result":6}}}
```

浏览器打开 <http://127.0.0.1:8770/> 输入 `client_id`（如 `ui-1`）点连接，
即可与 `py-worker` 互发消息、观察全部报文——联调时非常好用。

---

## 4. 客户端怎么接

### 4.1 Vue3 前端（接收任务的一方）

SDK 文件两种引入方式：

- 拷走 `src/relay/static/relay-client.js` 放进前端工程（推荐）；
- 或直接从服务端拉：`http://<server>:8770/sdk/relay-client.js`（服务已内置该路由）。

```js
import { RelayClient } from '@/lib/relay-client.js'

const client = new RelayClient({
  url: 'ws://10.1.42.2:8770/ws',
  clientId: 'ui-1',          // 这个页面的身份，别人靠它找到你
  groups: ['ui'],            // 可选，便于按组广播
  token: import.meta.env.VITE_RELAY_TOKEN,  // 服务端开了鉴权才需要
})

// 别人 call 过来时触发，返回值会自动回给对方（支持 async）
client.onRequest(async (env) => {
  if (env.payload.cmd === 'render') {
    await doRender(env.payload)
    return { ok: true, cost: 12 }
  }
  return { ok: false, reason: 'unknown cmd' }
})

client.onMessage((env) => console.log('单向消息', env.payload))   // 不需要回复的
client.onBroadcast((env) => console.log('广播', env.payload))
client.onPresence((env) => console.log('谁上下线了', env.payload))

await client.connect()

// 页面也可以主动找别人干活
const r = await client.call('py-worker', { cmd: 'add', args: [1, 2] }, 15000)
```

Vue 组件里更省事的写法见 `examples/vue3/useRelay.js`。
断线会自动重连（指数退避），无需自己处理。

### 4.2 Python 常驻进程

```python
from yagent_relay_client import RelayClient

client = RelayClient("ws://127.0.0.1:8770/ws", client_id="py-worker", groups=["worker"])

@client.on_request                 # 别人 call 我，返回值自动回传
async def handle(env):
    return {"ok": True, "echo": env["payload"]}

await client.connect()
result = await client.call("ui-1", {"cmd": "render"}, timeout=15)   # 我也可以找别人
```

### 4.3 shell / 一次性脚本

不需要长连接，直接 REST：

```bash
# 单向发送
curl -X POST http://127.0.0.1:8770/api/send \
  -H "Content-Type: application/json" \
  -d '{"from":"shell","to":"ui-1","payload":{"msg":"hi"}}'

# 发送并等对方回复（服务端替你挂着等）
curl -X POST http://127.0.0.1:8770/api/send \
  -H "Content-Type: application/json" \
  -d '{"from":"shell","to":"ui-1","payload":{"cmd":"render"},"expect_reply":true,"timeout":20}'
```

Python 一次性脚本可用零依赖的同步客户端：

```python
from yagent_relay_client import RelayRestClient
rest = RelayRestClient("http://127.0.0.1:8770", from_id="my-script")
print(rest.call("ui-1", {"cmd": "render"}, timeout=20))
```

完整 shell 示例见 `examples/shell_send.sh`。

---

## 5. 核心语义（重要）

| 行为 | 语义 |
|---|---|
| **单向消息** (`send`) | 投递给目标 `client_id` 的**全部连接**。通知类消息，多开的页面都该看到 |
| **请求-响应** (`expect_reply`) | 只投递给**一条连接**（最近活跃的那条）。任务只能被执行一次，不会重复 |
| **目标离线** | 立即返回 `PEER_OFFLINE` 错误，**不暂存**消息（按当前需求确定的策略） |
| **回复权限** | 只有当初的目标方能回复该请求，第三方回复会被拒绝（`REPLY_FORBIDDEN`） |
| **发送方身份** | 服务端以连接注册的身份为准，客户端伪造 `from` 无效 |
| **超时** | 请求超过 `timeout` 未被回复，服务端主动给发起方回 `TIMEOUT` |
| **慢消费** | 某连接积压超过 `RELAY_OUTBOX_SIZE` 条未发出，主动断开该连接，保护服务端内存 |
| **心跳** | 服务端定期 ping，客户端需回 pong；`RELAY_CLIENT_TIMEOUT` 内无任何消息即判定掉线 |

消息格式、全部消息类型与错误码见 [docs/PROTOCOL.md](docs/PROTOCOL.md)。

---

## 6. REST 接口一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查（不需要鉴权，便于探活） |
| GET | `/api/metrics` | 运行指标（在线数、转发量、错误数等） |
| GET | `/api/clients` | 在线客户端列表 |
| GET | `/api/clients/{id}` | 某个客户端是否在线 |
| GET | `/api/groups` | 分组列表 |
| POST | `/api/send` | 定向发送，可 `expect_reply` 等回复 |
| POST | `/api/broadcast` | 广播 / 按分组发送 |
| POST | `/api/kick/{id}` | 强制断开某个客户端 |
| GET | `/api/trace/overview` | 跨端任务总览：KPI + 用户维度统计 |
| GET | `/api/trace/tasks` | 任务列表（按用户/角色/状态/关键词/时间段过滤，可分页）|
| GET | `/api/trace/tasks/{id}` | 单任务完整时间线 |
| GET | `/api/trace/tasks/{id}/events` | 该任务的原始事件明细（含每一轮 probe）|
| GET | `/api/trace/stream` | SSE 实时推送任务变更 |
| GET | `/api/trace/stats` | 追踪自身运行状况（库大小、行数、数据跨度）|
| POST | `/api/trace/maintain` | 立即合并 WAL（备份前用）|
| GET | `/trace` | 跨端任务追踪看板 |
| GET | `/sdk/relay-client.js` | 直接下发浏览器 SDK，前端可直引 |
| GET | `/` | Web 调试台 |
| WS | `/ws?client_id=X&groups=a,b&token=T` | WebSocket 接入 |

鉴权：设置 `RELAY_AUTH_TOKEN` 后，REST 需带 `X-Auth-Token`（或 `Authorization: Bearer`），
WS 需带 `?token=`。留空则不校验，方便本地开发。

---

## 7. 跨端任务链路追踪

看板：**<http://127.0.0.1:8770/trace>**

### 7.1 为什么这个功能能做在 Relay 上

YAgent 的跨端协作（Hub）**刻意不用** Relay 的 `expect_reply`/`reply`，
request / response / notify / probe / probe_ack / cancel 六种信封全部走 `send`，
都必须经过 Relay 这一跳（原因见 `Yagent_Desk/src/main/hub/relay-protocol.ts`）。
于是 Relay 成了整条跨端链路上**唯一的全局汇聚点**：
客户端一行不改，就能还原“谁把什么任务派给了谁、对方处理到哪一步、花了多久”。

### 7.2 看板能回答什么

- **按人定位**：左栏列出所有用户（`client_id` 就是用户名），带在线状态、
  发起/处理/失败计数；点人后可切「他发出的」/「他处理的」
- **时间范围**：今天 / 7 / 14（默认）/ 30 / 90 天 / 全部历史 / 自定义起止日期，
  超过一页自动出分页
- **任务列表**：状态、发起→处理双方、任务摘要、耗时（进行中本地走秒）、发起时间
- **任务时间线**：发起 → 投递结果 → 对端状态拐点（排队/处理中/推迟/待重投）
  → 结果，并给出总耗时 / 等待 / 处理 三个口径
- **失败归因**：`PEER_OFFLINE`、`rejected`、`no_reply`（对端不认识该请求）、发起方撤回
- **静默告警**：超过 200s（≈ 3 轮 probe）没任何帧且未终结的任务会被标出

任务粒度是**一条 request**，task_id 就是它的 `msg_id`；
response / error / cancel / probe / probe_ack 靠 `reply_to` 归因，probe 的
`PB:<task_id>:<seq>` 前缀让探针的投递失败也能归到具体任务。

### 7.3 精度与边界（很重要，别对着看板得出错误结论）

| 指标 | 精度 |
|---|---|
| 发起时间、投递结果、完成时间、总耗时 | 精确 |
| 开始处理时间、等待/处理时长 | **±60s**（只能由 60s 一轮的 probe_ack 推断）|
| 对端 agent 内部执行到第几步、调了什么工具 | **拿不到**，这些数据不出本机 |

想要秒级的阶段进度，必须让 Desk / Server 侧主动上报（新增进度帧），
Relay 无法凭空得知。另外跨端大文件走的是 8771 那条独立链路，**不在**本看板里。

### 7.4 存储与隐私

**数据落在 SQLite（默认 `logs/trace/trace.db`），默认永久保留，不会自动删。**
看板默认只显示最近 14 天，但顶部可选 30/90 天、全部历史或自定义起止日期。

两张表分工：

| 表 | 内容 | 定位 |
|---|---|---|
| `events` | append-only 原始事件，包含每一轮 probe 心跳 | **真相**，以后口径要改能重算 |
| `tasks` | 一行一任务的聚合快照（含折叠后的时间线）| 派生数据，看板查它，可从 events 重建 |

其余要点：

- **写入不做缓冲**：每条事件一个事务（INSERT events + UPSERT tasks）同步提交，
  进程被 `kill -9` 也不会丢已转发的记录（有对应验证）
- **重启无缝接上**：启动时把未结束的任务全部载回内存，
  几十分钟后才回来的 response 仍能接在原任务上，不会变成孤儿记录
- 内存只是活跃任务的缓存（`RELAY_TRACE_MAX_TASKS`），被淘汰的老任务收到回复时
  会先从库里回填，不会覆盖历史
- 查询全走带索引的 SQL + 分页，“查三个月前”与“查今天”一样快
- payload **只留摘要**（默认 200 字符）与字节数，不留全文
- 非 crossnode 的业务消息（同一个 Relay 上其他人的 ui / py_worker）**完全不记**
- 默认不鉴权（内网随手打开）；公网暴露前务必 `RELAY_TRACE_REQUIRE_TOKEN=true`
- 追踪全程 try/except 包裹，**坏了也不会影响转发**（有专门的回归测试）；
  `RELAY_TRACE_ENABLED=false` 可彻底关掉，埋点直接 return

**磁盘估算**：一条 10 分钟的任务大约 24 行事件（含 20 条 probe 心跳）。
每天 100 条跨端任务 ≈ 0.6MB/天 ≈ 220MB/年。嫌大可以
`RELAY_TRACE_STORE_PROBE_EVENTS=false`，心跳就不逐条入库（对账轮数仍保留），
体积能降一个数量级。真要清理历史，显式设 `RELAY_TRACE_PRUNE_DAYS=365` 之类的值。

**备份**：WAL 模式下直接拷 `trace.db` 可能拿不到最新数据（那些还在 `-wal` 里）。
要么先 `curl -X POST http://<relay>:8770/api/trace/maintain` 合并一下再拷单文件，
要么三个文件（`trace.db` / `-wal` / `-shm`）一起拷。

### 7.5 没有真实跨端流量时怎么看效果

```bash
# 另起一个不影响生产的实例
RELAY_PORT=8779 RELAY_TRACE_DIR=temp/trace-demo \
  .venv/Scripts/python -m uvicorn relay.main:app --port 8779 --app-dir src

# 灌入仿真任务（按 Hub 的真实帧序列发，与真链路同构）
.venv/Scripts/python scripts/trace_demo.py --url ws://127.0.0.1:8779/ws
# 然后打开 http://127.0.0.1:8779/trace
```

→ 往生产 Relay(8770) 灌演示数据会污染真实看板，该脚本默认连 8779。

---

## 8. 测试与验证

```bash
# 自动化测试（不需要启动服务，105 项）
.venv/Scripts/python -m pytest -q

# 以下三项需先启动服务（run.bat 或 python scripts/start_background.py）
.venv/Scripts/python scripts/e2e_check.py                          # 端到端联调 18 项
"$YAGENT_PYTHON_RUNNER" run-python-script scripts/sdk_js_check.py  # 浏览器 JS SDK 12 项
"$YAGENT_PYTHON_RUNNER" run-python-script scripts/ui_check.py      # Web 调试台 8 项
```

当前实测结果：

| 检查 | 结果 | 覆盖内容 |
|---|---|---|
| pytest | 105/105 通过 | 协议、转发、请求-响应、广播、鉴权、多端投递语义、异常分支、**跨端追踪（34 项，含持久化/范围筛选/分页/重建）** |
| 端到端（真实服务） | 18/18 通过 | Python SDK + REST：转发、call、超时、离线、重连、多端 |
| 浏览器 JS SDK | 12/12 通过 | 真实 Chromium：连接、call、异常传递、广播、订阅、自动重连 |
| Web 调试台 | 8/8 通过 | 页面 ↔ Python worker ↔ shell(REST) 三方互通 |

---

## 9. 部署与扩展

**部署**：单进程即可，几百客户端场景无需负载均衡。

```bash
uvicorn relay.main:app --host 0.0.0.0 --port 8770        # PYTHONPATH=src
```

放到 Nginx 后面时记得为 `/ws` 开启 WebSocket 升级：

```nginx
location /ws {
    proxy_pass http://127.0.0.1:8770;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 3600s;
}
```

**将来要多节点**（超出单机承载或需要高可用）：注册表与路由已经收敛在
`registry.py` / `router.py` 两个类里，届时把它们换成 Redis 版实现
（`Pub/Sub` 转发 + `Hash` 存在线表），`ws.py` 与 `api.py` 无需改动。

**若将来需要离线消息**：当前策略是目标离线直接报错。要改成暂存，
只需在 `Router.deliver` 捕获 `PEER_OFFLINE` 时写入待发队列，并在
`ws.py` 客户端注册成功后补发即可，协议层无需变更。
