# 通信协议

传输层：WebSocket，端点 `/ws`，消息体为 UTF-8 JSON 文本（每帧一条消息）。
另有等价的 REST 接口，见 README 第 6 节。

## 1. 消息信封

所有消息共用一个结构，靠 `type` 区分用途：

```json
{
  "v": 1,
  "id": "b6f1c8...",
  "type": "send",
  "from": "py-caller",
  "to": "ui-1",
  "group": null,
  "reply_to": null,
  "expect_reply": true,
  "need_ack": false,
  "timeout": 30,
  "ts": 1786539884.41,
  "payload": { "任意业务数据": "..." },
  "error": null
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `v` | int | 协议版本，当前 1 |
| `id` | string | 消息唯一 ID。客户端不传则服务端生成；请求-响应靠它关联 |
| `type` | string | 消息类型，见下表 |
| `from` | string | 发送方 `client_id`。**服务端会以连接身份强制覆盖**，无法伪造 |
| `to` | string | 目标 `client_id`（定向消息必填） |
| `group` | string | 分组名（广播/订阅时使用） |
| `reply_to` | string | 回复哪条消息的 `id` |
| `expect_reply` | bool | true 表示期待对方回复，服务端会登记并在超时后通知发起方 |
| `need_ack` | bool | true 表示希望服务端回一个投递确认 `ack` |
| `timeout` | number | 等待回复的秒数，缺省用服务端 `RELAY_REQUEST_TIMEOUT` |
| `ts` | number | Unix 时间戳（秒，浮点） |
| `payload` | any | 业务数据，服务端不做任何解释 |
| `error` | object | `{code, message, ref}`，仅错误/失败回复时出现 |

## 2. 客户端 → 服务端

| type | 必填字段 | 说明 |
|---|---|---|
| `hello` | `from` 或 `payload.client_id` | 注册身份。也可用 `/ws?client_id=X` 代替；重复发送可更新分组和元信息 |
| `ping` | — | 心跳，服务端回 `pong` |
| `pong` | — | 对服务端 `ping` 的应答 |
| `send` | `to` | 定向发送。`expect_reply=true` 时进入请求-响应模式 |
| `reply` | `reply_to` | 回复某条请求；`payload` 为结果，或用 `error` 表示失败 |
| `broadcast` | — | 广播；带 `group` 则只发该组。不会回给自己 |
| `subscribe` | `payload.groups` | 加入分组 |
| `unsubscribe` | `payload.groups` | 退出分组 |
| `list` | — | 查询在线客户端，服务端回 `list_result` |

### 连接方式

两种等价写法：

```
ws://host:8770/ws?client_id=ui-1&groups=ui,room1&token=xxx
```

或先连 `ws://host:8770/ws`，再在 10 秒内发出：

```json
{"type":"hello","from":"ui-1","payload":{"groups":["ui"],"meta":{"ver":"1.0"},"token":"xxx"}}
```

超时未 `hello` 会被关闭（close code 4400）。

### 独占接入 `exclusive`

默认一个 `client_id` 允许多条连接（见第 6 节）。若某类客户端要求
「一个标识只能对应一个实例」，可在接入时声明独占：

```
ws://host:8770/ws?client_id=liupeiyu&exclusive=1
```

也可写在 hello 里：`{"type":"hello","from":"liupeiyu","payload":{"exclusive":true}}`。

规则：

- 声明独占时，若该 `client_id` **已有任何连接**，新连接被拒（`DUPLICATE_CLIENT`，close code 4409）；
- 已存在独占连接时，后续**普通连接**同样被拒；
- **先到先得**：拒绝新来者而不是踢掉旧连接，因为旧连接可能正在处理任务，
  踢掉会造成静默的任务丢失；拒绝新来者能立即给出明确错误；
- 独占连接断开后标识立即释放，可重新占用；
- **不传该参数的客户端行为完全不变**，是向后兼容的扩展。

收到 `DUPLICATE_CLIENT` 说明标识被占用，**重连也没用**，应提示用户换标识或断开另一端。

适用场景：跨端 agent 通信中「一个账号 = 一台机器」，若同名多开，
定向消息会投给全部连接，导致同一个任务被多台机器各执行一次。

## 3. 服务端 → 客户端

| type | 说明 |
|---|---|
| `welcome` | 注册成功。`payload` 含 `client_id`、`session_id`、`groups`、`heartbeat_interval`、`request_timeout`、当前 `online_clients` |
| `ping` / `pong` | 心跳 |
| `forward` | 别人发给你的定向消息。若 `expect_reply=true`，你**必须**用 `reply` + `reply_to=该消息 id` 回复 |
| `reply` | 你发出的请求收到了回复 |
| `ack` | 投递确认（你设置了 `need_ack`），`payload.delivered` 为实际投递的连接数 |
| `broadcast` | 收到广播 |
| `presence` | 有客户端上线/下线，`payload` 含 `client_id`、`online`、`groups`、`connections` |
| `list_result` | 在线列表查询结果 |
| `error` | 出错，见错误码表 |

## 4. 错误码

| code | 含义 | HTTP 对应 |
|---|---|---|
| `BAD_REQUEST` | 消息格式错误、缺少必填字段 | 400 |
| `UNAUTHORIZED` | token 无效或缺失（WS close code 4401） | 401 |
| `PAYLOAD_TOO_LARGE` | 超过 `RELAY_MAX_MESSAGE_BYTES` | 413 |
| `PEER_OFFLINE` | 目标客户端不在线（消息**不会**被暂存） | 404 |
| `DUPLICATE_CLIENT` | 标识被独占连接占用（WS close code 4409），重连无效 | 409 |
| `UNKNOWN_TYPE` | 不支持的消息类型 | — |
| `UNKNOWN_REPLY` | 回复的请求不存在或已超时 | 409 |
| `REPLY_FORBIDDEN` | 你不是该请求的目标方，无权回复 | 403 |
| `TIMEOUT` | 等待回复超时 | 504 |
| `SLOW_CONSUMER` | 消费太慢被断开（WS close code 4408） | — |
| `INTERNAL` | 服务端内部异常 | 500 |

客户端 SDK 额外产生的错误码：`NO_HANDLER`（未注册请求处理器）、
`HANDLER_ERROR`（业务处理函数抛异常）、`NOT_CONNECTED`、`DISCONNECTED`、`CLOSED`。

## 5. 完整交互示例

### 5.1 请求-响应

```
caller                     server                      worker
  │  send(id=r1,to=worker,expect_reply=true)
  ├──────────────────────────►│
  │                           │  forward(id=r1,from=caller,expect_reply=true)
  │                           ├──────────────────────────►│
  │                           │                            (处理任务)
  │                           │  reply(reply_to=r1)        │
  │                           │◄───────────────────────────┤
  │  reply(reply_to=r1,from=worker,payload=结果)          │
  │◄──────────────────────────┤
```

worker 处理失败时：

```json
{"type":"reply","reply_to":"r1","error":{"code":"HANDLER_ERROR","message":"具体原因"}}
```

发起方会收到带 `error` 的 `reply`（SDK 会抛成异常）。

### 5.2 目标不在线

```json
// 发出
{"type":"send","id":"m9","to":"ghost","payload":{}}
// 收到
{"type":"error","reply_to":"m9",
 "error":{"code":"PEER_OFFLINE","message":"目标客户端 'ghost' 不在线","ref":"m9"}}
```

### 5.3 分组广播

```json
// ui-1 加入 room1
{"type":"subscribe","payload":{"groups":["room1"]}}
// 任何人向 room1 广播
{"type":"broadcast","group":"room1","payload":{"event":"refresh"}}
```

## 6. 投递语义要点

1. 同一个 `client_id` 允许多条连接（多开页面 / 多进程）：
   - 单向消息 → 投给**所有**连接；
   - `expect_reply` 请求 → 只投给**一条**连接（最近活跃的），保证任务不被重复执行。
   - 接入时带 `exclusive=1` 可要求独占，此时同名连接会被拒绝（见第 2 节）。
2. 消息不持久化：服务端重启或目标离线时消息即丢弃（当前设计的明确取舍）。
3. 顺序：同一条连接上的消息按发出顺序投递；不同连接之间不保证全局顺序。
4. 心跳：客户端应至少每 `heartbeat_interval` 秒发一次 `ping`（或回 `pong`），
   否则会在 `client_timeout` 后被判定掉线。SDK 已内置。
