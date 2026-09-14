"""YAgent Blob Store —— 跨端文件中转服务。

## 它解决什么问题

跨端通信（Relay）只能传 JSON 消息，且 `RELAY_MAX_MESSAGE_BYTES` 默认 1MB。
把文件塞进 payload 有两个死结：base64 膨胀 1.33 倍，1MB 的文件都过不去；
更要命的是大流量会拖慢 Relay 的心跳往返，一旦超过 `RELAY_CLIENT_TIMEOUT`（60s），
Relay 会主动断开连接，Hub 侧收到 presence offline 后会给正在等待的跨端任务
注入 `peer_disconnected` —— **传文件把消息通道搞挂**。

所以文件走这条独立链路：
    发送方 upload → 拿到 sha256 → 把 URL 写进消息 payload → 接收方 download

## 为什么与 Relay 同仓库但独立进程

同仓库：两者共用一套部署/运维习惯，配置项能相互引用（BLOB_AUTH_TOKEN 缺省
回落到 RELAY_AUTH_TOKEN），避免两端各配一遍。

独立进程：Relay 是单进程 asyncio + 纯内存注册表，挂了重启就好；
文件服务有磁盘状态，且大流量会与心跳争抢事件循环（见上）。
两者生命周期与故障域必须分开 —— 磁盘满绝不能连带转发一起挂。

## 内容寻址

blob_id 就是文件内容的 sha256。三个好处，都是实测中会碰到的问题：
  1. 天然去重：同一份文件重复上传不占额外空间；
  2. 天然秒传：上传前 HEAD 一下就知道服务端有没有，有就不用传；
  3. 天然校验：接收方下载完自己算一遍 sha256 就能确认没截断。
     没有这一条的话，半个文件会被当成成功交付 —— 这是最难查的一类故障。

原始文件名只作为元数据保存，下载时用 Content-Disposition 还原。
**不用原始文件名做存储路径**：既消掉路径穿越，也避免可执行文件按原名落地。
"""

from blobstore.config import Settings, get_settings

__all__ = ["Settings", "get_settings"]
