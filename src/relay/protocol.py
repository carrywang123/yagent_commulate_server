"""消息协议定义。

所有经由本服务流转的消息都是同一个"信封"结构，用 type 字段区分用途。

客户端 -> 服务端：
    hello       注册身份（也可用 ws 查询参数 client_id 代替）
    ping        心跳
    pong        对服务端 ping 的应答
    send        定向发送给某个 client_id；expect_reply=true 时期待对方回复
    reply       回复某条 send，必须带 reply_to = 原消息 id
    broadcast   广播；带 group 则只发该分组
    subscribe   加入分组
    unsubscribe 退出分组
    list        查询在线客户端

服务端 -> 客户端：
    welcome     注册成功
    ping/pong   心跳
    forward     转发过来的对方消息（由 send/broadcast 产生）
    reply       转发回来的回复
    ack         服务端确认某条消息已投递
    error       出错
    presence    某个客户端上线/下线
    list_result 在线列表查询结果
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

PROTOCOL_VERSION = 1


def new_id() -> str:
    return uuid.uuid4().hex


class MsgType:
    # client -> server
    HELLO = "hello"
    PING = "ping"
    PONG = "pong"
    SEND = "send"
    REPLY = "reply"
    BROADCAST = "broadcast"
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    LIST = "list"

    # server -> client
    WELCOME = "welcome"
    FORWARD = "forward"
    ACK = "ack"
    ERROR = "error"
    PRESENCE = "presence"
    LIST_RESULT = "list_result"


class ErrCode:
    BAD_REQUEST = "BAD_REQUEST"
    UNAUTHORIZED = "UNAUTHORIZED"
    PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"
    PEER_OFFLINE = "PEER_OFFLINE"
    UNKNOWN_TYPE = "UNKNOWN_TYPE"
    UNKNOWN_REPLY = "UNKNOWN_REPLY"
    REPLY_FORBIDDEN = "REPLY_FORBIDDEN"
    TIMEOUT = "TIMEOUT"
    DUPLICATE_CLIENT = "DUPLICATE_CLIENT"
    SLOW_CONSUMER = "SLOW_CONSUMER"
    INTERNAL = "INTERNAL"


class ErrorInfo(BaseModel):
    code: str
    message: str = ""
    ref: Optional[str] = None  # 出错的原始消息 id


class Envelope(BaseModel):
    """统一消息信封。

    注意：字段名 from 是 Python 关键字，模型内用 from_，序列化/反序列化时
    通过别名 "from" 与外部交互。
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    v: int = PROTOCOL_VERSION
    id: str = Field(default_factory=new_id)
    type: str
    from_: Optional[str] = Field(default=None, alias="from")
    to: Optional[str] = None
    group: Optional[str] = None
    reply_to: Optional[str] = None
    expect_reply: bool = False
    need_ack: bool = False
    timeout: Optional[float] = None
    ts: float = Field(default_factory=time.time)
    payload: Any = None
    error: Optional[ErrorInfo] = None

    def dump(self) -> dict[str, Any]:
        """导出为对外的 dict（使用 from 别名，去掉空字段）。"""
        return self.model_dump(by_alias=True, exclude_none=True)


def make_error(
    code: str,
    message: str = "",
    ref: str | None = None,
    to: str | None = None,
) -> Envelope:
    return Envelope(
        type=MsgType.ERROR,
        to=to,
        error=ErrorInfo(code=code, message=message, ref=ref),
    )
