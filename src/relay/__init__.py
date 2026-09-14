"""YAgent Commulate Server —— 客户端消息转发 / 中转服务。"""

from .protocol import Envelope, ErrCode, MsgType, PROTOCOL_VERSION

__version__ = "0.1.0"

__all__ = ["Envelope", "ErrCode", "MsgType", "PROTOCOL_VERSION", "__version__"]
