"""token 鉴权。与 relay/security.py 同构，便于两个服务行为一致。"""

from __future__ import annotations

import hmac
from typing import Optional

from fastapi import HTTPException, Request

from blobstore.config import Settings, get_settings


def token_matches(settings: Settings, token: Optional[str]) -> bool:
    """未开启鉴权时一律通过；开启后用常量时间比较，避免计时侧信道。"""
    expected = settings.auth_token_effective
    if not expected:
        return True
    return bool(token) and hmac.compare_digest(str(token), expected)


def extract_token(request: Request) -> Optional[str]:
    """从多个位置取 token。

    支持 query 参数是刻意的：下载 URL 会被交给浏览器、`curl -O`、
    或写进消息 payload 由对端直接取，这些场景都塞不进自定义请求头。
    """
    header = request.headers.get("x-auth-token")
    if header:
        return header.strip()
    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    query = request.query_params.get("token")
    if query:
        return query.strip()
    return None


async def require_token(request: Request) -> None:
    settings: Settings = getattr(request.app.state, "settings", None) or get_settings()
    if not token_matches(settings, extract_token(request)):
        raise HTTPException(
            status_code=401,
            detail={"code": "unauthorized", "message": "token 无效或缺失"},
        )
