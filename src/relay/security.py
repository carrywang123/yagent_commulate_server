"""鉴权工具。RELAY_AUTH_TOKEN 为空时全部放行。"""

from __future__ import annotations

import hmac
from typing import Optional

from fastapi import Header, HTTPException, Request, status

from .config import Settings, get_settings


def token_matches(settings: Settings, token: Optional[str]) -> bool:
    if not settings.auth_enabled:
        return True
    if not token:
        return False
    return hmac.compare_digest(token, settings.auth_token)


def extract_bearer(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    if value.lower().startswith("bearer "):
        return value[7:].strip()
    return value


async def require_token(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
) -> None:
    """FastAPI 依赖：校验 REST 请求的 token。"""
    settings: Settings = getattr(request.app.state, "settings", None) or get_settings()
    if not settings.auth_enabled:
        return
    token = x_auth_token or extract_bearer(authorization)
    if not token_matches(settings, token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "UNAUTHORIZED", "message": "token 无效或缺失"},
        )
