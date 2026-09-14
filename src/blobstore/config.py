"""服务配置。全部通过环境变量或 .env 文件注入，前缀 BLOB_。"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

#: 项目根（src/blobstore/config.py -> 上三层）
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="BLOB_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # 网络。端口刻意与 Relay(8770) 相邻，便于记忆和防火墙放行。
    host: str = "0.0.0.0"
    port: int = 8771

    #: 鉴权 token，为空则不校验。
    #: 留空时会回落到 RELAY_AUTH_TOKEN（见 auth_token_effective），
    #: 这样同机部署只需配一处，不会出现"消息通道开了鉴权、文件通道忘了开"的错配。
    auth_token: str = ""

    # 存储
    #: 空 = 项目根下 blobs/
    data_dir: str = ""

    #: 单文件上限。默认 200MB —— 跨端场景是"传个工程/素材包"，
    #: 不是网盘。上限太大时一次误操作就能打满磁盘，且上传期间占着 worker。
    max_file_bytes: int = 200 * 1024 * 1024
    #: 全局存储上限，超过后拒绝新上传（先跑一次过期清理再判定）
    max_total_bytes: int = 20 * 1024 * 1024 * 1024
    #: 流式读写块大小
    chunk_bytes: int = 1024 * 1024

    #: 保留天数。跨端文件是临时中转物，不是长期存储。
    #: 到期自动删除，避免无人管理的目录无限膨胀。
    ttl_days: float = 7.0
    #: 后台清理间隔（秒）
    cleanup_interval: float = 600.0

    # 日志（与 relay 保持同一套习惯，便于运维）
    log_level: str = "INFO"
    log_dir: str = ""
    log_to_file: bool = True
    log_max_bytes: int = 10 * 1024 * 1024
    log_backup_count: int = 5

    # CORS
    allow_origins: str = "*"

    @property
    def origins(self) -> list[str]:
        raw = (self.allow_origins or "").strip()
        if not raw or raw == "*":
            return ["*"]
        return [item.strip() for item in raw.split(",") if item.strip()]

    @property
    def auth_token_effective(self) -> str:
        """实际生效的 token：BLOB_AUTH_TOKEN 优先，否则借用 RELAY_AUTH_TOKEN。

        借用是有意的：两个服务同机部署、面向同一批客户端，
        分别配 token 只会制造"一个开一个关"的安全缺口。
        """
        if self.auth_token:
            return self.auth_token
        return (os.getenv("RELAY_AUTH_TOKEN") or "").strip()

    @property
    def auth_enabled(self) -> bool:
        return bool(self.auth_token_effective)

    @property
    def data_path(self) -> Path:
        raw = (self.data_dir or "").strip()
        base = Path(raw) if raw else PROJECT_ROOT / "blobs"
        return base.expanduser().resolve()

    @property
    def ttl_seconds(self) -> float:
        """<=0 表示永不过期。"""
        return float(self.ttl_days) * 86400.0


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
