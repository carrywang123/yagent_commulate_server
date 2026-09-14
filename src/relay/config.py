"""服务配置。全部通过环境变量或 .env 文件注入，前缀 RELAY_。"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RELAY_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # 网络
    host: str = "0.0.0.0"
    port: int = 8770

    # 鉴权：为空则不校验
    auth_token: str = ""

    # 日志
    log_level: str = "INFO"
    #: 日志目录，空 = 项目根下 logs/
    log_dir: str = ""
    #: 是否写文件日志（常驻服务建议开）
    log_to_file: bool = True
    #: 单个日志文件上限，超过则轮转
    log_max_bytes: int = 10 * 1024 * 1024
    #: 保留几个历史文件
    log_backup_count: int = 5

    # CORS
    allow_origins: str = "*"

    # 心跳与超时
    heartbeat_interval: float = 20.0
    client_timeout: float = 60.0
    request_timeout: float = 30.0

    # 限制
    max_message_bytes: int = 1024 * 1024
    outbox_size: int = 256

    # 上下线广播
    presence_broadcast: bool = True

    # 跨端任务链路追踪（见 trace.py / trace_db.py）
    #: 关掉后埋点直接 return，零开销
    trace_enabled: bool = True
    #: SQLite 文件路径，空 = <trace_dir>/trace.db
    trace_db: str = ""
    #: 追踪数据目录，空 = <日志目录>/trace
    trace_dir: str = ""
    #: 看板默认显示多少天（只是默认值，页面可筛选到任意区间）
    trace_default_days: int = 14
    #: 历史清理天数。0 = **永不删除**（默认）
    trace_prune_days: int = 0
    #: 内存里最多缓存多少条活跃任务（仅影响内存，不影响已落盘数据）
    trace_max_tasks: int = 5000
    #: 单个任务时间线最多保留多少条（probe 已折叠，一般远达不到）
    trace_max_timeline: int = 60
    #: payload 摘要长度。只留摘要，不留全文
    trace_summary_chars: int = 200
    #: probe 心跳是否逐条入 events 表（关掉可大幅减小数据库，
    #: 任务上的对账轮数与时间线区间仍保留）
    trace_store_probe_events: bool = True
    #: 追踪接口/看板是否需要 token（默认放行，内网直接打开）
    trace_require_token: bool = False

    @property
    def origins(self) -> list[str]:
        raw = (self.allow_origins or "").strip()
        if not raw or raw == "*":
            return ["*"]
        return [item.strip() for item in raw.split(",") if item.strip()]

    @property
    def auth_enabled(self) -> bool:
        return bool(self.auth_token)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
