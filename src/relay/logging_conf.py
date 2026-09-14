"""日志配置。

控制台 + 文件双输出：
  - 控制台保留原样，方便直接看；
  - 文件带大小轮转，长期挂着不会把磁盘写满。

文件日志默认开启（RELAY_LOG_DIR 为空则落到项目下 logs/），
因为这个服务通常是双击 run.bat 常驻运行的，窗口一关日志就没了，
出问题时没有任何可追溯的记录。
"""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


def resolve_log_dir(log_dir: str = "") -> Path:
    """确定日志目录。空值时用项目根下的 logs/。

    项目根 = 本文件向上三层（src/relay/logging_conf.py -> 项目根）。
    """
    if log_dir.strip():
        return Path(log_dir.strip()).expanduser()
    return Path(__file__).resolve().parents[2] / "logs"


def setup_logging(
    level: str = "INFO",
    log_dir: str = "",
    *,
    to_file: bool = True,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> Path | None:
    """初始化日志。返回实际写入的日志文件路径（未写文件时返回 None）。"""
    root = logging.getLogger()
    resolved_level = level.upper()

    if root.handlers:
        root.setLevel(resolved_level)
        return None

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
    root.addHandler(handler)
    root.setLevel(resolved_level)
    # uvicorn 的 access 日志太吵，降一级
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    if not to_file:
        return None

    # 文件日志失败不能影响服务启动：磁盘满、目录不可写都只降级成控制台输出。
    try:
        directory = resolve_log_dir(log_dir)
        directory.mkdir(parents=True, exist_ok=True)
        log_path = directory / "relay.log"
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
        root.addHandler(file_handler)
        return log_path
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).warning("文件日志初始化失败，仅输出到控制台: %s", exc)
        return None
