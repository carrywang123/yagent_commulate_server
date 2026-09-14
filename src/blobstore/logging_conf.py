"""日志配置。与 relay/logging_conf.py 保持相同行为，只是默认文件名不同。"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path
from typing import Optional

from blobstore.config import PROJECT_ROOT

_CONFIGURED = False

FORMAT = "%(asctime)s %(levelname)-7s [%(name)s] %(message)s"
DATEFMT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    level: str = "INFO",
    log_dir: str = "",
    *,
    to_file: bool = True,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    filename: str = "blobstore.log",
) -> Optional[Path]:
    """初始化日志，返回日志文件路径（未写文件则返回 None）。

    重复调用是安全的：只有首次会真正配置。
    uvicorn --reload 会在子进程再走一遍 create_app，没有这个保护会挂上重复 handler，
    每条日志打印两遍。
    """
    global _CONFIGURED
    if _CONFIGURED:
        return None

    root = logging.getLogger()
    root.setLevel(getattr(logging, str(level).upper(), logging.INFO))

    formatter = logging.Formatter(FORMAT, datefmt=DATEFMT)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    path: Optional[Path] = None
    if to_file:
        directory = Path(log_dir).expanduser() if str(log_dir).strip() else PROJECT_ROOT / "logs"
        try:
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / filename
            handler = logging.handlers.RotatingFileHandler(
                path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
            )
            handler.setFormatter(formatter)
            root.addHandler(handler)
        except OSError as exc:
            # 日志目录不可写不该让服务起不来，退化成只有控制台
            root.warning("无法写入日志文件（%s），仅输出到控制台", exc)
            path = None

    _CONFIGURED = True
    return path
