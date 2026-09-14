"""命令行入口：python -m blobstore

刻意与 relay/__main__.py 保持同样的启动体验（端口预检、启动横幅、
Windows ANSI 处理），因为运维会把这两个服务当成一对来看。
"""

from __future__ import annotations

import argparse
import logging
import socket
import subprocess
import sys

import uvicorn

from blobstore.config import get_settings
from blobstore.logging_conf import setup_logging


def _enable_ansi() -> bool:
    """让 Windows 控制台解析 ANSI 转义码，否则 uvicorn 彩色日志会打成方块乱码。"""
    if sys.platform != "win32":
        return True
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        enable_vt = 0x0004  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
        ok = False
        for std_handle in (-11, -12):
            handle = kernel32.GetStdHandle(std_handle)
            mode = ctypes.c_uint32()
            if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
                continue
            if mode.value & enable_vt:
                ok = True
                continue
            if kernel32.SetConsoleMode(handle, mode.value | enable_vt):
                ok = True
        return ok
    except Exception:
        return False


def _port_holder(port: int) -> str:
    if sys.platform != "win32":
        return ""
    try:
        out = subprocess.run(
            ["netstat", "-ano", "-p", "TCP"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=8,
        ).stdout
    except Exception:
        return ""
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 5 and parts[3].upper() == "LISTENING" and parts[1].endswith(f":{port}"):
            pid = parts[4]
            name = ""
            try:
                info = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="ignore",
                    timeout=8,
                ).stdout.strip()
                if info.startswith('"'):
                    name = info.split('","')[0].strip('"')
            except Exception:
                pass
            return f"PID {pid}" + (f" ({name})" if name else "")
    return ""


def _port_in_use(host: str, port: int) -> bool:
    """启动前预测端口：不预测的话 uvicorn 只会抛一堆 traceback，看不出是端口被抢。

    注意不能设 SO_REUSEADDR，Windows 上会把已占用端口误判为可用。
    """
    bind_host = host if host not in ("", "*") else "0.0.0.0"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((bind_host, port))
        except OSError:
            return True
    return False


def main() -> None:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="YAgent Blob Store（跨端文件中转）")
    parser.add_argument("--host", default=settings.host)
    parser.add_argument("--port", type=int, default=settings.port)
    parser.add_argument("--reload", action="store_true", help="开发模式，代码变更自动重启")
    parser.add_argument("--log-level", default=settings.log_level.lower())
    parser.add_argument("--log-dir", default=settings.log_dir, help="缺省为项目根下 logs/")
    parser.add_argument("--no-log-file", action="store_true", help="只输出到控制台")
    parser.add_argument("--data-dir", default=settings.data_dir, help="缺省为项目根下 blobs/")
    args = parser.parse_args()

    use_colors = _enable_ansi()

    # 把命令行覆盖写回 settings，否则 uvicorn 用 args、日志打 settings，两边不一致
    settings.host = args.host
    settings.port = args.port
    settings.log_level = args.log_level.upper()
    settings.log_dir = args.log_dir
    settings.data_dir = args.data_dir
    if args.no_log_file:
        settings.log_to_file = False

    log_path = setup_logging(
        args.log_level,
        args.log_dir,
        to_file=settings.log_to_file,
        max_bytes=settings.log_max_bytes,
        backup_count=settings.log_backup_count,
    )

    if _port_in_use(args.host, args.port):
        holder = _port_holder(args.port) or f"未能识别（netstat -ano | findstr :{args.port}）"
        logging.getLogger("blobstore.main").error(
            "port %s already in use, holder=%s", args.port, holder
        )
        print("=" * 62)
        print(f"  [启动失败] 端口 {args.port} 已被占用，说明已经有一个文件中转服在跑")
        print(f"  占用进程: {holder}")
        print("  三种处理方式，任选一种：")
        print(f"    1) 直接用已在跑的那个: http://127.0.0.1:{args.port}/health")
        print("    2) 先停掉它再启动    : taskkill /PID <上面的PID> /F")
        print(f"    3) 换个端口起新实例  : run_blob.bat --port {args.port + 1}")
        print("=" * 62)
        raise SystemExit(1)

    print("=" * 62)
    print("  跨端文件中转（YAgent Blob Store）")
    print(f"  监听    : {args.host}:{args.port}")
    print(f"  鉴权    : {'开' if settings.auth_enabled else '关'}")
    print(f"  存储目录: {settings.data_path}")
    print(f"  单文件上限: {settings.max_file_bytes / 1048576:.0f} MB")
    print(f"  保留天数: {settings.ttl_days}")
    print(f"  日志文件: {log_path if log_path else '仅控制台'}")
    print(f"  健康检查: http://127.0.0.1:{args.port}/health")
    print(f"  调试台  : http://127.0.0.1:{args.port}/")
    print("=" * 62)

    uvicorn.run(
        "blobstore.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level.lower(),
        use_colors=use_colors,
    )


if __name__ == "__main__":
    main()
