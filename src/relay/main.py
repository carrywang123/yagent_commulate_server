"""应用入口：组装 FastAPI、后台维护任务。"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response

from .api import api_router
from .config import Settings, get_settings
from .logging_conf import setup_logging
from .protocol import Envelope, MsgType
from .registry import ClientRegistry
from .router import Router
from .trace import build_store
from .trace_api import trace_router
from .ws import ws_router

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"
JS_SDK = STATIC_DIR / "relay-client.js"


async def _maintenance_loop(app: FastAPI) -> None:
    """后台维护：心跳探活 + 清理超时请求 + 踢掉僵尸连接 + 追踪落盘。"""
    settings: Settings = app.state.settings
    router: Router = app.state.router
    registry: ClientRegistry = app.state.registry
    tracer = getattr(app.state, "tracer", None)
    interval = max(1.0, min(settings.heartbeat_interval, 5.0))
    last_ping = 0.0
    last_trace_cleanup = time.time()

    while True:
        try:
            await asyncio.sleep(interval)
            now = time.time()

            expired = router.expire_pending(now)
            if expired:
                logger.debug("expired %d pending request(s)", expired)

            # 追踪写入是同步的（不会丢数据），这里只做定期维护：
            # WAL checkpoint，以及——当且仅当 RELAY_TRACE_PRUNE_DAYS > 0 时——清理过期历史。
            if tracer is not None and now - last_trace_cleanup > 600:
                last_trace_cleanup = now
                await asyncio.to_thread(tracer.maintain)

            send_ping = now - last_ping >= settings.heartbeat_interval
            if send_ping:
                last_ping = now

            for conn in registry.all_connections():
                if conn.closing:
                    continue
                idle = now - conn.last_seen
                if idle > settings.client_timeout:
                    logger.info(
                        "heartbeat timeout, closing: client=%s idle=%.1fs",
                        conn.client_id,
                        idle,
                    )
                    conn.mark_closing("HEARTBEAT_TIMEOUT")
                    continue
                if send_ping:
                    conn.send(Envelope(type=MsgType.PING, to=conn.client_id))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("maintenance loop error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings: Settings = app.state.settings
    log_path = _init_logging(settings)
    logger.info(
        "relay server starting: %s:%s auth=%s heartbeat=%ss timeout=%ss",
        settings.host,
        settings.port,
        "on" if settings.auth_enabled else "off",
        settings.heartbeat_interval,
        settings.client_timeout,
    )
    # log_path 仅在本进程首次初始化日志时非空（如 uvicorn --reload 的子进程）；
    # 走 run.bat 时 __main__ 已经初始化过并在启动横幅里打印了路径，这里不会重复输出。
    if log_path is not None:
        logger.info("日志文件: %s", log_path)

    # 追踪：SQLite 永久落盘，启动时把未结束的任务载回内存接着算。
    tracer = getattr(app.state, "tracer", None)
    if tracer is not None and tracer.enabled:
        stats = tracer.stats()
        logger.info(
            "跨端任务追踪已开启: 库=%s 已存任务=%s 事件=%s 保留=%s 看板=/trace",
            stats.get("db_path"),
            stats.get("tasks"),
            stats.get("events"),
            "永久" if tracer.prune_days <= 0 else f"{tracer.prune_days} 天",
        )

    task = asyncio.create_task(_maintenance_loop(app), name="relay-maintenance")
    app.state.maintenance_task = task
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        if tracer is not None:
            # 关库：提交 + WAL 合并，避免留下 -wal 文件
            try:
                tracer.close()
            except Exception:  # noqa: BLE001
                logger.debug("trace: 关闭数据库失败", exc_info=True)
        for conn in app.state.registry.all_connections():
            conn.mark_closing("server_shutdown")
        logger.info("relay server stopped")


def _init_logging(settings: Settings):
    """统一的日志初始入口，避免 create_app / lifespan 两处参数写法漂移。"""
    return setup_logging(
        settings.log_level,
        settings.log_dir,
        to_file=settings.log_to_file,
        max_bytes=settings.log_max_bytes,
        backup_count=settings.log_backup_count,
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    _init_logging(settings)

    app = FastAPI(
        title="YAgent Commulate Server",
        description="客户端消息转发 / 中转服务：WebSocket 长连接 + REST 接口",
        version="0.1.0",
        lifespan=lifespan,
    )

    registry = ClientRegistry()
    router = Router(registry, settings)
    tracer = build_store(settings)
    # 转发路径上的埋点通过 router.tracer 拿到它；为 None 时全部退化为空操作
    router.tracer = tracer

    app.state.settings = settings
    app.state.registry = registry
    app.state.router = router
    app.state.tracer = tracer

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(ws_router)
    app.include_router(api_router)
    app.include_router(trace_router)

    @app.get("/health", tags=["ops"], summary="健康检查")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "version": app.version,
            "online_clients": len(registry.client_ids()),
            "online_connections": len(registry.all_connections()),
            "auth": settings.auth_enabled,
            "ts": time.time(),
        }

    @app.get("/", include_in_schema=False)
    async def console():
        page = STATIC_DIR / "console.html"
        if page.exists():
            return FileResponse(page, media_type="text/html")
        return JSONResponse({"service": "yagent-commulate-server", "docs": "/docs"})

    @app.get("/trace", include_in_schema=False)
    async def trace_console():
        """跨端任务追踪看板（零依赖单文件页面）。"""
        page = STATIC_DIR / "trace.html"
        if page.exists():
            return FileResponse(page, media_type="text/html")
        return JSONResponse({"error": "trace.html not found"}, status_code=404)

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon() -> Response:
        return Response(status_code=204)

    @app.get("/sdk/relay-client.js", include_in_schema=False)
    async def js_sdk():
        """直接向前端提供 JS SDK，Vue3 工程可以
        import('http://<server>/sdk/relay-client.js') 或拷走自用。"""
        if JS_SDK.exists():
            return FileResponse(
                JS_SDK,
                media_type="application/javascript",
                headers={"Cache-Control": "no-cache"},
            )
        return JSONResponse({"error": "sdk not found"}, status_code=404)

    return app


app = create_app()
