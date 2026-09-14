"""应用入口：FastAPI 组装 + 后台清理任务。

## 为什么上传用原始字节流而不是 multipart/form-data

FastAPI 的 `UploadFile` 需要 `python-multipart`，而本仓库当前的 .venv 里没有它
（实测：relay 的依赖只有 fastapi/uvicorn/pydantic/pydantic-settings）。
为了给中转服务加个上传口而引入新依赖并不划算，何况：

  * 原始流更省内存：`request.stream()` 逐块给我们字节，全程不落整份到内存；
    multipart 解析器则要处理边界、可能缓冲整个 part。
  * 客户端更简单：`requests.post(url, data=open(f,'rb'))` 或
    `curl --data-binary @file` 即可，不需要构造表单。
  * 文件名/上传者这些元数据放请求头（X-Filename / X-Uploader），比表单字段更好取。

代价是不能用浏览器 <form> 直接传。调试台用 fetch(body: File) 一样能传，所以不构成问题。
"""

from __future__ import annotations

import asyncio
import logging
import time
import urllib.parse
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response

from blobstore.config import Settings, get_settings
from blobstore.logging_conf import setup_logging
from blobstore.storage import BlobStorage, StorageError, is_blob_id, sanitize_filename
from blobstore.security import require_token

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

#: StorageError.code -> HTTP 状态
_STATUS_MAP = {
    "bad_request": 400,
    "too_large": 413,
    "checksum_mismatch": 422,
    "not_found": 404,
    "quota_exceeded": 507,
}


def _storage_http_error(exc: StorageError) -> HTTPException:
    return HTTPException(
        status_code=_STATUS_MAP.get(exc.code, 500),
        detail={"code": exc.code, "message": exc.message},
    )


def _decode_header(raw: str | None) -> str:
    """HTTP 头只能放 latin-1，中文文件名必须由客户端 percent-encode。

    这里同时容忍未编码的裸值：不 unquote 的后果是中文名变成乱码，
    而 unquote 一个本就没编码的 ASCII 名是无损的，所以无条件解一次更稳。
    """
    if not raw:
        return ""
    try:
        return urllib.parse.unquote(raw, encoding="utf-8", errors="replace").strip()
    except Exception:
        return raw.strip()


#: curl 不带 -H 'Content-Type' 时会自作主张地发这个值。
#: 不过滤的后果：它会被当成真实类型存进元数据，下载时再原样回放，
#: 浏览器拿到一个“表单编码”的 zip 会行为奇怪。而用 curl 上传恰好是技能文档里的主路径。
_BOGUS_CONTENT_TYPES = {
    "application/x-www-form-urlencoded",
    "",
}


def _pick_content_type(explicit: str | None, raw: str | None) -> str:
    """X-Content-Type 优先；否则用 Content-Type，但过滤揉 curl 默认值。"""
    for candidate in (explicit, raw):
        value = (candidate or "").split(";")[0].strip().lower()
        if value and value not in _BOGUS_CONTENT_TYPES:
            return value
    return "application/octet-stream"


async def _cleanup_loop(app: FastAPI) -> None:
    settings: Settings = app.state.settings
    storage: BlobStorage = app.state.storage
    interval = max(30.0, float(settings.cleanup_interval))
    while True:
        try:
            await asyncio.sleep(interval)
            # 清理是同步的磁盘遍历，放到线程池里，不阻塞正在进行的上传/下载
            await asyncio.to_thread(storage.cleanup, settings.ttl_seconds)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("cleanup loop error")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    setup_logging(
        settings.log_level,
        settings.log_dir,
        to_file=settings.log_to_file,
        max_bytes=settings.log_max_bytes,
        backup_count=settings.log_backup_count,
    )

    storage = BlobStorage(settings.data_path, chunk_bytes=settings.chunk_bytes)

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        logger.info(
            "blob store starting: %s:%s auth=%s dir=%s ttl=%.1fd max_file=%.0fMB",
            settings.host,
            settings.port,
            "on" if settings.auth_enabled else "off",
            settings.data_path,
            settings.ttl_days,
            settings.max_file_bytes / 1048576,
        )
        # 启动时先扫一次：进程可能停了很久，期间该过期的都该清掉
        await asyncio.to_thread(storage.cleanup, settings.ttl_seconds)
        task = asyncio.create_task(_cleanup_loop(application), name="blob-cleanup")
        application.state.cleanup_task = task
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            logger.info("blob store stopped")

    app = FastAPI(
        title="YAgent Blob Store",
        description="跨端文件中转：内容寻址上传 / 下载，供 crossnode 消息引用",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.state.settings = settings
    app.state.storage = storage

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        # 让浏览器端能读到这几个响应头，否则 fetch 里拿不到校验和
        expose_headers=["X-Blob-Id", "X-Blob-Size", "X-Blob-Filename", "X-Blob-Sha256"],
    )

    # ------------------------------------------------------------------
    # 运维
    # ------------------------------------------------------------------

    @app.get("/health", tags=["ops"], summary="健康检查（不鉴权，便于探活）")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "yagent-blob-store",
            "version": app.version,
            "auth": settings.auth_enabled,
            "max_file_bytes": settings.max_file_bytes,
            "ttl_days": settings.ttl_days,
            "ts": time.time(),
        }

    @app.get("/api/stats", tags=["ops"], summary="存储统计")
    async def stats(_: None = Depends(require_token)) -> dict[str, Any]:
        info = await asyncio.to_thread(storage.stats)
        info["max_total_bytes"] = settings.max_total_bytes
        info["max_file_bytes"] = settings.max_file_bytes
        info["ttl_days"] = settings.ttl_days
        return info

    @app.post("/api/cleanup", tags=["ops"], summary="立即执行一次过期清理")
    async def cleanup_now(_: None = Depends(require_token)) -> dict[str, Any]:
        return await asyncio.to_thread(storage.cleanup, settings.ttl_seconds)

    # ------------------------------------------------------------------
    # 上传 / 下载
    # ------------------------------------------------------------------

    @app.head("/api/blob/{blob_id}", tags=["blob"], summary="探测是否已存在（秒传用）")
    async def head_blob(
        blob_id: str, response: Response, _: None = Depends(require_token)
    ) -> Response:
        if not is_blob_id(blob_id):
            raise HTTPException(
                status_code=400, detail={"code": "bad_request", "message": "非法 blob_id"}
            )
        meta = await asyncio.to_thread(storage.read_meta, blob_id)
        if meta is None or not storage.exists(blob_id):
            return Response(status_code=404)
        return Response(
            status_code=200,
            headers={
                "X-Blob-Id": meta.blob_id,
                "X-Blob-Size": str(meta.size),
                "X-Blob-Sha256": meta.blob_id,
                "Content-Length": str(meta.size),
            },
        )

    @app.get("/api/blob/{blob_id}/meta", tags=["blob"], summary="查询元数据")
    async def blob_meta(blob_id: str, _: None = Depends(require_token)) -> dict[str, Any]:
        if not is_blob_id(blob_id):
            raise HTTPException(
                status_code=400, detail={"code": "bad_request", "message": "非法 blob_id"}
            )
        meta = await asyncio.to_thread(storage.read_meta, blob_id)
        if meta is None or not storage.exists(blob_id):
            raise HTTPException(
                status_code=404, detail={"code": "not_found", "message": "blob 不存在或已过期"}
            )
        return meta.to_dict()

    @app.post("/api/blob", tags=["blob"], summary="上传（请求体为原始字节流）")
    @app.put("/api/blob", tags=["blob"], summary="上传（同 POST，供 curl -T 使用）")
    async def upload(request: Request, _: None = Depends(require_token)) -> dict[str, Any]:
        filename = sanitize_filename(
            _decode_header(request.headers.get("x-filename")) or "unnamed.bin"
        )
        uploader = _decode_header(request.headers.get("x-uploader"))
        expected = (request.headers.get("x-sha256") or "").strip().lower()
        content_type = _pick_content_type(
            request.headers.get("x-content-type"), request.headers.get("content-type")
        )

        # 先看 Content-Length 提前拒绝，省掉白传几百 MB 再报错。
        # 但不能只依赖它：chunked 编码时没有这个头，所以 save_stream 里仍要边写边判。
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > settings.max_file_bytes:
            raise HTTPException(
                status_code=413,
                detail={
                    "code": "too_large",
                    "message": f"文件超过上限 {settings.max_file_bytes / 1048576:.0f}MB",
                },
            )

        # 配额：先清理再判定，避免"其实全是过期文件"却拒绝新上传
        if settings.max_total_bytes > 0:
            used = await asyncio.to_thread(storage.total_bytes)
            if used > settings.max_total_bytes:
                await asyncio.to_thread(storage.cleanup, settings.ttl_seconds)
                used = await asyncio.to_thread(storage.total_bytes)
            if used > settings.max_total_bytes:
                raise HTTPException(
                    status_code=507,
                    detail={
                        "code": "quota_exceeded",
                        "message": f"存储已用 {used / 1048576:.0f}MB，超过上限 "
                        f"{settings.max_total_bytes / 1048576:.0f}MB，请联系管理员清理",
                    },
                )

        async def body_chunks() -> AsyncIterator[bytes]:
            async for chunk in request.stream():
                yield chunk

        try:
            meta, deduped = await storage.save_stream(
                body_chunks(),
                filename=filename,
                uploader=uploader,
                content_type=content_type,
                max_bytes=settings.max_file_bytes,
                expected_sha256=expected,
            )
        except StorageError as exc:
            raise _storage_http_error(exc) from exc

        logger.info(
            "upload ok: %s %s %.2fMB dedup=%s uploader=%s",
            meta.blob_id[:12],
            meta.filename,
            meta.size / 1048576,
            deduped,
            uploader or "-",
        )
        result = meta.to_dict()
        result["deduplicated"] = deduped
        result["download_path"] = f"/api/blob/{meta.blob_id}"
        result["expires_at"] = (
            meta.last_seen + settings.ttl_seconds if settings.ttl_seconds > 0 else 0
        )
        return result

    @app.get("/api/blob/{blob_id}", tags=["blob"], summary="下载")
    async def download(blob_id: str, _: None = Depends(require_token)) -> FileResponse:
        if not is_blob_id(blob_id):
            raise HTTPException(
                status_code=400, detail={"code": "bad_request", "message": "非法 blob_id"}
            )
        if not storage.exists(blob_id):
            raise HTTPException(
                status_code=404, detail={"code": "not_found", "message": "blob 不存在或已过期"}
            )
        meta = await asyncio.to_thread(storage.read_meta, blob_id)
        name = meta.filename if meta else f"{blob_id[:12]}.bin"
        # 用 RFC 5987 的 filename* 传中文名；filename= 留个 ASCII 兜底给老客户端
        ascii_fallback = name.encode("ascii", "replace").decode("ascii").replace("?", "_")
        quoted = urllib.parse.quote(name, safe="")
        return FileResponse(
            path=storage.blob_path(blob_id),
            media_type=(meta.content_type if meta else "application/octet-stream"),
            headers={
                "Content-Disposition": (
                    f'attachment; filename="{ascii_fallback}"; filename*=UTF-8\'\'{quoted}'
                ),
                "X-Blob-Id": blob_id,
                "X-Blob-Sha256": blob_id,
                "X-Blob-Filename": quoted,
                "X-Blob-Size": str(meta.size if meta else 0),
            },
        )

    @app.delete("/api/blob/{blob_id}", tags=["blob"], summary="删除")
    async def delete(blob_id: str, _: None = Depends(require_token)) -> dict[str, Any]:
        if not is_blob_id(blob_id):
            raise HTTPException(
                status_code=400, detail={"code": "bad_request", "message": "非法 blob_id"}
            )
        removed = await asyncio.to_thread(storage.delete, blob_id)
        if not removed:
            raise HTTPException(
                status_code=404, detail={"code": "not_found", "message": "blob 不存在"}
            )
        return {"ok": True, "blob_id": blob_id}

    @app.get("/api/blobs", tags=["blob"], summary="列出全部（调试用）")
    async def list_blobs(
        limit: int = 100, _: None = Depends(require_token)
    ) -> dict[str, Any]:
        def collect() -> list[dict[str, Any]]:
            items = [meta.to_dict() for meta in storage.iter_blobs()]
            items.sort(key=lambda item: item.get("last_seen", 0), reverse=True)
            return items[: max(1, min(int(limit), 1000))]

        items = await asyncio.to_thread(collect)
        return {"count": len(items), "blobs": items}

    @app.get("/", include_in_schema=False)
    async def console():
        page = STATIC_DIR / "console.html"
        if page.exists():
            return FileResponse(page, media_type="text/html")
        return JSONResponse({"service": "yagent-blob-store", "docs": "/docs"})

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon() -> Response:
        return Response(status_code=204)

    return app


app = create_app()
