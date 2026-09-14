"""YAgent Blob Store 客户端（零第三方依赖，只用标准库）。

## 为什么不用 requests / httpx

这份 SDK 会被 yagent（agent 侧）直接 import。yagent 的依赖树已经很大，
再加一层传递依赖不划算；而上传下载只需要 urllib 就够，标准库版本还有个好处：
**在任何 Python 环境里都能跑**，包括没装依赖的排障现场。

## 核心用法

    up = BlobClient("http://10.1.42.2:8771").upload("D:/work/pack.zip")
    # up.url 写进跨端消息 → 对端：
    BlobClient("http://10.1.42.2:8771").download(up.blob_id, "D:/dest/pack.zip")

## 秒传

upload() 默认先在本地算 sha256，再 HEAD 一次服务端。命中就直接返回，
一个字节都不用传。大素材包反复在两台机器间流转时，这一步能省掉绝大部分流量。
"""

from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Callable, Optional

__all__ = ["BlobClient", "BlobError", "BlobInfo", "sha256_file"]

DEFAULT_CHUNK = 1024 * 1024
DEFAULT_TIMEOUT = 300.0


class BlobError(Exception):
    """文件中转失败。code 与服务端返回的一致，便于上层决定是否重试。"""

    def __init__(self, code: str, message: str, status: int = 0) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status

    def __str__(self) -> str:  # pragma: no cover - 展示用
        return f"[{self.code}] {self.message}"


@dataclass
class BlobInfo:
    blob_id: str
    filename: str
    size: int
    url: str
    sha256: str = ""
    expires_at: float = 0.0
    deduplicated: bool = False
    content_type: str = "application/octet-stream"

    def to_dict(self) -> dict[str, Any]:
        return {
            "blob_id": self.blob_id,
            "filename": self.filename,
            "size": self.size,
            "url": self.url,
            "sha256": self.sha256 or self.blob_id,
            "expires_at": self.expires_at,
            "content_type": self.content_type,
        }


def sha256_file(path: str | Path, chunk: int = DEFAULT_CHUNK) -> tuple[str, int]:
    """返回 (sha256_hex, size)。分块读，避免大文件占满内存。"""
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as handle:
        while True:
            block = handle.read(chunk)
            if not block:
                break
            size += len(block)
            digest.update(block)
    return digest.hexdigest(), size


class _FileStream:
    """给 urllib 用的可读包装，顺带回报进度。

    urllib 需要 Content-Length + 一个 read() 对象来做流式上传；
    直接传 file object 也行，但那样拿不到进度，几百 MB 的上传会像卡死。
    """

    def __init__(
        self,
        handle: BinaryIO,
        total: int,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        self._handle = handle
        self._total = total
        self._sent = 0
        self._on_progress = on_progress

    def read(self, amount: int = -1) -> bytes:
        block = self._handle.read(amount)
        if block:
            self._sent += len(block)
            if self._on_progress:
                try:
                    self._on_progress(self._sent, self._total)
                except Exception:
                    pass  # 进度回调出错绝不能中断传输
        return block

    def __len__(self) -> int:
        return self._total


class BlobClient:
    def __init__(
        self,
        base_url: str,
        token: str = "",
        *,
        timeout: float = DEFAULT_TIMEOUT,
        chunk: int = DEFAULT_CHUNK,
    ) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        if not self.base_url:
            raise ValueError("base_url 不能为空")
        if "://" not in self.base_url:
            self.base_url = "http://" + self.base_url
        self.token = str(token or "")
        self.timeout = float(timeout)
        self.chunk = int(chunk)

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    def _headers(self, extra: Optional[dict[str, str]] = None) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.token:
            headers["X-Auth-Token"] = self.token
        if extra:
            headers.update({k: v for k, v in extra.items() if v})
        return headers

    def _request(
        self,
        method: str,
        path: str,
        *,
        data: Any = None,
        headers: Optional[dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> tuple[int, dict[str, str], bytes]:
        url = self.base_url + path
        request = urllib.request.Request(
            url, data=data, headers=self._headers(headers), method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout or self.timeout) as resp:
                return resp.status, dict(resp.headers), resp.read()
        except urllib.error.HTTPError as exc:
            body = b""
            try:
                body = exc.read()
            except Exception:
                pass
            raise self._to_error(exc.code, body) from exc
        except urllib.error.URLError as exc:
            raise BlobError(
                "service_unreachable",
                f"无法连接文件中转服务 {self.base_url}（{exc.reason}）",
            ) from exc

    @staticmethod
    def _to_error(status: int, body: bytes) -> BlobError:
        code = "internal_error"
        message = body.decode("utf-8", "replace")[:500]
        try:
            parsed = json.loads(body.decode("utf-8"))
            detail = parsed.get("detail") if isinstance(parsed, dict) else None
            if isinstance(detail, dict):
                code = str(detail.get("code") or code)
                message = str(detail.get("message") or message)
            elif isinstance(detail, str):
                message = detail
        except Exception:
            pass
        if status == 401:
            code = "unauthorized"
        elif status == 404 and code == "internal_error":
            code = "not_found"
        return BlobError(code, message or f"HTTP {status}", status=status)

    # ------------------------------------------------------------------
    # 公开接口
    # ------------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        _, _, body = self._request("GET", "/health", timeout=10.0)
        return json.loads(body.decode("utf-8"))

    def exists(self, blob_id: str) -> bool:
        try:
            status, _, _ = self._request("HEAD", f"/api/blob/{blob_id}", timeout=30.0)
            return status == 200
        except BlobError as exc:
            if exc.code == "not_found" or exc.status == 404:
                return False
            raise

    def meta(self, blob_id: str) -> dict[str, Any]:
        _, _, body = self._request("GET", f"/api/blob/{blob_id}/meta", timeout=30.0)
        return json.loads(body.decode("utf-8"))

    def download_url(self, blob_id: str, *, with_token: bool = True) -> str:
        """完整下载 URL。

        with_token=True 时把 token 拼进 query —— 这个 URL 会被写进跨端消息，
        对端不一定知道 token。安全边界是"能拿到这条消息的人才能下载"，
        与消息本身同级，没有额外泄露面。
        """
        url = f"{self.base_url}/api/blob/{blob_id}"
        if with_token and self.token:
            url += "?token=" + urllib.parse.quote(self.token, safe="")
        return url

    def upload(
        self,
        path: str | Path,
        *,
        filename: str = "",
        uploader: str = "",
        fast_path: bool = True,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> BlobInfo:
        """上传本地文件，返回可写进跨端消息的信息。

        fast_path=True 时先算 sha256 并探测服务端，命中则跳过传输（秒传）。
        """
        source = Path(path).expanduser()
        if not source.is_file():
            raise BlobError("not_found", f"本地文件不存在: {source}")

        digest, size = sha256_file(source, self.chunk)
        if size == 0:
            raise BlobError("bad_request", f"文件为空，无需传输: {source}")

        name = filename or source.name
        quoted_name = urllib.parse.quote(name, safe="")

        if fast_path and self.exists(digest):
            info = self.meta(digest)
            return BlobInfo(
                blob_id=digest,
                filename=str(info.get("filename") or name),
                size=int(info.get("size") or size),
                url=self.download_url(digest),
                sha256=digest,
                expires_at=float(info.get("last_seen") or 0.0),
                deduplicated=True,
                content_type=str(info.get("content_type") or "application/octet-stream"),
            )

        headers = {
            "X-Filename": quoted_name,
            "X-Sha256": digest,
            "Content-Type": "application/octet-stream",
            "Content-Length": str(size),
        }
        if uploader:
            headers["X-Uploader"] = urllib.parse.quote(uploader, safe="")

        with source.open("rb") as handle:
            stream = _FileStream(handle, size, on_progress)
            _, _, body = self._request("POST", "/api/blob", data=stream, headers=headers)

        result = json.loads(body.decode("utf-8"))
        return BlobInfo(
            blob_id=str(result["blob_id"]),
            filename=str(result.get("filename") or name),
            size=int(result.get("size") or size),
            url=self.download_url(str(result["blob_id"])),
            sha256=str(result.get("sha256") or digest),
            expires_at=float(result.get("expires_at") or 0.0),
            deduplicated=bool(result.get("deduplicated")),
            content_type=str(result.get("content_type") or "application/octet-stream"),
        )

    def download(
        self,
        blob_id_or_url: str,
        target: str | Path,
        *,
        verify: bool = True,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> Path:
        """下载到本地并校验。返回落地路径。

        verify=True 时下载完对比 sha256。**强烈建议保留**：
        没有这一步，被截断的半个文件会被当成成功交付，
        而这类故障在下游（解压失败、图片打不开）才暴露，极难定位。

        先写 .part 再 rename：中断时不会留下一个"看起来完整"的坏文件。
        """
        blob_id, url = self._resolve(blob_id_or_url)

        destination = Path(target).expanduser()
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = destination.with_name(destination.name + ".part")

        request = urllib.request.Request(url, headers=self._headers(), method="GET")
        digest = hashlib.sha256()
        written = 0
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as resp:
                total = int(resp.headers.get("Content-Length") or 0)
                with temp.open("wb") as handle:
                    while True:
                        block = resp.read(self.chunk)
                        if not block:
                            break
                        handle.write(block)
                        digest.update(block)
                        written += len(block)
                        if on_progress:
                            try:
                                on_progress(written, total)
                            except Exception:
                                pass
        except urllib.error.HTTPError as exc:
            temp.unlink(missing_ok=True)
            body = b""
            try:
                body = exc.read()
            except Exception:
                pass
            raise self._to_error(exc.code, body) from exc
        except urllib.error.URLError as exc:
            temp.unlink(missing_ok=True)
            raise BlobError(
                "service_unreachable",
                f"无法连接文件中转服务（{exc.reason}）",
            ) from exc
        except Exception:
            temp.unlink(missing_ok=True)
            raise

        actual = digest.hexdigest()
        if verify and blob_id and actual != blob_id:
            temp.unlink(missing_ok=True)
            raise BlobError(
                "checksum_mismatch",
                f"下载内容校验失败：期望 {blob_id}，实际 {actual}（文件可能被截断）",
            )

        os.replace(temp, destination)
        return destination

    def _resolve(self, blob_id_or_url: str) -> tuple[str, str]:
        """既接受纯 blob_id，也接受完整 URL。

        宽容是必要的：跨端消息里带的是 URL，而模型也可能只转述了 blob_id。
        从 URL 里把 64 位 hex 抠出来，才能继续做校验。
        """
        raw = str(blob_id_or_url or "").strip()
        if not raw:
            raise BlobError("bad_request", "缺少 blob_id 或下载地址")
        if "://" in raw:
            path = urllib.parse.urlparse(raw).path
            candidate = path.rstrip("/").split("/")[-1].lower()
            blob_id = candidate if len(candidate) == 64 else ""
            return blob_id, raw
        blob_id = raw.lower()
        return blob_id, self.download_url(blob_id)

    def delete(self, blob_id: str) -> bool:
        try:
            self._request("DELETE", f"/api/blob/{blob_id}", timeout=30.0)
            return True
        except BlobError as exc:
            if exc.code == "not_found":
                return False
            raise

    def stats(self) -> dict[str, Any]:
        _, _, body = self._request("GET", "/api/stats", timeout=30.0)
        return json.loads(body.decode("utf-8"))
