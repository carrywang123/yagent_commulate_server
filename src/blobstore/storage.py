"""内容寻址的文件存储。

磁盘布局（data_dir 下）：
    ab/cd/abcdef...     ← 内容本体，文件名 = sha256 全长
    ab/cd/abcdef....json ← 元数据（原始文件名、大小、上传者、时间）

两级前缀分桶是必要的：单目录几万个文件时，Windows 上 `glob` 和资源管理器
都会明显变慢，而清理任务需要周期性遍历。

设计要点：
  * 落盘先写临时文件，算完 sha256 再原子 rename 到最终位置。
    直接写目标路径的后果：上传中断会留下一个"名字对但内容不全"的 blob，
    而内容寻址的前提正是"名字即内容"，一旦被污染，秒传会直接返回坏文件。
  * 已存在同 sha256 时丢弃临时文件、只刷新元数据的 last_seen（去重）。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Iterator, Optional

logger = logging.getLogger(__name__)

#: sha256 十六进制串
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

#: 文件名里不允许出现的字符（Windows 保留字符 + 路径分隔符 + 控制字符）
_UNSAFE_NAME_RE = re.compile(r'[\x00-\x1f<>:"/\\|?*]')

#: Windows 保留设备名，作为文件名会导致无法创建/打开
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}

MAX_NAME_LEN = 180


class StorageError(Exception):
    """存储层的业务错误。code 用于映射 HTTP 状态。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def is_blob_id(value: str) -> bool:
    return bool(_SHA256_RE.match(str(value or "").strip().lower()))


def sanitize_filename(raw: str) -> str:
    """把上传方给的文件名收敛成安全的展示名。

    这个值只用于下载时的 Content-Disposition 与元数据展示，**不参与路径拼接**，
    但仍然要清洗：接收方很可能拿它直接落盘，`../../x` 或 `NUL` 会在对端出事。
    宁可在这里改名，也不要把危险的名字原样传下去。
    """
    name = str(raw or "").strip()
    # 只取最后一段：上传方可能传了完整路径
    name = name.replace("\\", "/").split("/")[-1]
    name = _UNSAFE_NAME_RE.sub("_", name)
    name = name.strip(" .")  # Windows 不允许结尾是空格或点
    if not name:
        return "unnamed.bin"
    stem = name.split(".")[0].upper()
    if stem in _WINDOWS_RESERVED:
        name = f"_{name}"
    if len(name) > MAX_NAME_LEN:
        suffix = Path(name).suffix[:20]
        name = name[: MAX_NAME_LEN - len(suffix)] + suffix
    return name


@dataclass
class BlobMeta:
    blob_id: str
    filename: str
    size: int
    created_at: float
    last_seen: float
    uploader: str = ""
    content_type: str = "application/octet-stream"
    #: 命中去重的次数，纯观测用
    hits: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "blob_id": self.blob_id,
            "filename": self.filename,
            "size": self.size,
            "sha256": self.blob_id,
            "created_at": self.created_at,
            "last_seen": self.last_seen,
            "uploader": self.uploader,
            "content_type": self.content_type,
            "hits": self.hits,
            "extra": self.extra,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BlobMeta":
        return cls(
            blob_id=str(data.get("blob_id", "")),
            filename=str(data.get("filename", "unnamed.bin")),
            size=int(data.get("size", 0)),
            created_at=float(data.get("created_at", 0.0)),
            last_seen=float(data.get("last_seen", data.get("created_at", 0.0))),
            uploader=str(data.get("uploader", "")),
            content_type=str(data.get("content_type", "application/octet-stream")),
            hits=int(data.get("hits", 0)),
            extra=data.get("extra") or {},
        )


class BlobStorage:
    def __init__(self, root: Path, *, chunk_bytes: int = 1024 * 1024) -> None:
        self.root = Path(root)
        self.chunk_bytes = max(64 * 1024, int(chunk_bytes))
        self.tmp_dir = self.root / "_tmp"
        self.root.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 路径
    # ------------------------------------------------------------------

    def blob_path(self, blob_id: str) -> Path:
        """blob 本体路径。调用方必须先用 is_blob_id 校验。

        因为 blob_id 被约束成 64 位十六进制，这里不可能拼出 `..`，
        路径穿越在类型层面就被排除了 —— 这正是选内容寻址的附带收益。
        """
        bid = blob_id.lower()
        if not is_blob_id(bid):
            raise StorageError("bad_request", f"非法的 blob_id: {blob_id!r}")
        return self.root / bid[:2] / bid[2:4] / bid

    def meta_path(self, blob_id: str) -> Path:
        return self.blob_path(blob_id).with_suffix(".json")

    def exists(self, blob_id: str) -> bool:
        return is_blob_id(blob_id) and self.blob_path(blob_id).is_file()

    # ------------------------------------------------------------------
    # 元数据
    # ------------------------------------------------------------------

    def read_meta(self, blob_id: str) -> Optional[BlobMeta]:
        path = self.meta_path(blob_id)
        if not path.is_file():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            logger.warning("元数据损坏，按缺失处理: %s", path)
            return None
        return BlobMeta.from_dict(data)

    def write_meta(self, meta: BlobMeta) -> None:
        path = self.meta_path(meta.blob_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        # 元数据也走临时文件 + rename：读方（下载、清理）随时可能并发读，
        # 直接覆写会让它们读到半截 JSON。
        tmp = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
        tmp.write_text(
            json.dumps(meta.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
        )
        os.replace(tmp, path)

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------

    async def save_stream(
        self,
        chunks: AsyncIterator[bytes],
        *,
        filename: str,
        uploader: str = "",
        content_type: str = "application/octet-stream",
        max_bytes: int = 0,
        expected_sha256: str = "",
    ) -> tuple[BlobMeta, bool]:
        """把字节流落盘，返回 (元数据, 是否命中去重)。

        max_bytes > 0 时超限立刻中止并删除临时文件 —— 必须边写边判，
        不能等写完再检查：那样磁盘已经被占满了。
        """
        tmp = self.tmp_dir / f"up-{uuid.uuid4().hex}.part"
        digest = hashlib.sha256()
        size = 0
        try:
            with tmp.open("wb") as handle:
                async for chunk in chunks:
                    if not chunk:
                        continue
                    size += len(chunk)
                    if max_bytes and size > max_bytes:
                        raise StorageError(
                            "too_large",
                            f"文件超过上限 {max_bytes} 字节（{max_bytes / 1048576:.0f}MB）",
                        )
                    digest.update(chunk)
                    handle.write(chunk)
            if size == 0:
                raise StorageError("bad_request", "上传内容为空")

            blob_id = digest.hexdigest()
            if expected_sha256:
                want = str(expected_sha256).strip().lower()
                if want != blob_id:
                    # 校验和不符说明传输过程中损坏了，绝不能收下：
                    # 内容寻址一旦被污染，后续所有秒传都会拿到坏文件。
                    raise StorageError(
                        "checksum_mismatch",
                        f"内容校验失败：声明 {want}，实际 {blob_id}",
                    )

            target = self.blob_path(blob_id)
            now = time.time()
            deduped = target.is_file()
            if deduped:
                meta = self.read_meta(blob_id) or BlobMeta(
                    blob_id=blob_id,
                    filename=sanitize_filename(filename),
                    size=size,
                    created_at=now,
                    last_seen=now,
                    uploader=uploader,
                    content_type=content_type,
                )
                meta.last_seen = now
                meta.hits += 1
                # 不覆盖 filename：首个上传者的名字更可能是"正确"的那个，
                # 而且后续下载方可能已经拿这个名字建立了引用。
                self.write_meta(meta)
                tmp.unlink(missing_ok=True)
                return meta, True

            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(tmp, target)
            meta = BlobMeta(
                blob_id=blob_id,
                filename=sanitize_filename(filename),
                size=size,
                created_at=now,
                last_seen=now,
                uploader=uploader,
                content_type=content_type,
            )
            self.write_meta(meta)
            return meta, False
        finally:
            # 任何异常路径都不留垃圾。成功路径上 tmp 已被 rename 走，unlink 是空操作。
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # 读取
    # ------------------------------------------------------------------

    def iter_blobs(self) -> Iterator[BlobMeta]:
        for meta_file in self.root.glob("*/*/*.json"):
            try:
                data = json.loads(meta_file.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            meta = BlobMeta.from_dict(data)
            if meta.blob_id:
                yield meta

    def delete(self, blob_id: str) -> bool:
        if not self.exists(blob_id):
            return False
        self.blob_path(blob_id).unlink(missing_ok=True)
        self.meta_path(blob_id).unlink(missing_ok=True)
        return True

    def total_bytes(self) -> int:
        return sum(meta.size for meta in self.iter_blobs())

    def stats(self) -> dict[str, Any]:
        count = 0
        total = 0
        for meta in self.iter_blobs():
            count += 1
            total += meta.size
        return {
            "total_files": count,
            "total_bytes": total,
            "total_mb": round(total / 1048576, 2),
            "data_dir": str(self.root),
        }

    # ------------------------------------------------------------------
    # 清理
    # ------------------------------------------------------------------

    def cleanup(self, ttl_seconds: float, *, now: float | None = None) -> dict[str, int]:
        """删除过期 blob 与残留临时文件。ttl_seconds <= 0 表示不做过期清理。

        过期基准用 last_seen 而不是 created_at：一份被反复引用的文件
        （比如公共素材包）不该因为首次上传时间早就被删掉。
        """
        moment = time.time() if now is None else now
        removed = 0
        freed = 0
        if ttl_seconds > 0:
            for meta in list(self.iter_blobs()):
                if moment - meta.last_seen <= ttl_seconds:
                    continue
                size = meta.size
                if self.delete(meta.blob_id):
                    removed += 1
                    freed += size

        # 残留 .part：进程被强杀时会留下。1 小时是安全余量，
        # 用更短的阈值有删掉"正在上传中"文件的风险。
        stale = 0
        for part in self.tmp_dir.glob("*.part"):
            try:
                if moment - part.stat().st_mtime > 3600:
                    part.unlink(missing_ok=True)
                    stale += 1
            except OSError:
                continue

        if removed or stale:
            logger.info(
                "cleanup: removed=%d freed=%.1fMB stale_tmp=%d",
                removed,
                freed / 1048576,
                stale,
            )
        return {"removed": removed, "freed_bytes": freed, "stale_tmp": stale}
