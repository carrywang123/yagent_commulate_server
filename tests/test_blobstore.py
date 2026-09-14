"""Blob Store 单元测试。

用 httpx.ASGITransport 直接打 app，不需要真的起端口。
覆盖重点是「会造成事故的路径」，不是接口清单：
  * 校验和不符必须拒收（内容寻址被污染 → 后续秒传全是坏文件）
  * 超限必须在写盘过程中拦下（不能等写完才判，磁盘已经满了）
  * 非法 blob_id 必须拒绝（路径穿越）
  * 中文文件名往返不丢
  * 过期清理只删该删的
"""

from __future__ import annotations

import hashlib
import time
import urllib.parse

import httpx
import pytest

from blobstore.config import Settings
from blobstore.main import create_app
from blobstore.storage import BlobStorage, is_blob_id, sanitize_filename


def make_settings(tmp_path, **kwargs) -> Settings:
    base = {
        "data_dir": str(tmp_path / "blobs"),
        "log_to_file": False,
        "auth_token": "",
        "cleanup_interval": 3600.0,
    }
    base.update(kwargs)
    return Settings(**base)


async def make_client(settings: Settings):
    app = create_app(settings)
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test"), app


# ----------------------------------------------------------------------
# 纯函数
# ----------------------------------------------------------------------


def test_is_blob_id():
    good = hashlib.sha256(b"x").hexdigest()
    assert is_blob_id(good)
    assert is_blob_id(good.upper())  # 大写要能识别（会被 lower 归一）
    assert not is_blob_id("")
    assert not is_blob_id("../../etc/passwd")
    assert not is_blob_id(good[:-1])  # 长度差一位
    assert not is_blob_id(good[:-1] + "g")  # 非十六进制字符


def test_sanitize_filename():
    assert sanitize_filename("a.txt") == "a.txt"
    # 路径必须被剥掉，否则接收方直接落盘会写到目录外
    assert sanitize_filename("../../../etc/passwd") == "passwd"
    assert sanitize_filename(r"C:\Windows\evil.exe") == "evil.exe"
    assert sanitize_filename("") == "unnamed.bin"
    assert sanitize_filename("   ") == "unnamed.bin"
    # Windows 保留名加前缀，否则对端建不出这个文件
    assert sanitize_filename("NUL") == "_NUL"
    assert sanitize_filename("con.txt") == "_con.txt"
    assert sanitize_filename('a<>:"|?*b.txt') == "a_______b.txt"
    assert sanitize_filename("中文 名字.zip") == "中文 名字.zip"
    # 结尾的点和空格在 Windows 上会被静默丢弃，提前处理掉
    assert sanitize_filename("trailing. ") == "trailing"
    long_name = "x" * 400 + ".zip"
    assert len(sanitize_filename(long_name)) <= 180


def test_blob_path_rejects_traversal(tmp_path):
    storage = BlobStorage(tmp_path / "b")
    with pytest.raises(Exception):
        storage.blob_path("../../evil")


# ----------------------------------------------------------------------
# 上传 / 下载
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_download_roundtrip(tmp_path):
    settings = make_settings(tmp_path)
    client, app = await make_client(settings)
    async with client:
        payload = b"hello crossnode" * 100
        digest = hashlib.sha256(payload).hexdigest()
        resp = await client.post(
            "/api/blob",
            content=payload,
            headers={"X-Filename": "demo.bin", "X-Uploader": "alice"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["blob_id"] == digest
        assert body["sha256"] == digest
        assert body["size"] == len(payload)
        assert body["filename"] == "demo.bin"
        assert body["uploader"] == "alice"
        assert body["deduplicated"] is False
        assert body["expires_at"] > time.time()

        got = await client.get(f"/api/blob/{digest}")
        assert got.status_code == 200
        assert got.content == payload
        assert got.headers["x-blob-sha256"] == digest
        assert "demo.bin" in got.headers["content-disposition"]


@pytest.mark.asyncio
async def test_upload_dedup(tmp_path):
    settings = make_settings(tmp_path)
    client, _ = await make_client(settings)
    async with client:
        payload = b"same content"
        first = await client.post(
            "/api/blob", content=payload, headers={"X-Filename": "a.txt"}
        )
        second = await client.post(
            "/api/blob", content=payload, headers={"X-Filename": "b.txt"}
        )
        assert first.json()["deduplicated"] is False
        assert second.json()["deduplicated"] is True
        assert first.json()["blob_id"] == second.json()["blob_id"]
        # 首个上传者的文件名保留，不被后来者改写
        assert second.json()["filename"] == "a.txt"

        stats = await client.get("/api/stats")
        assert stats.json()["total_files"] == 1  # 去重生效，没有存两份


@pytest.mark.asyncio
async def test_head_probe_enables_fast_path(tmp_path):
    """秒传：上传前先 HEAD，命中就完全不用传字节。"""
    settings = make_settings(tmp_path)
    client, _ = await make_client(settings)
    async with client:
        payload = b"probe me"
        digest = hashlib.sha256(payload).hexdigest()

        missing = await client.head(f"/api/blob/{digest}")
        assert missing.status_code == 404

        await client.post("/api/blob", content=payload, headers={"X-Filename": "p.bin"})

        hit = await client.head(f"/api/blob/{digest}")
        assert hit.status_code == 200
        assert hit.headers["x-blob-size"] == str(len(payload))


@pytest.mark.asyncio
async def test_checksum_mismatch_rejected(tmp_path):
    """声明的 sha256 与实际不符 → 必须拒收，且不留下任何 blob。"""
    settings = make_settings(tmp_path)
    client, _ = await make_client(settings)
    async with client:
        payload = b"real content"
        wrong = hashlib.sha256(b"other").hexdigest()
        resp = await client.post(
            "/api/blob",
            content=payload,
            headers={"X-Filename": "x.bin", "X-Sha256": wrong},
        )
        assert resp.status_code == 422
        assert resp.json()["detail"]["code"] == "checksum_mismatch"

        stats = await client.get("/api/stats")
        assert stats.json()["total_files"] == 0
        # 临时文件也不能残留
        leftovers = list((settings.data_path / "_tmp").glob("*.part"))
        assert leftovers == []


@pytest.mark.asyncio
async def test_oversize_rejected_by_stream(tmp_path):
    """超限必须在写入过程中被拦下。

    这里刻意用 chunked（异步生成器作为 content，httpx 不会设 Content-Length），
    绕开请求头预检，验证 save_stream 内部的边写边判确实生效。
    """
    settings = make_settings(tmp_path, max_file_bytes=1024)
    client, _ = await make_client(settings)
    async with client:

        async def gen():
            for _ in range(10):
                yield b"x" * 512

        resp = await client.post(
            "/api/blob", content=gen(), headers={"X-Filename": "big.bin"}
        )
        assert resp.status_code == 413, resp.text
        assert resp.json()["detail"]["code"] == "too_large"
        assert list((settings.data_path / "_tmp").glob("*.part")) == []


@pytest.mark.asyncio
async def test_oversize_rejected_by_content_length(tmp_path):
    settings = make_settings(tmp_path, max_file_bytes=1024)
    client, _ = await make_client(settings)
    async with client:
        resp = await client.post(
            "/api/blob", content=b"y" * 5000, headers={"X-Filename": "big.bin"}
        )
        assert resp.status_code == 413
        assert resp.json()["detail"]["code"] == "too_large"


@pytest.mark.asyncio
async def test_empty_upload_rejected(tmp_path):
    settings = make_settings(tmp_path)
    client, _ = await make_client(settings)
    async with client:
        resp = await client.post("/api/blob", content=b"", headers={"X-Filename": "e"})
        assert resp.status_code == 400
        assert resp.json()["detail"]["code"] == "bad_request"


@pytest.mark.asyncio
async def test_unicode_filename_roundtrip(tmp_path):
    """中文名：HTTP 头只能放 latin-1，客户端 percent-encode，服务端要能还原。"""
    settings = make_settings(tmp_path)
    client, _ = await make_client(settings)
    async with client:
        name = "跨端 素材包(v2).zip"
        resp = await client.post(
            "/api/blob",
            content=b"zipdata",
            headers={"X-Filename": urllib.parse.quote(name)},
        )
        assert resp.status_code == 200
        assert resp.json()["filename"] == name

        blob_id = resp.json()["blob_id"]
        got = await client.get(f"/api/blob/{blob_id}")
        disposition = got.headers["content-disposition"]
        assert urllib.parse.quote(name, safe="") in disposition
        assert urllib.parse.unquote(got.headers["x-blob-filename"]) == name


@pytest.mark.asyncio
async def test_bad_blob_id_rejected(tmp_path):
    settings = make_settings(tmp_path)
    client, _ = await make_client(settings)
    async with client:
        for bad in ("..", "abc", "%2e%2e%2fetc", "z" * 64):
            resp = await client.get(f"/api/blob/{bad}")
            assert resp.status_code in (400, 404), (bad, resp.status_code)


@pytest.mark.asyncio
async def test_download_missing(tmp_path):
    settings = make_settings(tmp_path)
    client, _ = await make_client(settings)
    async with client:
        absent = hashlib.sha256(b"nope").hexdigest()
        resp = await client.get(f"/api/blob/{absent}")
        assert resp.status_code == 404
        assert resp.json()["detail"]["code"] == "not_found"


@pytest.mark.asyncio
async def test_delete(tmp_path):
    settings = make_settings(tmp_path)
    client, _ = await make_client(settings)
    async with client:
        resp = await client.post(
            "/api/blob", content=b"to delete", headers={"X-Filename": "d.bin"}
        )
        blob_id = resp.json()["blob_id"]
        assert (await client.delete(f"/api/blob/{blob_id}")).status_code == 200
        assert (await client.get(f"/api/blob/{blob_id}")).status_code == 404
        assert (await client.delete(f"/api/blob/{blob_id}")).status_code == 404


# ----------------------------------------------------------------------
# 鉴权
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auth_required(tmp_path):
    settings = make_settings(tmp_path, auth_token="s3cret")
    client, _ = await make_client(settings)
    async with client:
        # health 必须不鉴权，否则探活脚本要配 token
        assert (await client.get("/health")).status_code == 200

        assert (await client.post("/api/blob", content=b"x")).status_code == 401
        assert (await client.get("/api/stats")).status_code == 401

        ok = await client.post(
            "/api/blob",
            content=b"authorized",
            headers={"X-Filename": "a.bin", "X-Auth-Token": "s3cret"},
        )
        assert ok.status_code == 200
        blob_id = ok.json()["blob_id"]

        # query 参数也要能带 token：下载 URL 会交给浏览器 / curl，塞不进请求头
        via_query = await client.get(f"/api/blob/{blob_id}?token=s3cret")
        assert via_query.status_code == 200

        bearer = await client.get(
            f"/api/blob/{blob_id}", headers={"Authorization": "Bearer s3cret"}
        )
        assert bearer.status_code == 200

        assert (await client.get(f"/api/blob/{blob_id}?token=wrong")).status_code == 401


def test_auth_falls_back_to_relay_token(monkeypatch, tmp_path):
    """没配 BLOB_AUTH_TOKEN 时借用 RELAY_AUTH_TOKEN，避免一个开一个关的错配。"""
    monkeypatch.setenv("RELAY_AUTH_TOKEN", "relay-token")
    settings = make_settings(tmp_path)
    assert settings.auth_token_effective == "relay-token"
    assert settings.auth_enabled

    explicit = make_settings(tmp_path, auth_token="own")
    assert explicit.auth_token_effective == "own"  # 自己的配置优先


# ----------------------------------------------------------------------
# 清理
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cleanup_removes_expired_only(tmp_path):
    settings = make_settings(tmp_path, ttl_days=1.0)
    storage = BlobStorage(settings.data_path)

    async def one(data: bytes):
        async def gen():
            yield data

        return await storage.save_stream(gen(), filename="f.bin")

    fresh, _ = await one(b"fresh data")
    old, _ = await one(b"old data")

    # 把其中一个的 last_seen 改成 3 天前
    old.last_seen = time.time() - 3 * 86400
    storage.write_meta(old)

    result = storage.cleanup(settings.ttl_seconds)
    assert result["removed"] == 1
    assert storage.exists(fresh.blob_id)
    assert not storage.exists(old.blob_id)


@pytest.mark.asyncio
async def test_cleanup_disabled_when_ttl_zero(tmp_path):
    storage = BlobStorage(tmp_path / "b")

    async def gen():
        yield b"keep me"

    meta, _ = await storage.save_stream(gen(), filename="k.bin")
    meta.last_seen = 0.0  # 1970 年
    storage.write_meta(meta)

    assert storage.cleanup(0)["removed"] == 0
    assert storage.exists(meta.blob_id)


@pytest.mark.asyncio
async def test_quota_exceeded(tmp_path):
    settings = make_settings(tmp_path, max_total_bytes=100, ttl_days=0)
    client, _ = await make_client(settings)
    async with client:
        first = await client.post(
            "/api/blob", content=b"z" * 200, headers={"X-Filename": "1.bin"}
        )
        assert first.status_code == 200  # 第一次不查（此时用量为 0）
        second = await client.post(
            "/api/blob", content=b"w" * 50, headers={"X-Filename": "2.bin"}
        )
        assert second.status_code == 507
        assert second.json()["detail"]["code"] == "quota_exceeded"


@pytest.mark.asyncio
async def test_meta_endpoint(tmp_path):
    settings = make_settings(tmp_path)
    client, _ = await make_client(settings)
    async with client:
        up = await client.post(
            "/api/blob", content=b"meta test", headers={"X-Filename": "m.bin"}
        )
        blob_id = up.json()["blob_id"]
        meta = await client.get(f"/api/blob/{blob_id}/meta")
        assert meta.status_code == 200
        assert meta.json()["filename"] == "m.bin"

        listing = await client.get("/api/blobs")
        assert listing.json()["count"] == 1
