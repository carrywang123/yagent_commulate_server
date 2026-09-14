"""对运行中的 Blob Store 做端到端联调检查（用真实 HTTP，不是 ASGI 内存传输）。

用法：
    先起服务：run_blob.bat
    再执行  ：.venv/Scripts/python scripts/blob_e2e_check.py
    可指定  ：--base http://10.1.42.2:8771 --token xxx

单元测试走 ASGITransport，覆盖不到"真的经过 socket"这一层：
Content-Length 处理、chunked、中文头编码、大文件分块，
这些只有打真实端口才能验证。
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "sdk" / "python"))

from yagent_blob_client import BlobClient, BlobError, sha256_file  # noqa: E402

PASSED: list[str] = []
FAILED: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        PASSED.append(name)
        print(f"  [OK]   {name}")
    else:
        FAILED.append(f"{name}: {detail}")
        print(f"  [FAIL] {name}  {detail}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default=os.getenv("BLOB_BASE_URL", "http://127.0.0.1:8771"))
    parser.add_argument("--token", default=os.getenv("BLOB_AUTH_TOKEN", ""))
    parser.add_argument("--big-mb", type=int, default=8, help="大文件测试的大小（MB）")
    args = parser.parse_args()

    client = BlobClient(args.base, args.token)
    print(f"目标服务: {args.base}")

    # 1. 健康检查
    try:
        health = client.health()
    except BlobError as exc:
        print(f"\n[中止] 服务不可用: {exc}")
        print("请先启动服务：run_blob.bat")
        return 2
    check("健康检查", health.get("status") == "ok", str(health))
    check("服务标识正确", health.get("service") == "yagent-blob-store", str(health))

    workdir = Path(tempfile.mkdtemp(prefix="blob-e2e-"))
    try:
        # 2. 小文件往返
        # 内容里掺入随机串：否则第二次跑这个脚本时会命中上一轮留下的 blob，
        # "首次上传未命中去重" 必然失败 —— 测试必须自带隔离，不能依赖服务端是干净的。
        small = workdir / "small.txt"
        nonce = os.urandom(8).hex()
        small.write_text(f"跨端文件中转测试 {nonce}\n" * 50, encoding="utf-8")
        want_hash, want_size = sha256_file(small)

        info = client.upload(small, uploader="e2e-check")
        check("上传返回 blob_id == sha256", info.blob_id == want_hash, info.blob_id)
        check("上传返回 size 正确", info.size == want_size, str(info.size))
        check("首次上传未命中去重", info.deduplicated is False)
        check("下载 URL 含 blob_id", info.blob_id in info.url, info.url)

        back = client.download(info.blob_id, workdir / "small_back.txt")
        check("下载内容一致", back.read_bytes() == small.read_bytes())

        # 3. 秒传
        again = client.upload(small, uploader="e2e-check")
        check("重复上传命中秒传", again.deduplicated is True)
        check("秒传 blob_id 不变", again.blob_id == info.blob_id)

        # 4. 中文文件名
        cn_name = "跨端 素材包(v2).zip"
        cn_file = workdir / cn_name
        cn_file.write_bytes(b"PK\x03\x04fake zip payload " + nonce.encode())
        cn_info = client.upload(cn_file)
        check("中文文件名往返不丢", cn_info.filename == cn_name, cn_info.filename)
        cn_meta = client.meta(cn_info.blob_id)
        check("元数据里中文名正确", cn_meta.get("filename") == cn_name, str(cn_meta.get("filename")))

        # 5. 用完整 URL 下载（模拟对端只拿到消息里的 url）
        via_url = client.download(cn_info.url, workdir / "via_url.zip")
        check("用完整 URL 能下载", via_url.read_bytes() == cn_file.read_bytes())

        # 6. 大文件 + 进度
        big = workdir / "big.bin"
        block = os.urandom(1024 * 1024)
        with big.open("wb") as handle:
            for _ in range(max(1, args.big_mb)):
                handle.write(block)
        big_hash, big_size = sha256_file(big)

        seen: list[int] = []
        started = time.time()
        big_info = client.upload(
            big, uploader="e2e-check", on_progress=lambda sent, total: seen.append(sent)
        )
        elapsed = max(0.001, time.time() - started)
        check("大文件上传成功", big_info.blob_id == big_hash, big_info.blob_id)
        check("上传进度回调被触发", len(seen) > 1, f"回调 {len(seen)} 次")
        print(f"         {big_size / 1048576:.0f}MB 上传耗时 {elapsed:.2f}s "
              f"({big_size / 1048576 / elapsed:.1f} MB/s)")

        down_seen: list[int] = []
        # 刻意用小 chunk 建一个客户端：默认 chunk 是 1MB，
        # 用它下 1MB 文件只会回调一次，测不出"分块推进"这件事。
        chunked = BlobClient(args.base, args.token, chunk=64 * 1024)
        big_back = chunked.download(
            big_info.blob_id,
            workdir / "big_back.bin",
            on_progress=lambda got, total: down_seen.append(got),
        )
        check("大文件下载校验通过", sha256_file(big_back)[0] == big_hash)
        check("下载进度回调被触发", len(down_seen) > 1, f"回调 {len(down_seen)} 次")
        check(
            "下载进度最终等于文件大小",
            bool(down_seen) and down_seen[-1] == big_size,
            f"最终 {down_seen[-1] if down_seen else 0} vs {big_size}",
        )

        # 7. exists 探测
        check("exists 命中", client.exists(big_info.blob_id) is True)
        absent = hashlib.sha256(os.urandom(32)).hexdigest()
        check("exists 未命中返回 False", client.exists(absent) is False)

        # 8. 下载不存在的 blob → not_found
        try:
            client.download(absent, workdir / "nope.bin")
            check("下载不存在的 blob 应报错", False, "居然成功了")
        except BlobError as exc:
            check("下载不存在的 blob 报 not_found", exc.code == "not_found", exc.code)
        check("失败的下载不留 .part", not (workdir / "nope.bin.part").exists())

        # 9. 校验和不符必须被拒（手工构造错误的 X-Sha256）
        # Content-Length 必须与 data 实际长度一致，否则服务端会一直等剩下的字节，
        # 表现为整个脚本挂住 —— 这个坑踩过一次，别再手写常量。
        tampered = b"tampered content"
        try:
            client._request(  # 内部方法，仅测试用
                "POST",
                "/api/blob",
                data=tampered,
                headers={
                    "X-Filename": "t.bin",
                    "X-Sha256": hashlib.sha256(b"different").hexdigest(),
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(len(tampered)),
                },
                timeout=30.0,
            )
            check("校验和不符应被拒收", False, "居然收下了")
        except BlobError as exc:
            check("校验和不符被拒收", exc.code == "checksum_mismatch", exc.code)

        # 10. 删除
        check("删除成功", client.delete(cn_info.blob_id) is True)
        check("删除后 exists 为 False", client.exists(cn_info.blob_id) is False)
        check("重复删除返回 False", client.delete(cn_info.blob_id) is False)

        # 11. 统计
        stats = client.stats()
        check("统计接口可用", "total_files" in stats, str(stats))

        # 清理本次产生的测试数据，不给服务留垃圾
        for bid in {info.blob_id, big_info.blob_id}:
            try:
                client.delete(bid)
            except BlobError:
                pass

    finally:
        for item in workdir.rglob("*"):
            if item.is_file():
                item.unlink(missing_ok=True)
        workdir.rmdir()

    print("\n" + "=" * 56)
    print(f"通过 {len(PASSED)} / {len(PASSED) + len(FAILED)}")
    if FAILED:
        print("失败项：")
        for item in FAILED:
            print("  -", item)
        return 1
    print("全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
