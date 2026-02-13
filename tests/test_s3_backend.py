from __future__ import annotations

import contextlib
from typing import Any

import anyio
import pytest

pytest_plugins = ["pytest_databases.docker.minio"]


_5MIB = 5 * 1024 * 1024


async def _aiter(data: bytes):
    yield data


async def _aiter_chunks(data: bytes, chunk_size: int = 1024 * 1024):
    """Yield data in fixed-size chunks to simulate streaming."""
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


@pytest.fixture
def s3_client(minio_service: Any):
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=f"http://{minio_service.host}:{minio_service.port}",
        aws_access_key_id=minio_service.access_key,
        aws_secret_access_key=minio_service.secret_key,
        region_name="us-east-1",
    )


@pytest.fixture
def s3_bucket(s3_client: Any) -> str:
    bucket_name = "test-tus-uploads"
    with contextlib.suppress(s3_client.exceptions.BucketAlreadyOwnedByYou):
        s3_client.create_bucket(Bucket=bucket_name)
    return bucket_name


@pytest.fixture
def s3_backend(s3_client: Any, s3_bucket: str):
    from litestar_tus.backends.s3 import S3StorageBackend

    return S3StorageBackend(client=s3_client, bucket=s3_bucket, key_prefix="uploads/")


@pytest.fixture
def s3_backend_5mib(s3_client: Any, s3_bucket: str):
    from litestar_tus.backends.s3 import S3StorageBackend

    return S3StorageBackend(
        client=s3_client, bucket=s3_bucket, key_prefix="uploads/", part_size=_5MIB
    )


class TestS3StorageBackend:
    async def test_create_upload(self, s3_backend: Any) -> None:
        from litestar_tus.models import UploadInfo

        info = UploadInfo(id="s3-test-1", size=100)
        upload = await s3_backend.create_upload(info)

        loaded = await upload.get_info()
        assert loaded.id == "s3-test-1"
        assert loaded.size == 100
        assert loaded.offset == 0
        assert "multipart_upload_id" in loaded.storage_meta

    async def test_write_chunk(self, s3_backend: Any) -> None:
        from litestar_tus.models import UploadInfo

        info = UploadInfo(id="s3-test-2", size=100)
        upload = await s3_backend.create_upload(info)

        written = await upload.write_chunk(0, _aiter(b"x" * 50))
        assert written == 50

        loaded = await upload.get_info()
        assert loaded.offset == 50

    async def test_write_and_finish(self, s3_backend: Any) -> None:
        from litestar_tus.models import UploadInfo

        # S3 multipart requires at least 5MB parts except the last one.
        # For testing with MinIO, smaller parts work.
        data = b"hello world test data"
        info = UploadInfo(id="s3-test-3", size=len(data))
        upload = await s3_backend.create_upload(info)

        await upload.write_chunk(0, _aiter(data))
        await upload.finish()

        loaded = await upload.get_info()
        assert loaded.is_final is True
        assert loaded.offset == len(data)

    async def test_terminate_upload(self, s3_backend: Any) -> None:
        from litestar_tus.models import UploadInfo

        info = UploadInfo(id="s3-test-4", size=100)
        await s3_backend.create_upload(info)

        await s3_backend.terminate_upload("s3-test-4")

        with pytest.raises(FileNotFoundError):
            await s3_backend.get_upload("s3-test-4")

    async def test_get_upload_not_found(self, s3_backend: Any) -> None:
        with pytest.raises(FileNotFoundError):
            await s3_backend.get_upload("nonexistent")

    async def test_get_upload_not_found_client_error(self, s3_backend: Any) -> None:
        from botocore.stub import Stubber

        stubber = Stubber(s3_backend._client)
        stubber.add_client_error(
            "get_object",
            service_error_code="NoSuchKey",
            service_message="Not Found",
            http_status_code=404,
            expected_params={
                "Bucket": s3_backend._bucket,
                "Key": f"{s3_backend._key_prefix}nonexistent.info",
            },
        )

        with stubber, pytest.raises(FileNotFoundError):
            await s3_backend.get_upload("nonexistent")

    async def test_offset_mismatch(self, s3_backend: Any) -> None:
        from litestar_tus.models import UploadInfo

        info = UploadInfo(id="s3-test-5", size=100)
        upload = await s3_backend.create_upload(info)

        with pytest.raises(ValueError, match="Offset mismatch"):
            await upload.write_chunk(50, _aiter(b"data"))

    async def test_get_reader_after_finish(self, s3_backend: Any) -> None:
        from litestar_tus.models import UploadInfo

        data = b"read me back"
        info = UploadInfo(id="s3-test-6", size=len(data))
        upload = await s3_backend.create_upload(info)
        await upload.write_chunk(0, _aiter(data))
        await upload.finish()

        result = b""
        async for chunk in upload.get_reader():
            result += chunk
        assert result == data


class TestS3Locking:
    async def test_two_concurrent_writes_same_offset(self, s3_backend: Any) -> None:
        """Two concurrent write_chunk calls at offset 0: exactly one succeeds."""
        from litestar_tus.models import UploadInfo

        info = UploadInfo(id="s3-lock-1", size=100)
        await s3_backend.create_upload(info)

        results: list[int | ValueError] = []

        async def attempt_write() -> None:
            upload = await s3_backend.get_upload("s3-lock-1")
            try:
                written = await upload.write_chunk(0, _aiter(b"hello"))
                results.append(written)
            except ValueError as exc:
                results.append(exc)

        async with anyio.create_task_group() as tg:
            tg.start_soon(attempt_write)
            tg.start_soon(attempt_write)

        successes = [r for r in results if isinstance(r, int)]
        failures = [r for r in results if isinstance(r, ValueError)]
        assert len(successes) == 1
        assert successes[0] == 5
        assert len(failures) == 1

    async def test_many_concurrent_writers(self, s3_backend: Any) -> None:
        """10 concurrent writers at offset 0: exactly one succeeds."""
        from litestar_tus.models import UploadInfo

        info = UploadInfo(id="s3-lock-2", size=100)
        await s3_backend.create_upload(info)

        results: list[int | ValueError] = []

        async def attempt_write() -> None:
            upload = await s3_backend.get_upload("s3-lock-2")
            try:
                written = await upload.write_chunk(0, _aiter(b"data"))
                results.append(written)
            except ValueError as exc:
                results.append(exc)

        async with anyio.create_task_group() as tg:
            for _ in range(10):
                tg.start_soon(attempt_write)

        successes = [r for r in results if isinstance(r, int)]
        failures = [r for r in results if isinstance(r, ValueError)]
        assert len(successes) == 1
        assert len(failures) == 9

    async def test_sequential_writes_with_locking(self, s3_backend: Any) -> None:
        """Sequential writes still work correctly with locking."""
        from litestar_tus.models import UploadInfo

        data1 = b"hello"
        data2 = b"world"
        info = UploadInfo(id="s3-lock-3", size=len(data1) + len(data2))
        upload = await s3_backend.create_upload(info)

        await upload.write_chunk(0, _aiter(data1))
        await upload.write_chunk(len(data1), _aiter(data2))

        loaded = await upload.get_info()
        assert loaded.offset == len(data1) + len(data2)
        assert loaded.is_final is True


class TestS3RollingBuffer:
    async def test_large_stream_produces_multiple_parts(
        self, s3_backend_5mib: Any
    ) -> None:
        """A single write_chunk larger than part_size flushes multiple S3 parts."""
        from litestar_tus.models import UploadInfo

        total_size = _5MIB * 2 + _5MIB // 2  # 12.5 MiB → 2 full parts + 2.5 MiB pending
        data = b"A" * total_size
        info = UploadInfo(id="s3-rolling-1", size=total_size)
        upload = await s3_backend_5mib.create_upload(info)

        written = await upload.write_chunk(0, _aiter_chunks(data))
        assert written == total_size

        loaded = await upload.get_info()
        assert loaded.offset == total_size
        assert len(loaded.storage_meta["parts"]) == 2
        assert loaded.storage_meta["pending_size"] == _5MIB // 2

    async def test_cross_call_pending_accumulation(self, s3_backend_5mib: Any) -> None:
        """Multiple small writes accumulate in pending, flush on part boundary."""
        from litestar_tus.models import UploadInfo

        chunk_a = b"B" * (_5MIB // 2)  # 2.5 MiB
        chunk_b = b"C" * (_5MIB // 2)  # 2.5 MiB
        chunk_c = b"D" * (_5MIB // 2)  # 2.5 MiB
        total = len(chunk_a) + len(chunk_b) + len(chunk_c)
        info = UploadInfo(id="s3-rolling-2", size=total)
        upload = await s3_backend_5mib.create_upload(info)

        # First write: 2.5 MiB → all goes to pending, no parts
        await upload.write_chunk(0, _aiter(chunk_a))
        loaded = await upload.get_info()
        assert len(loaded.storage_meta["parts"]) == 0
        assert loaded.storage_meta["pending_size"] == len(chunk_a)

        # Second write: 2.5 MiB → pending (2.5) + new (2.5) = 5 MiB → flush 1 part, 0 pending
        await upload.write_chunk(len(chunk_a), _aiter(chunk_b))
        loaded = await upload.get_info()
        assert len(loaded.storage_meta["parts"]) == 1
        assert loaded.storage_meta["pending_size"] == 0

        # Third write: 2.5 MiB → all goes to pending
        await upload.write_chunk(len(chunk_a) + len(chunk_b), _aiter(chunk_c))
        loaded = await upload.get_info()
        assert len(loaded.storage_meta["parts"]) == 1
        assert loaded.storage_meta["pending_size"] == len(chunk_c)

    async def test_finish_flushes_pending(self, s3_backend_5mib: Any) -> None:
        """Small write + finish → pending flushed as final part, data readable."""
        from litestar_tus.models import UploadInfo

        data = b"E" * 1000
        info = UploadInfo(id="s3-rolling-3", size=len(data))
        upload = await s3_backend_5mib.create_upload(info)

        await upload.write_chunk(0, _aiter(data))
        loaded = await upload.get_info()
        assert len(loaded.storage_meta["parts"]) == 0
        assert loaded.storage_meta["pending_size"] == len(data)

        await upload.finish()
        loaded = await upload.get_info()
        assert loaded.is_final is True
        assert len(loaded.storage_meta["parts"]) == 1

        result = b""
        async for chunk in upload.get_reader():
            result += chunk
        assert result == data

    async def test_part_size_validation(self, s3_client: Any, s3_bucket: str) -> None:
        """part_size < 5 MiB raises ValueError."""
        from litestar_tus.backends.s3 import S3StorageBackend

        with pytest.raises(ValueError, match="part_size must be >= "):
            S3StorageBackend(
                client=s3_client,
                bucket=s3_bucket,
                key_prefix="uploads/",
                part_size=1024 * 1024,  # 1 MiB — too small
            )

    async def test_terminate_cleans_pending(self, s3_backend_5mib: Any) -> None:
        """Terminate after a small write cleans up the .pending object."""
        from litestar_tus.models import UploadInfo

        data = b"F" * 1000
        info = UploadInfo(id="s3-rolling-5", size=10000)
        upload = await s3_backend_5mib.create_upload(info)

        await upload.write_chunk(0, _aiter(data))
        loaded = await upload.get_info()
        assert loaded.storage_meta["pending_size"] == len(data)

        await s3_backend_5mib.terminate_upload("s3-rolling-5")

        with pytest.raises(FileNotFoundError):
            await s3_backend_5mib.get_upload("s3-rolling-5")


class TestS3OptimisticLocking:
    async def test_stale_etag_raises_on_save_info(
        self, s3_backend: Any, s3_client: Any, s3_bucket: str
    ) -> None:
        """Overwriting .info externally changes its ETag; next _save_info fails."""
        from litestar_tus.models import UploadInfo

        info = UploadInfo(id="s3-etag-1", size=100)
        upload = await s3_backend.create_upload(info)

        # Externally overwrite the .info object to change its ETag
        s3_client.put_object(
            Bucket=s3_bucket,
            Key="uploads/s3-etag-1.info",
            Body=b'{"id":"s3-etag-1","size":100,"offset":0}',
        )

        # upload still holds the old ETag — _save_info should fail
        with pytest.raises(ValueError, match="Concurrent modification detected"):
            await upload._save_info()

    async def test_etag_updates_through_writes(self, s3_backend: Any) -> None:
        """ETag changes after each successful _save_info call."""
        from litestar_tus.models import UploadInfo

        info = UploadInfo(id="s3-etag-2", size=100)
        upload = await s3_backend.create_upload(info)
        etag_after_create = upload._info_etag
        assert etag_after_create is not None

        await upload.write_chunk(0, _aiter(b"hello"))
        etag_after_write1 = upload._info_etag
        assert etag_after_write1 is not None
        assert etag_after_write1 != etag_after_create

        await upload.write_chunk(5, _aiter(b"world"))
        etag_after_write2 = upload._info_etag
        assert etag_after_write2 is not None
        assert etag_after_write2 != etag_after_write1

    async def test_get_info_refreshes_etag(self, s3_backend: Any) -> None:
        """get_info() populates _info_etag from the S3 response."""
        from litestar_tus.models import UploadInfo

        info = UploadInfo(id="s3-etag-3", size=100)
        upload = await s3_backend.create_upload(info)
        original_etag = upload._info_etag

        loaded = await upload.get_info()
        assert upload._info_etag is not None
        assert upload._info_etag == original_etag
        assert loaded.id == "s3-etag-3"

    async def test_double_create_prevented(self, s3_backend: Any) -> None:
        """Re-creating with IfNoneMatch='*' should fail when .info already exists."""
        from litestar_tus.models import UploadInfo

        info = UploadInfo(id="s3-etag-4", size=100)
        upload = await s3_backend.create_upload(info)

        # Reset etag to None so _save_info uses IfNoneMatch='*'
        upload._info_etag = None

        with pytest.raises(ValueError, match="Concurrent modification detected"):
            await upload._save_info()
