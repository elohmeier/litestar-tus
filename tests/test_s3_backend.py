from __future__ import annotations

from typing import Any

import anyio
import pytest

pytest_plugins = ["pytest_databases.docker.minio"]


async def _aiter(data: bytes):
    yield data


@pytest.fixture()
def s3_client(minio_service: Any):
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=f"http://{minio_service.host}:{minio_service.port}",
        aws_access_key_id=minio_service.access_key,
        aws_secret_access_key=minio_service.secret_key,
        region_name="us-east-1",
    )


@pytest.fixture()
def s3_bucket(s3_client: Any) -> str:
    bucket_name = "test-tus-uploads"
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass
    return bucket_name


@pytest.fixture()
def s3_backend(s3_client: Any, s3_bucket: str):
    from litestar_tus.backends.s3 import S3StorageBackend

    return S3StorageBackend(client=s3_client, bucket=s3_bucket, key_prefix="uploads/")


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

        with stubber:
            with pytest.raises(FileNotFoundError):
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
