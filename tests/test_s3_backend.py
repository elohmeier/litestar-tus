from __future__ import annotations

from typing import Any

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
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
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
