from __future__ import annotations

from pathlib import Path

import pytest

from litestar_tus.backends.file import FileStorageBackend
from litestar_tus.models import UploadInfo


async def _aiter(data: bytes):
    yield data


@pytest.fixture()
def backend(upload_dir: Path) -> FileStorageBackend:
    return FileStorageBackend(upload_dir)


class TestFileStorageBackend:
    async def test_create_upload(
        self, backend: FileStorageBackend, upload_dir: Path
    ) -> None:
        info = UploadInfo(id="test-1", size=100)
        upload = await backend.create_upload(info)

        assert (upload_dir / "test-1").exists()
        assert (upload_dir / "test-1.info").exists()

        loaded = await upload.get_info()
        assert loaded.id == "test-1"
        assert loaded.size == 100
        assert loaded.offset == 0

    async def test_write_chunk(
        self, backend: FileStorageBackend, upload_dir: Path
    ) -> None:
        info = UploadInfo(id="test-2", size=10)
        upload = await backend.create_upload(info)

        written = await upload.write_chunk(0, _aiter(b"hello"))
        assert written == 5

        loaded = await upload.get_info()
        assert loaded.offset == 5

    async def test_write_multiple_chunks(self, backend: FileStorageBackend) -> None:
        info = UploadInfo(id="test-3", size=10)
        upload = await backend.create_upload(info)

        await upload.write_chunk(0, _aiter(b"hello"))
        await upload.write_chunk(5, _aiter(b"world"))

        loaded = await upload.get_info()
        assert loaded.offset == 10
        assert loaded.is_final is True

    async def test_write_chunk_offset_mismatch(
        self, backend: FileStorageBackend
    ) -> None:
        info = UploadInfo(id="test-4", size=10)
        upload = await backend.create_upload(info)

        with pytest.raises(ValueError, match="Offset mismatch"):
            await upload.write_chunk(5, _aiter(b"hello"))

    async def test_finish(self, backend: FileStorageBackend) -> None:
        info = UploadInfo(id="test-5", size=5)
        upload = await backend.create_upload(info)

        await upload.write_chunk(0, _aiter(b"hello"))
        await upload.finish()

        loaded = await upload.get_info()
        assert loaded.is_final is True

    async def test_get_reader(self, backend: FileStorageBackend) -> None:
        info = UploadInfo(id="test-6", size=5)
        upload = await backend.create_upload(info)
        await upload.write_chunk(0, _aiter(b"hello"))

        data = b""
        async for chunk in upload.get_reader():
            data += chunk
        assert data == b"hello"

    async def test_terminate_upload(
        self, backend: FileStorageBackend, upload_dir: Path
    ) -> None:
        info = UploadInfo(id="test-7", size=10)
        await backend.create_upload(info)

        assert (upload_dir / "test-7").exists()
        assert (upload_dir / "test-7.info").exists()

        await backend.terminate_upload("test-7")

        assert not (upload_dir / "test-7").exists()
        assert not (upload_dir / "test-7.info").exists()

    async def test_get_upload_not_found(self, backend: FileStorageBackend) -> None:
        with pytest.raises(FileNotFoundError):
            await backend.get_upload("nonexistent")

    async def test_terminate_not_found(self, backend: FileStorageBackend) -> None:
        with pytest.raises(FileNotFoundError):
            await backend.terminate_upload("nonexistent")

    async def test_get_upload_existing(self, backend: FileStorageBackend) -> None:
        info = UploadInfo(id="test-8", size=100)
        await backend.create_upload(info)

        upload = await backend.get_upload("test-8")
        loaded = await upload.get_info()
        assert loaded.id == "test-8"
        assert loaded.size == 100
