from __future__ import annotations

from pathlib import Path

import anyio
import pytest
from litestar_tus.backends.file import FileStorageBackend
from litestar_tus.models import UploadInfo


async def _aiter(data: bytes):
    yield data


@pytest.fixture
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


class TestFileLocking:
    async def test_two_concurrent_writes_same_offset(
        self, backend: FileStorageBackend
    ) -> None:
        """Two concurrent write_chunk calls at offset 0: exactly one succeeds."""
        info = UploadInfo(id="lock-1", size=100)
        await backend.create_upload(info)

        results: list[int | ValueError] = []

        async def attempt_write() -> None:
            upload = await backend.get_upload("lock-1")
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

    async def test_many_concurrent_writers(self, backend: FileStorageBackend) -> None:
        """10 concurrent writers at offset 0: exactly one succeeds."""
        info = UploadInfo(id="lock-2", size=100)
        await backend.create_upload(info)

        results: list[int | ValueError] = []

        async def attempt_write() -> None:
            upload = await backend.get_upload("lock-2")
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

    async def test_sequential_writes_with_locking(
        self, backend: FileStorageBackend, upload_dir: Path
    ) -> None:
        """Sequential writes still work correctly with locking."""
        info = UploadInfo(id="lock-3", size=10)
        upload = await backend.create_upload(info)

        await upload.write_chunk(0, _aiter(b"hello"))
        await upload.write_chunk(5, _aiter(b"world"))

        loaded = await upload.get_info()
        assert loaded.offset == 10
        assert loaded.is_final is True

        data = (upload_dir / "lock-3").read_bytes()
        assert data == b"helloworld"

    async def test_lock_file_cleaned_on_terminate(
        self, backend: FileStorageBackend, upload_dir: Path
    ) -> None:
        """Lock file is cleaned up when upload is terminated."""
        info = UploadInfo(id="lock-4", size=100)
        upload = await backend.create_upload(info)

        # Write a chunk to create the lock file
        await upload.write_chunk(0, _aiter(b"data"))
        assert (upload_dir / "lock-4.lock").exists()

        await backend.terminate_upload("lock-4")

        assert not (upload_dir / "lock-4").exists()
        assert not (upload_dir / "lock-4.info").exists()
        assert not (upload_dir / "lock-4.lock").exists()


class TestFileConcatenation:
    async def test_concatenate_two_partials(
        self, backend: FileStorageBackend, upload_dir: Path
    ) -> None:
        # Create and write partial 1
        info1 = UploadInfo(id="cat-p1", size=5, concat_type="partial")
        upload1 = await backend.create_upload(info1)
        await upload1.write_chunk(0, _aiter(b"hello"))
        await upload1.finish()

        # Create and write partial 2
        info2 = UploadInfo(id="cat-p2", size=6, concat_type="partial")
        upload2 = await backend.create_upload(info2)
        await upload2.write_chunk(0, _aiter(b" world"))
        await upload2.finish()

        # Concatenate
        final_info = UploadInfo(
            id="cat-final",
            size=11,
            offset=11,
            is_final=True,
            concat_type="final",
            concat_parts=["cat-p1", "cat-p2"],
        )
        final_upload = await backend.concatenate_uploads(
            final_info, ["cat-p1", "cat-p2"]
        )

        # Verify data
        data = b""
        async for chunk in final_upload.get_reader():
            data += chunk
        assert data == b"hello world"

        # Verify info
        loaded = await final_upload.get_info()
        assert loaded.concat_type == "final"
        assert loaded.concat_parts == ["cat-p1", "cat-p2"]
        assert loaded.is_final is True
        assert loaded.offset == 11
        assert loaded.size == 11
