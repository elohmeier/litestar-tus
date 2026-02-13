from __future__ import annotations

import contextlib
import fcntl
import json
from collections.abc import AsyncIterator
from pathlib import Path

import anyio
import anyio.to_thread
from anyio import from_thread

from litestar_tus.models import UploadInfo


class FileUpload:
    def __init__(self, info: UploadInfo, data_path: Path, info_path: Path) -> None:
        self._info = info
        self._data_path = data_path
        self._info_path = info_path

    @property
    def _lock_path(self) -> Path:
        return self._data_path.with_suffix(".lock")

    async def _save_info(self) -> None:
        content = json.dumps(self._info.to_dict()).encode("utf-8")
        await anyio.Path(self._info_path).write_bytes(content)

    def _write_chunk_locked(self, offset: int, data: bytes) -> int:
        lock_fd = open(self._lock_path, "w")  # noqa: SIM115
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)

            # Re-read authoritative state from disk
            info = UploadInfo.from_dict(json.loads(self._info_path.read_bytes()))

            if offset != info.offset:
                msg = f"Offset mismatch: expected {info.offset}, got {offset}"
                raise ValueError(msg)

            with open(self._data_path, "ab") as f:
                f.write(data)

            bytes_written = len(data)
            info.offset += bytes_written
            if info.size is not None and info.offset >= info.size:
                info.is_final = True

            self._info_path.write_bytes(json.dumps(info.to_dict()).encode("utf-8"))
            self._info = info
            return bytes_written
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()

    def _write_chunk_stream_locked(self, offset: int, src: AsyncIterator[bytes]) -> int:
        async def _next_chunk() -> bytes | None:
            try:
                return await src.__anext__()
            except StopAsyncIteration:
                return None

        lock_fd = open(self._lock_path, "w")  # noqa: SIM115
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)

            # Re-read authoritative state from disk
            info = UploadInfo.from_dict(json.loads(self._info_path.read_bytes()))

            if offset != info.offset:
                msg = f"Offset mismatch: expected {info.offset}, got {offset}"
                raise ValueError(msg)

            bytes_written = 0
            with open(self._data_path, "ab") as f:
                try:
                    while True:
                        chunk = from_thread.run(_next_chunk)
                        if chunk is None:
                            break
                        f.write(chunk)
                        bytes_written += len(chunk)
                except Exception:
                    f.flush()
                    f.truncate(info.offset)
                    raise

            info.offset += bytes_written
            if info.size is not None and info.offset >= info.size:
                info.is_final = True

            self._info_path.write_bytes(json.dumps(info.to_dict()).encode("utf-8"))
            self._info = info
            return bytes_written
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()

    async def write_chunk(self, offset: int, src: AsyncIterator[bytes]) -> int:
        return await anyio.to_thread.run_sync(
            lambda: self._write_chunk_stream_locked(offset, src)
        )

    async def get_info(self) -> UploadInfo:
        content = await anyio.Path(self._info_path).read_bytes()
        self._info = UploadInfo.from_dict(json.loads(content))
        return self._info

    async def finish(self) -> None:
        self._info.is_final = True
        await self._save_info()

    async def get_reader(self) -> AsyncIterator[bytes]:
        async with await anyio.open_file(self._data_path, "rb") as f:
            while True:
                chunk = await f.read(65536)
                if not chunk:
                    break
                yield chunk


class FileStorageBackend:
    def __init__(self, upload_dir: Path | str) -> None:
        self.upload_dir = Path(upload_dir)

    async def _ensure_dir(self) -> None:
        await anyio.Path(self.upload_dir).mkdir(parents=True, exist_ok=True)

    async def create_upload(self, info: UploadInfo) -> FileUpload:
        await self._ensure_dir()
        data_path = self.upload_dir / info.id
        info_path = self.upload_dir / f"{info.id}.info"

        await anyio.Path(data_path).write_bytes(b"")
        content = json.dumps(info.to_dict()).encode("utf-8")
        await anyio.Path(info_path).write_bytes(content)

        return FileUpload(info, data_path, info_path)

    async def get_upload(self, upload_id: str) -> FileUpload:
        data_path = self.upload_dir / upload_id
        info_path = self.upload_dir / f"{upload_id}.info"

        if not await anyio.Path(info_path).exists():
            msg = f"Upload {upload_id} not found"
            raise FileNotFoundError(msg)

        content = await anyio.Path(info_path).read_bytes()
        info = UploadInfo.from_dict(json.loads(content))
        return FileUpload(info, data_path, info_path)

    def _terminate_locked(self, upload_id: str) -> None:
        data_path = self.upload_dir / upload_id
        info_path = self.upload_dir / f"{upload_id}.info"
        lock_path = self.upload_dir / f"{upload_id}.lock"

        if not info_path.exists():
            msg = f"Upload {upload_id} not found"
            raise FileNotFoundError(msg)

        lock_fd = open(lock_path, "w")  # noqa: SIM115
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            for p in (data_path, info_path, lock_path):
                with contextlib.suppress(FileNotFoundError):
                    p.unlink()
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()

    async def terminate_upload(self, upload_id: str) -> None:
        await anyio.to_thread.run_sync(lambda: self._terminate_locked(upload_id))
