from __future__ import annotations

import contextlib
import fcntl
import json
import os
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

    @staticmethod
    def _write_info_atomic(info_path: Path, content: bytes) -> None:
        tmp_path = info_path.with_suffix(f"{info_path.suffix}.tmp")
        tmp_path.write_bytes(content)
        os.replace(tmp_path, info_path)

    def _read_info_locked(self) -> UploadInfo:
        lock_fd = open(self._lock_path, "a+")  # noqa: SIM115
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_SH)
            return UploadInfo.from_dict(json.loads(self._info_path.read_bytes()))
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()

    async def _save_info(self) -> None:
        content = json.dumps(self._info.to_dict()).encode("utf-8")
        await anyio.to_thread.run_sync(
            lambda: self._write_info_atomic(self._info_path, content)
        )

    def _write_chunk_locked(self, offset: int, data: bytes) -> int:
        lock_fd = open(self._lock_path, "a+")  # noqa: SIM115
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

            self._write_info_atomic(
                self._info_path, json.dumps(info.to_dict()).encode("utf-8")
            )
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

        lock_fd = open(self._lock_path, "a+")  # noqa: SIM115
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)

            # Re-read authoritative state from disk
            info = UploadInfo.from_dict(json.loads(self._info_path.read_bytes()))

            if offset != info.offset:
                msg = f"Offset mismatch: expected {info.offset}, got {offset}"
                raise ValueError(msg)

            bytes_written = 0
            stream_exc: Exception | None = None
            with open(self._data_path, "ab") as f:
                try:
                    while True:
                        chunk = from_thread.run(_next_chunk)
                        if chunk is None:
                            break
                        f.write(chunk)
                        bytes_written += len(chunk)
                except Exception as exc:
                    status_code = getattr(exc, "status_code", None)
                    if status_code is not None and status_code < 500:
                        f.flush()
                        f.truncate(info.offset)
                        raise
                    stream_exc = exc
                    f.flush()

            info.offset += bytes_written
            if info.size is not None and info.offset >= info.size:
                info.is_final = True

            self._write_info_atomic(
                self._info_path, json.dumps(info.to_dict()).encode("utf-8")
            )
            self._info = info

            if stream_exc:
                raise stream_exc

            return bytes_written
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()

    async def write_chunk(self, offset: int, src: AsyncIterator[bytes]) -> int:
        return await anyio.to_thread.run_sync(
            lambda: self._write_chunk_stream_locked(offset, src)
        )

    async def get_info(self) -> UploadInfo:
        self._info = await anyio.to_thread.run_sync(self._read_info_locked)
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
        lock_path = self.upload_dir / f"{info.id}.lock"

        await anyio.Path(data_path).write_bytes(b"")
        await anyio.Path(lock_path).write_bytes(b"")
        content = json.dumps(info.to_dict()).encode("utf-8")
        await anyio.to_thread.run_sync(
            lambda: FileUpload._write_info_atomic(info_path, content)
        )

        return FileUpload(info, data_path, info_path)

    async def get_upload(self, upload_id: str) -> FileUpload:
        data_path = self.upload_dir / upload_id
        info_path = self.upload_dir / f"{upload_id}.info"
        lock_path = self.upload_dir / f"{upload_id}.lock"

        if not await anyio.Path(info_path).exists():
            msg = f"Upload {upload_id} not found"
            raise FileNotFoundError(msg)

        def _read_info_locked() -> UploadInfo:
            lock_fd = open(lock_path, "a+")  # noqa: SIM115
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_SH)
                return UploadInfo.from_dict(json.loads(info_path.read_bytes()))
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                lock_fd.close()

        info = await anyio.to_thread.run_sync(_read_info_locked)
        return FileUpload(info, data_path, info_path)

    def _terminate_locked(self, upload_id: str) -> None:
        data_path = self.upload_dir / upload_id
        info_path = self.upload_dir / f"{upload_id}.info"
        lock_path = self.upload_dir / f"{upload_id}.lock"

        if not info_path.exists():
            msg = f"Upload {upload_id} not found"
            raise FileNotFoundError(msg)

        lock_fd = open(lock_path, "a+")  # noqa: SIM115
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            for p in (data_path, info_path, lock_path):
                with contextlib.suppress(FileNotFoundError):
                    p.unlink()
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()

    async def concatenate_uploads(
        self, final_info: UploadInfo, partial_ids: list[str]
    ) -> FileUpload:
        await self._ensure_dir()
        data_path = self.upload_dir / final_info.id
        info_path = self.upload_dir / f"{final_info.id}.info"
        lock_path = self.upload_dir / f"{final_info.id}.lock"

        partial_data_paths = [self.upload_dir / pid for pid in partial_ids]

        def _concatenate() -> None:
            # Create lock file
            lock_path.write_bytes(b"")
            # Write concatenated data
            with open(data_path, "wb") as dst:
                for p_path in partial_data_paths:
                    with open(p_path, "rb") as src:
                        while True:
                            chunk = src.read(1048576)  # 1 MiB
                            if not chunk:
                                break
                            dst.write(chunk)
            # Write info atomically
            content = json.dumps(final_info.to_dict()).encode("utf-8")
            FileUpload._write_info_atomic(info_path, content)

        await anyio.to_thread.run_sync(_concatenate)
        return FileUpload(final_info, data_path, info_path)

    async def terminate_upload(self, upload_id: str) -> None:
        await anyio.to_thread.run_sync(lambda: self._terminate_locked(upload_id))
