from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path

import anyio
import anyio.to_thread

from litestar_tus.models import UploadInfo


class FileUpload:
    def __init__(self, info: UploadInfo, data_path: Path, info_path: Path) -> None:
        self._info = info
        self._data_path = data_path
        self._info_path = info_path

    async def _save_info(self) -> None:
        content = json.dumps(self._info.to_dict()).encode("utf-8")
        await anyio.Path(self._info_path).write_bytes(content)

    async def write_chunk(self, offset: int, src: AsyncIterator[bytes]) -> int:
        if offset != self._info.offset:
            msg = f"Offset mismatch: expected {self._info.offset}, got {offset}"
            raise ValueError(msg)

        bytes_written = 0
        async with await anyio.open_file(self._data_path, "ab") as f:
            async for chunk in src:
                await f.write(chunk)
                bytes_written += len(chunk)

        self._info.offset += bytes_written
        if self._info.size is not None and self._info.offset >= self._info.size:
            self._info.is_final = True
        await self._save_info()
        return bytes_written

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
            raise FileNotFoundError(f"Upload {upload_id} not found")

        content = await anyio.Path(info_path).read_bytes()
        info = UploadInfo.from_dict(json.loads(content))
        return FileUpload(info, data_path, info_path)

    async def terminate_upload(self, upload_id: str) -> None:
        data_path = self.upload_dir / upload_id
        info_path = self.upload_dir / f"{upload_id}.info"

        if not await anyio.Path(info_path).exists():
            raise FileNotFoundError(f"Upload {upload_id} not found")

        for p in (data_path, info_path):
            try:
                await anyio.Path(p).unlink()
            except FileNotFoundError:
                pass
