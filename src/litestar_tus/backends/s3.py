from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncIterator
from typing import Any

import anyio
import anyio.to_thread
from botocore.exceptions import ClientError

from litestar_tus.models import UploadInfo

# boto3 S3 client type — use Any to avoid requiring mypy_boto3_s3 stubs
S3Client = Any


def _normalize_etag(etag: str | None) -> str | None:
    """Normalize S3 ETag values for conditional headers.

    Some S3-compatible providers return ETag values quoted, while conditional
    request headers such as ``If-Match`` may require the raw (unquoted) token.
    """
    if etag is None:
        return None
    normalized = etag.strip()
    if normalized.startswith("W/"):
        normalized = normalized[2:].strip()
    return normalized.strip('"')


class S3Upload:
    def __init__(
        self,
        info: UploadInfo,
        *,
        client: S3Client,
        bucket: str,
        key_prefix: str,
        lock: anyio.Lock,
        part_size: int,
        info_etag: str | None = None,
    ) -> None:
        self._info = info
        self._client = client
        self._bucket = bucket
        self._key_prefix = key_prefix
        self._lock = lock
        self._part_size = part_size
        self._info_etag = _normalize_etag(info_etag)

    @property
    def _data_key(self) -> str:
        return f"{self._key_prefix}{self._info.id}"

    @property
    def _info_key(self) -> str:
        return f"{self._key_prefix}{self._info.id}.info"

    @property
    def _pending_key(self) -> str:
        return f"{self._key_prefix}{self._info.id}.pending"

    async def _save_info(self) -> None:
        body = json.dumps(self._info.to_dict()).encode("utf-8")

        def _put() -> str:
            kwargs: dict[str, Any] = {
                "Bucket": self._bucket,
                "Key": self._info_key,
                "Body": body,
            }
            if self._info_etag is not None:
                kwargs["IfMatch"] = self._info_etag
            else:
                kwargs["IfNoneMatch"] = "*"
            try:
                resp = self._client.put_object(**kwargs)
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code in {"PreconditionFailed", "412"}:
                    msg = f"Concurrent modification detected on upload {self._info.id}"
                    raise ValueError(msg) from exc
                raise
            return _normalize_etag(resp["ETag"]) or ""

        self._info_etag = await anyio.to_thread.run_sync(_put)

    async def _upload_part(
        self, upload_id: str, part_number: int, body: bytes
    ) -> dict[str, Any]:
        def _do_upload() -> dict[str, Any]:
            resp = self._client.upload_part(
                Bucket=self._bucket,
                Key=self._data_key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=body,
            )
            return {"ETag": resp["ETag"], "PartNumber": part_number}

        return await anyio.to_thread.run_sync(_do_upload)

    async def _get_pending(self) -> bytes:
        def _get() -> bytes:
            try:
                resp = self._client.get_object(
                    Bucket=self._bucket, Key=self._pending_key
                )
                return resp["Body"].read()
            except self._client.exceptions.NoSuchKey:
                return b""
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                if code in {"NoSuchKey", "404", "NotFound"}:
                    return b""
                raise

        return await anyio.to_thread.run_sync(_get)

    async def _put_pending(self, data: bytes) -> None:
        await anyio.to_thread.run_sync(
            lambda: self._client.put_object(
                Bucket=self._bucket, Key=self._pending_key, Body=data
            )
        )

    async def _delete_pending(self) -> None:
        def _del() -> None:
            with contextlib.suppress(Exception):
                self._client.delete_object(Bucket=self._bucket, Key=self._pending_key)

        await anyio.to_thread.run_sync(_del)

    async def write_chunk(self, offset: int, src: AsyncIterator[bytes]) -> int:
        async with self._lock:
            info = await self.get_info()

            if offset != info.offset:
                msg = f"Offset mismatch: expected {info.offset}, got {offset}"
                raise ValueError(msg)

            upload_id = str(info.storage_meta.get("multipart_upload_id", ""))
            parts: list[dict[str, Any]] = list(info.storage_meta.get("parts", []))  # type: ignore[arg-type]
            pending_size: int = info.storage_meta.get("pending_size", 0)  # type: ignore[assignment]

            # Load pending buffer from previous call if any
            buf = bytearray()
            if pending_size > 0:
                buf.extend(await self._get_pending())

            total_written = 0
            async for chunk in src:
                buf.extend(chunk)
                total_written += len(chunk)

                # Flush full parts as they accumulate
                while len(buf) >= self._part_size:
                    part_number = len(parts) + 1
                    part_data = bytes(buf[: self._part_size])
                    part = await self._upload_part(upload_id, part_number, part_data)
                    parts.append(part)
                    del buf[: self._part_size]

            if total_written == 0 and pending_size == 0:
                return 0

            # Store leftover as pending or delete if empty
            if buf:
                await self._put_pending(bytes(buf))
            elif pending_size > 0:
                await self._delete_pending()

            info.offset += total_written
            info.storage_meta["parts"] = parts
            info.storage_meta["pending_size"] = len(buf)
            if info.size is not None and info.offset >= info.size:
                info.is_final = True
            self._info = info
            await self._save_info()
            return total_written

    async def get_info(self) -> UploadInfo:
        def _get() -> tuple[bytes, str]:
            resp = self._client.get_object(Bucket=self._bucket, Key=self._info_key)
            return resp["Body"].read(), resp["ETag"]

        content, etag = await anyio.to_thread.run_sync(_get)
        self._info = UploadInfo.from_dict(json.loads(content))
        self._info_etag = _normalize_etag(etag)
        return self._info

    async def finish(self) -> None:
        async with self._lock:
            info = await self.get_info()
            upload_id = str(info.storage_meta.get("multipart_upload_id", ""))
            parts: list[dict[str, Any]] = list(info.storage_meta.get("parts", []))  # type: ignore[arg-type]
            pending_size: int = info.storage_meta.get("pending_size", 0)  # type: ignore[assignment]

            # Flush any pending data as the final part
            if pending_size > 0:
                pending_data = await self._get_pending()
                if pending_data:
                    part_number = len(parts) + 1
                    part = await self._upload_part(upload_id, part_number, pending_data)
                    parts.append(part)
                await self._delete_pending()

            def _complete() -> None:
                self._client.complete_multipart_upload(
                    Bucket=self._bucket,
                    Key=self._data_key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )

            await anyio.to_thread.run_sync(_complete)
            info.is_final = True
            info.storage_meta["parts"] = parts
            info.storage_meta["pending_size"] = 0
            self._info = info
            await self._save_info()

    async def get_reader(self) -> AsyncIterator[bytes]:
        def _get() -> bytes:
            resp = self._client.get_object(Bucket=self._bucket, Key=self._data_key)
            return resp["Body"].read()

        data = await anyio.to_thread.run_sync(_get)
        yield data


_MIN_PART_SIZE = 5 * 1024 * 1024  # 5 MiB — AWS S3 minimum for multipart parts
_DEFAULT_PART_SIZE = 10 * 1024 * 1024  # 10 MiB


class S3StorageBackend:
    """S3-based storage backend using multipart upload.

    Uses S3 conditional writes (``IfMatch`` ETag) for optimistic concurrency
    control. Process-local ``anyio.Lock`` reduces unnecessary S3 round-trips
    within a single worker.
    """

    def __init__(
        self,
        client: S3Client,
        bucket: str,
        key_prefix: str = "tus-uploads/",
        part_size: int = _DEFAULT_PART_SIZE,
    ) -> None:
        if part_size < _MIN_PART_SIZE:
            msg = f"part_size must be >= {_MIN_PART_SIZE} (5 MiB), got {part_size}"
            raise ValueError(msg)
        self._client = client
        self._bucket = bucket
        self._key_prefix = key_prefix
        self._part_size = part_size
        self._locks: dict[str, anyio.Lock] = {}

    def _get_lock(self, upload_id: str) -> anyio.Lock:
        if upload_id not in self._locks:
            self._locks[upload_id] = anyio.Lock()
        return self._locks[upload_id]

    async def create_upload(self, info: UploadInfo) -> S3Upload:
        data_key = f"{self._key_prefix}{info.id}"

        def _create_multipart() -> str:
            resp = self._client.create_multipart_upload(
                Bucket=self._bucket, Key=data_key
            )
            return resp["UploadId"]

        upload_id = await anyio.to_thread.run_sync(_create_multipart)
        info.storage_meta["multipart_upload_id"] = upload_id
        info.storage_meta["parts"] = []

        upload = S3Upload(
            info,
            client=self._client,
            bucket=self._bucket,
            key_prefix=self._key_prefix,
            lock=self._get_lock(info.id),
            part_size=self._part_size,
        )
        await upload._save_info()
        return upload

    async def get_upload(self, upload_id: str) -> S3Upload:
        info_key = f"{self._key_prefix}{upload_id}.info"

        def _get() -> tuple[bytes, str]:
            try:
                resp = self._client.get_object(Bucket=self._bucket, Key=info_key)
                return resp["Body"].read(), resp["ETag"]
            except self._client.exceptions.NoSuchKey:
                msg = f"Upload {upload_id} not found"
                raise FileNotFoundError(msg)
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                if code in {"NoSuchKey", "404", "NotFound"}:
                    msg = f"Upload {upload_id} not found"
                    raise FileNotFoundError(msg)
                raise

        content, etag = await anyio.to_thread.run_sync(_get)
        info = UploadInfo.from_dict(json.loads(content))
        return S3Upload(
            info,
            client=self._client,
            bucket=self._bucket,
            key_prefix=self._key_prefix,
            lock=self._get_lock(upload_id),
            part_size=self._part_size,
            info_etag=_normalize_etag(etag),
        )

    async def terminate_upload(self, upload_id: str) -> None:
        lock = self._get_lock(upload_id)
        async with lock:
            info_key = f"{self._key_prefix}{upload_id}.info"
            data_key = f"{self._key_prefix}{upload_id}"

            # Get info to find multipart upload ID
            def _get_info() -> dict[str, Any] | None:
                try:
                    resp = self._client.get_object(Bucket=self._bucket, Key=info_key)
                    return json.loads(resp["Body"].read())
                except self._client.exceptions.NoSuchKey:
                    msg = f"Upload {upload_id} not found"
                    raise FileNotFoundError(msg)
                except ClientError as exc:
                    code = exc.response.get("Error", {}).get("Code")
                    if code in {"NoSuchKey", "404", "NotFound"}:
                        msg = f"Upload {upload_id} not found"
                        raise FileNotFoundError(msg)
                    raise

            info_data = await anyio.to_thread.run_sync(_get_info)
            assert info_data is not None
            info = UploadInfo.from_dict(info_data)

            mp_upload_id = info.storage_meta.get("multipart_upload_id")
            if mp_upload_id and not info.is_final:

                def _abort() -> None:
                    with contextlib.suppress(Exception):
                        self._client.abort_multipart_upload(
                            Bucket=self._bucket,
                            Key=data_key,
                            UploadId=str(mp_upload_id),
                        )

                await anyio.to_thread.run_sync(_abort)

            pending_key = f"{self._key_prefix}{upload_id}.pending"

            def _delete_info_and_pending() -> None:
                for key in (info_key, pending_key):
                    with contextlib.suppress(Exception):
                        self._client.delete_object(Bucket=self._bucket, Key=key)

            await anyio.to_thread.run_sync(_delete_info_and_pending)

            if info.is_final:

                def _delete_data() -> None:
                    with contextlib.suppress(Exception):
                        self._client.delete_object(Bucket=self._bucket, Key=data_key)

                await anyio.to_thread.run_sync(_delete_data)

        self._locks.pop(upload_id, None)
