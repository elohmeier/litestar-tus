from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import anyio
import anyio.to_thread
from botocore.exceptions import ClientError

from litestar_tus.models import UploadInfo

# boto3 S3 client type — use Any to avoid requiring mypy_boto3_s3 stubs
S3Client = Any


class S3Upload:
    def __init__(
        self,
        info: UploadInfo,
        *,
        client: S3Client,
        bucket: str,
        key_prefix: str,
        lock: anyio.Lock,
    ) -> None:
        self._info = info
        self._client = client
        self._bucket = bucket
        self._key_prefix = key_prefix
        self._lock = lock

    @property
    def _data_key(self) -> str:
        return f"{self._key_prefix}{self._info.id}"

    @property
    def _info_key(self) -> str:
        return f"{self._key_prefix}{self._info.id}.info"

    async def _save_info(self) -> None:
        body = json.dumps(self._info.to_dict()).encode("utf-8")
        await anyio.to_thread.run_sync(
            lambda: self._client.put_object(
                Bucket=self._bucket, Key=self._info_key, Body=body
            )
        )

    async def write_chunk(self, offset: int, src: AsyncIterator[bytes]) -> int:
        # Collect all data from the async iterator before acquiring the lock
        data = bytearray()
        async for chunk in src:
            data.extend(chunk)

        if not data:
            return 0

        data_bytes = bytes(data)

        async with self._lock:
            # Re-read authoritative state from S3
            info = await self.get_info()

            if offset != info.offset:
                msg = f"Offset mismatch: expected {info.offset}, got {offset}"
                raise ValueError(msg)

            upload_id = str(info.storage_meta.get("multipart_upload_id", ""))
            parts: list[dict[str, Any]] = list(info.storage_meta.get("parts", []))  # type: ignore[arg-type]
            part_number = len(parts) + 1

            def _upload_part() -> dict[str, Any]:
                resp = self._client.upload_part(
                    Bucket=self._bucket,
                    Key=self._data_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=data_bytes,
                )
                return {"ETag": resp["ETag"], "PartNumber": part_number}

            part = await anyio.to_thread.run_sync(_upload_part)
            parts.append(part)

            info.offset += len(data_bytes)
            info.storage_meta["parts"] = parts
            if info.size is not None and info.offset >= info.size:
                info.is_final = True
            self._info = info
            await self._save_info()
            return len(data_bytes)

    async def get_info(self) -> UploadInfo:
        def _get() -> bytes:
            resp = self._client.get_object(Bucket=self._bucket, Key=self._info_key)
            return resp["Body"].read()

        content = await anyio.to_thread.run_sync(_get)
        self._info = UploadInfo.from_dict(json.loads(content))
        return self._info

    async def finish(self) -> None:
        async with self._lock:
            info = await self.get_info()
            upload_id = str(info.storage_meta.get("multipart_upload_id", ""))
            parts: list[dict[str, Any]] = list(info.storage_meta.get("parts", []))  # type: ignore[arg-type]

            def _complete() -> None:
                self._client.complete_multipart_upload(
                    Bucket=self._bucket,
                    Key=self._data_key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )

            await anyio.to_thread.run_sync(_complete)
            info.is_final = True
            self._info = info
            await self._save_info()

    async def get_reader(self) -> AsyncIterator[bytes]:
        def _get() -> bytes:
            resp = self._client.get_object(Bucket=self._bucket, Key=self._data_key)
            return resp["Body"].read()

        data = await anyio.to_thread.run_sync(_get)
        yield data


class S3StorageBackend:
    """S3-based storage backend using multipart upload.

    Concurrency note: uses process-local locks (``anyio.Lock``). Only safe
    with a single worker process. For multi-worker deployments, use sticky
    sessions or a distributed lock.
    """

    def __init__(
        self, client: S3Client, bucket: str, key_prefix: str = "tus-uploads/"
    ) -> None:
        self._client = client
        self._bucket = bucket
        self._key_prefix = key_prefix
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
        )
        await upload._save_info()
        return upload

    async def get_upload(self, upload_id: str) -> S3Upload:
        info_key = f"{self._key_prefix}{upload_id}.info"

        def _get() -> bytes:
            try:
                resp = self._client.get_object(Bucket=self._bucket, Key=info_key)
                return resp["Body"].read()
            except self._client.exceptions.NoSuchKey:
                raise FileNotFoundError(f"Upload {upload_id} not found")
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                if code in {"NoSuchKey", "404", "NotFound"}:
                    raise FileNotFoundError(f"Upload {upload_id} not found")
                raise

        content = await anyio.to_thread.run_sync(_get)
        info = UploadInfo.from_dict(json.loads(content))
        return S3Upload(
            info,
            client=self._client,
            bucket=self._bucket,
            key_prefix=self._key_prefix,
            lock=self._get_lock(upload_id),
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
                    raise FileNotFoundError(f"Upload {upload_id} not found")
                except ClientError as exc:
                    code = exc.response.get("Error", {}).get("Code")
                    if code in {"NoSuchKey", "404", "NotFound"}:
                        raise FileNotFoundError(f"Upload {upload_id} not found")
                    raise

            info_data = await anyio.to_thread.run_sync(_get_info)
            assert info_data is not None
            info = UploadInfo.from_dict(info_data)

            mp_upload_id = info.storage_meta.get("multipart_upload_id")
            if mp_upload_id and not info.is_final:

                def _abort() -> None:
                    try:
                        self._client.abort_multipart_upload(
                            Bucket=self._bucket,
                            Key=data_key,
                            UploadId=str(mp_upload_id),
                        )
                    except Exception:
                        pass

                await anyio.to_thread.run_sync(_abort)

            def _delete_info() -> None:
                try:
                    self._client.delete_object(Bucket=self._bucket, Key=info_key)
                except Exception:
                    pass

            await anyio.to_thread.run_sync(_delete_info)

            if info.is_final:

                def _delete_data() -> None:
                    try:
                        self._client.delete_object(Bucket=self._bucket, Key=data_key)
                    except Exception:
                        pass

                await anyio.to_thread.run_sync(_delete_data)

        self._locks.pop(upload_id, None)
