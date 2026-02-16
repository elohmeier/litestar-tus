from __future__ import annotations

import contextlib
import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

import anyio
import anyio.to_thread
from botocore.exceptions import ClientError

from litestar_tus.models import UploadInfo

logger = logging.getLogger("litestar_tus.s3")

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
            t0 = time.perf_counter()
            info = await self.get_info()
            t_get_info = time.perf_counter()

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
            parts_uploaded = 0
            t_stream_start = time.perf_counter()
            t_upload_parts = 0.0
            stream_exc: Exception | None = None
            try:
                async for chunk in src:
                    buf.extend(chunk)
                    total_written += len(chunk)

                    # Flush full parts as they accumulate
                    while len(buf) >= self._part_size:
                        part_number = len(parts) + 1
                        part_data = bytes(buf[: self._part_size])
                        t_part = time.perf_counter()
                        part = await self._upload_part(upload_id, part_number, part_data)
                        t_upload_parts += time.perf_counter() - t_part
                        parts_uploaded += 1
                        parts.append(part)
                        del buf[: self._part_size]
            except Exception as exc:
                status_code = getattr(exc, "status_code", None)
                if status_code is not None and status_code < 500:
                    raise
                stream_exc = exc

            t_stream_end = time.perf_counter()

            if total_written == 0 and pending_size == 0 and not stream_exc:
                return 0

            # Store leftover as pending or delete if empty
            t_pending = time.perf_counter()
            if buf:
                await self._put_pending(bytes(buf))
            elif pending_size > 0:
                await self._delete_pending()
            t_pending_done = time.perf_counter()

            info.offset += total_written
            info.storage_meta["parts"] = parts
            info.storage_meta["pending_size"] = len(buf)
            if info.size is not None and info.offset >= info.size:
                info.is_final = True
            self._info = info
            await self._save_info()
            t_end = time.perf_counter()

            logger.debug(
                "write_chunk %s offset=%d wrote=%d parts_uploaded=%d "
                "pending=%d | get_info=%.1fms stream+upload=%.1fms "
                "(upload_parts=%.1fms) pending_io=%.1fms save_info=%.1fms "
                "total=%.1fms",
                self._info.id,
                offset,
                total_written,
                parts_uploaded,
                len(buf),
                (t_get_info - t0) * 1000,
                (t_stream_end - t_stream_start) * 1000,
                t_upload_parts * 1000,
                (t_pending_done - t_pending) * 1000,
                (t_end - t_pending_done) * 1000,
                (t_end - t0) * 1000,
            )

            if stream_exc:
                raise stream_exc

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
            t0 = time.perf_counter()
            info = await self.get_info()
            t_get_info = time.perf_counter()
            upload_id = str(info.storage_meta.get("multipart_upload_id", ""))
            parts: list[dict[str, Any]] = list(info.storage_meta.get("parts", []))  # type: ignore[arg-type]
            pending_size: int = info.storage_meta.get("pending_size", 0)  # type: ignore[assignment]

            # Flush any pending data as the final part
            t_pending = time.perf_counter()
            if pending_size > 0:
                pending_data = await self._get_pending()
                if pending_data:
                    part_number = len(parts) + 1
                    part = await self._upload_part(upload_id, part_number, pending_data)
                    parts.append(part)
                await self._delete_pending()
            t_pending_done = time.perf_counter()

            def _complete() -> None:
                self._client.complete_multipart_upload(
                    Bucket=self._bucket,
                    Key=self._data_key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )

            await anyio.to_thread.run_sync(_complete)
            t_complete = time.perf_counter()
            info.is_final = True
            info.storage_meta["parts"] = parts
            info.storage_meta["pending_size"] = 0
            self._info = info
            await self._save_info()
            t_end = time.perf_counter()

            logger.debug(
                "finish %s parts=%d | get_info=%.1fms flush_pending=%.1fms "
                "complete_multipart=%.1fms save_info=%.1fms total=%.1fms",
                self._info.id,
                len(parts),
                (t_get_info - t0) * 1000,
                (t_pending_done - t_pending) * 1000,
                (t_complete - t_pending_done) * 1000,
                (t_end - t_complete) * 1000,
                (t_end - t0) * 1000,
            )

    async def get_reader(self) -> AsyncIterator[bytes]:
        def _get() -> bytes:
            resp = self._client.get_object(Bucket=self._bucket, Key=self._data_key)
            return resp["Body"].read()

        data = await anyio.to_thread.run_sync(_get)
        yield data


_MIN_PART_SIZE = 5 * 1024 * 1024  # 5 MiB — AWS S3 minimum for multipart parts
_DEFAULT_PART_SIZE = 10 * 1024 * 1024  # 10 MiB
_MAX_COPY_PART_SIZE = 5 * 1024 * 1024 * 1024  # 5 GiB — AWS S3 max part size


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
            except self._client.exceptions.NoSuchKey as exc:
                msg = f"Upload {upload_id} not found"
                raise FileNotFoundError(msg) from exc
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                if code in {"NoSuchKey", "404", "NotFound"}:
                    msg = f"Upload {upload_id} not found"
                    raise FileNotFoundError(msg) from exc
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

    async def concatenate_uploads(
        self, final_info: UploadInfo, partial_ids: list[str]
    ) -> S3Upload:
        t0 = time.perf_counter()
        data_key = f"{self._key_prefix}{final_info.id}"

        def _create_multipart() -> str:
            resp = self._client.create_multipart_upload(
                Bucket=self._bucket, Key=data_key
            )
            return resp["UploadId"]

        mp_upload_id = await anyio.to_thread.run_sync(_create_multipart)
        t_create = time.perf_counter()
        logger.debug(
            "concatenate %s: create_multipart_upload %.1fms",
            final_info.id,
            (t_create - t0) * 1000,
        )

        # Get sizes for all partials so we can decide copy vs download
        partial_keys = [f"{self._key_prefix}{pid}" for pid in partial_ids]

        def _head_sizes() -> list[int]:
            sizes = []
            for key in partial_keys:
                resp = self._client.head_object(Bucket=self._bucket, Key=key)
                sizes.append(resp["ContentLength"])
            return sizes

        partial_sizes = await anyio.to_thread.run_sync(_head_sizes)
        t_head = time.perf_counter()
        logger.debug(
            "concatenate %s: head %d partials %.1fms (sizes: %s)",
            final_info.id,
            len(partial_ids),
            (t_head - t_create) * 1000,
            [f"{s / 1024 / 1024:.1f}MiB" for s in partial_sizes],
        )

        copies = 0
        downloads = 0
        uploads = 0

        # Decide which partials can use server-side copy.  We can only
        # use copy when no small-partial buffer has accumulated before
        # this index.  Scan forward: once we hit a partial that needs
        # download, all subsequent ones must also go through download
        # (because the buffer won't be empty).
        copy_eligible: list[bool] = []
        buf_pending = False
        for idx, size in enumerate(partial_sizes):
            is_last = idx == len(partial_ids) - 1
            if (
                not buf_pending
                and size <= _MAX_COPY_PART_SIZE
                and (size >= _MIN_PART_SIZE or is_last)
                and size > 0
            ):
                copy_eligible.append(True)
            else:
                copy_eligible.append(False)
                buf_pending = True

        # ── Fast path: run all server-side copies in parallel ──
        # Each copy gets a fixed part number so they can run concurrently.
        copy_results: dict[int, dict[str, Any]] = {}

        async def _do_copy(pn: int, pk: str, idx: int, size: int) -> None:
            t_op = time.perf_counter()

            def _copy() -> dict[str, Any]:
                resp = self._client.upload_part_copy(
                    Bucket=self._bucket,
                    Key=data_key,
                    UploadId=mp_upload_id,
                    PartNumber=pn,
                    CopySource={"Bucket": self._bucket, "Key": pk},
                )
                return {
                    "ETag": resp["CopyPartResult"]["ETag"],
                    "PartNumber": pn,
                }

            result = await anyio.to_thread.run_sync(_copy)
            copy_results[pn] = result
            logger.debug(
                "concatenate %s: upload_part_copy part=%d partial=%d/%d %.1fMiB %.1fms",
                final_info.id,
                pn,
                idx + 1,
                len(partial_ids),
                size / 1024 / 1024,
                (time.perf_counter() - t_op) * 1000,
            )

        # Assign part numbers up-front so copies and downloads don't
        # conflict.  Copies get the first N part numbers (one per
        # copy-eligible partial), downloads fill remaining slots after.
        next_part = 1
        copy_tasks: list[tuple[int, str, int, int]] = []
        download_indices: list[int] = []

        for idx, eligible in enumerate(copy_eligible):
            if eligible:
                copy_tasks.append(
                    (next_part, partial_keys[idx], idx, partial_sizes[idx])
                )
                next_part += 1
                copies += 1
            else:
                download_indices.append(idx)

        if copy_tasks:
            async with anyio.create_task_group() as tg:
                for pn, pk, idx, size in copy_tasks:
                    tg.start_soon(_do_copy, pn, pk, idx, size)

        # Collect copy results in part-number order
        parts: list[dict[str, Any]] = [copy_results[pn] for pn, _, _, _ in copy_tasks]

        # ── Slow path: download + re-upload for small partials ──
        small_buf = bytearray()
        for idx in download_indices:
            partial_key = partial_keys[idx]
            size = partial_sizes[idx]
            t_op = time.perf_counter()

            def _download(key: str = partial_key) -> bytes:
                resp = self._client.get_object(Bucket=self._bucket, Key=key)
                return resp["Body"].read()

            data = await anyio.to_thread.run_sync(_download)
            downloads += 1
            logger.debug(
                "concatenate %s: download partial=%d/%d %.1fMiB %.1fms (buffering)",
                final_info.id,
                idx + 1,
                len(partial_ids),
                size / 1024 / 1024,
                (time.perf_counter() - t_op) * 1000,
            )
            small_buf.extend(data)

            while len(small_buf) >= self._part_size:
                part_number = next_part
                next_part += 1
                part_data = bytes(small_buf[: self._part_size])
                t_up = time.perf_counter()

                def _upload(
                    pn: int = part_number, pd: bytes = part_data
                ) -> dict[str, Any]:
                    resp = self._client.upload_part(
                        Bucket=self._bucket,
                        Key=data_key,
                        UploadId=mp_upload_id,
                        PartNumber=pn,
                        Body=pd,
                    )
                    return {"ETag": resp["ETag"], "PartNumber": pn}

                part = await anyio.to_thread.run_sync(_upload)
                parts.append(part)
                uploads += 1
                logger.debug(
                    "concatenate %s: upload_part part=%d %.1fMiB %.1fms",
                    final_info.id,
                    part_number,
                    self._part_size / 1024 / 1024,
                    (time.perf_counter() - t_up) * 1000,
                )
                del small_buf[: self._part_size]

        t_parts_done = time.perf_counter()

        # Flush remaining buffer as the last part
        if small_buf:
            part_number = next_part
            remaining = bytes(small_buf)
            t_flush = time.perf_counter()

            def _upload_last() -> dict[str, Any]:
                resp = self._client.upload_part(
                    Bucket=self._bucket,
                    Key=data_key,
                    UploadId=mp_upload_id,
                    PartNumber=part_number,
                    Body=remaining,
                )
                return {"ETag": resp["ETag"], "PartNumber": part_number}

            part = await anyio.to_thread.run_sync(_upload_last)
            parts.append(part)
            uploads += 1
            logger.debug(
                "concatenate %s: upload_part (final flush) part=%d %.1fMiB %.1fms",
                final_info.id,
                part_number,
                len(remaining) / 1024 / 1024,
                (time.perf_counter() - t_flush) * 1000,
            )

        def _complete() -> None:
            self._client.complete_multipart_upload(
                Bucket=self._bucket,
                Key=data_key,
                UploadId=mp_upload_id,
                MultipartUpload={"Parts": parts},
            )

        await anyio.to_thread.run_sync(_complete)
        t_complete = time.perf_counter()

        final_info.storage_meta["multipart_upload_id"] = mp_upload_id
        final_info.storage_meta["parts"] = parts
        final_info.storage_meta["pending_size"] = 0

        upload = S3Upload(
            final_info,
            client=self._client,
            bucket=self._bucket,
            key_prefix=self._key_prefix,
            lock=self._get_lock(final_info.id),
            part_size=self._part_size,
        )
        await upload._save_info()
        t_end = time.perf_counter()

        logger.debug(
            "concatenate %s: DONE %d partials -> %d parts "
            "(copies=%d downloads=%d uploads=%d) | "
            "create=%.1fms head=%.1fms parts=%.1fms "
            "complete=%.1fms save_info=%.1fms total=%.1fms",
            final_info.id,
            len(partial_ids),
            len(parts),
            copies,
            downloads,
            uploads,
            (t_create - t0) * 1000,
            (t_head - t_create) * 1000,
            (t_parts_done - t_head) * 1000,
            (t_complete - t_parts_done) * 1000,
            (t_end - t_complete) * 1000,
            (t_end - t0) * 1000,
        )
        return upload

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
                except self._client.exceptions.NoSuchKey as exc:
                    msg = f"Upload {upload_id} not found"
                    raise FileNotFoundError(msg) from exc
                except ClientError as exc:
                    code = exc.response.get("Error", {}).get("Code")
                    if code in {"NoSuchKey", "404", "NotFound"}:
                        msg = f"Upload {upload_id} not found"
                        raise FileNotFoundError(msg) from exc
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
