from __future__ import annotations

import base64
import hashlib
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone

from litestar import Controller, Request, Response, delete, head, patch, post
from litestar.exceptions import HTTPException, NotFoundException

from litestar_tus._utils import (
    encode_metadata,
    generate_upload_id,
    parse_metadata_header,
    safe_emit,
)
from litestar_tus.config import SUPPORTED_CHECKSUM_ALGORITHMS, TUSConfig
from litestar_tus.events import TUSEvent
from litestar_tus.models import UploadInfo
from litestar_tus.protocols import StorageBackend


def _format_http_date(dt: datetime) -> str:
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


class ChecksumAwareStream:
    """Async iterator wrapper that validates checksum as data streams through."""

    def __init__(
        self,
        source: AsyncIterator[bytes],
        checksum_header: str | None = None,
    ) -> None:
        self._source = source
        self._hasher: hashlib._Hash | None = None
        self._expected_digest: bytes | None = None

        if checksum_header is not None:
            self._parse_header(checksum_header)

    def _parse_header(self, header_value: str) -> None:
        parts = header_value.split(" ", 1)
        if len(parts) != 2:
            raise HTTPException(
                status_code=400, detail="Malformed Upload-Checksum header"
            )
        algo, b64digest = parts
        if algo not in SUPPORTED_CHECKSUM_ALGORITHMS:
            raise HTTPException(
                status_code=400, detail=f"Unsupported checksum algorithm: {algo}"
            )
        try:
            self._expected_digest = base64.b64decode(b64digest, validate=True)
        except Exception:
            raise HTTPException(
                status_code=400, detail="Invalid base64 in Upload-Checksum header"
            )
        self._hasher = hashlib.new(algo)

    def __aiter__(self) -> ChecksumAwareStream:
        return self

    async def __anext__(self) -> bytes:
        try:
            chunk = await self._source.__anext__()
        except StopAsyncIteration:
            if self._hasher is not None and self._expected_digest is not None:
                if self._hasher.digest() != self._expected_digest:
                    raise HTTPException(status_code=460, detail="Checksum mismatch")
            raise
        if self._hasher is not None:
            self._hasher.update(chunk)
        return chunk


def _check_expired(info: UploadInfo) -> None:
    if info.expires_at is not None and datetime.now(tz=timezone.utc) >= info.expires_at:
        raise HTTPException(status_code=410, detail="Upload has expired")


def build_tus_controller(config: TUSConfig) -> type[Controller]:
    class TUSController(Controller):
        path = config.path_prefix

        @post("/", status_code=201)
        async def create_upload(
            self, request: Request, tus_storage: StorageBackend
        ) -> Response:
            upload_length_header = request.headers.get("upload-length")
            size: int | None = None
            if upload_length_header is not None:
                size = int(upload_length_header)
                if config.max_size is not None and size > config.max_size:
                    raise HTTPException(
                        status_code=413, detail="Upload exceeds maximum size"
                    )

            metadata_header = request.headers.get("upload-metadata", "")
            metadata = parse_metadata_header(metadata_header)

            now = datetime.now(tz=timezone.utc)
            expires_at: datetime | None = None
            if config.expiration_seconds is not None:
                expires_at = now + timedelta(seconds=config.expiration_seconds)

            upload_id = generate_upload_id()
            info = UploadInfo(
                id=upload_id,
                size=size,
                offset=0,
                metadata=metadata,
                created_at=now,
                expires_at=expires_at,
            )

            safe_emit(request.app, TUSEvent.PRE_CREATE, upload_info=info)

            upload = await tus_storage.create_upload(info)

            safe_emit(request.app, TUSEvent.POST_CREATE, upload_info=info)

            # creation-with-upload: if request has body, write it
            content_type = request.headers.get("content-type", "")
            if content_type == "application/offset+octet-stream":
                content_length_header = request.headers.get("content-length")
                if content_length_header is not None and size is not None:
                    if int(content_length_header) > size:
                        raise HTTPException(
                            status_code=400,
                            detail="Body exceeds declared upload size",
                        )

                checksum_header = request.headers.get("upload-checksum")
                stream = ChecksumAwareStream(request.stream(), checksum_header)

                try:
                    await upload.write_chunk(0, stream)
                except ValueError as exc:
                    raise HTTPException(status_code=409, detail=str(exc))
                info = await upload.get_info()
                safe_emit(request.app, TUSEvent.POST_RECEIVE, upload_info=info)

                if info.is_final:
                    safe_emit(request.app, TUSEvent.PRE_FINISH, upload_info=info)
                    await upload.finish()
                    safe_emit(request.app, TUSEvent.POST_FINISH, upload_info=info)

            location = f"{config.path_prefix}/{upload_id}"
            response_headers: dict[str, str] = {
                "Location": location,
                "Upload-Offset": str(info.offset),
            }
            if expires_at is not None:
                response_headers["Upload-Expires"] = _format_http_date(expires_at)

            return Response(content=None, status_code=201, headers=response_headers)

        @head("/{upload_id:str}")
        async def get_upload_info(
            self, upload_id: str, tus_storage: StorageBackend
        ) -> Response[None]:
            try:
                upload = await tus_storage.get_upload(upload_id)
            except FileNotFoundError:
                raise NotFoundException(detail="Upload not found")

            info = await upload.get_info()
            _check_expired(info)
            response_headers: dict[str, str] = {
                "Upload-Offset": str(info.offset),
                "Cache-Control": "no-store",
            }
            if info.size is not None:
                response_headers["Upload-Length"] = str(info.size)
            if info.metadata:
                response_headers["Upload-Metadata"] = encode_metadata(info.metadata)
            if info.expires_at is not None:
                response_headers["Upload-Expires"] = _format_http_date(info.expires_at)

            return Response(content=None, status_code=200, headers=response_headers)

        @patch("/{upload_id:str}", status_code=204)
        async def write_chunk(
            self, upload_id: str, request: Request, tus_storage: StorageBackend
        ) -> Response[None]:
            content_type = request.headers.get("content-type", "")
            if content_type != "application/offset+octet-stream":
                raise HTTPException(status_code=415, detail="Invalid Content-Type")

            offset_header = request.headers.get("upload-offset")
            if offset_header is None:
                raise HTTPException(
                    status_code=400, detail="Missing Upload-Offset header"
                )
            client_offset = int(offset_header)

            try:
                upload = await tus_storage.get_upload(upload_id)
            except FileNotFoundError:
                raise NotFoundException(detail="Upload not found")

            info = await upload.get_info()
            _check_expired(info)
            if client_offset != info.offset:
                raise HTTPException(status_code=409, detail="Offset mismatch")

            content_length_header = request.headers.get("content-length")
            if content_length_header is not None and info.size is not None:
                if client_offset + int(content_length_header) > info.size:
                    raise HTTPException(
                        status_code=400,
                        detail="Body exceeds declared upload size",
                    )

            checksum_header = request.headers.get("upload-checksum")
            stream = ChecksumAwareStream(request.stream(), checksum_header)

            try:
                await upload.write_chunk(client_offset, stream)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc))
            info = await upload.get_info()

            safe_emit(request.app, TUSEvent.POST_RECEIVE, upload_info=info)

            if info.is_final:
                safe_emit(request.app, TUSEvent.PRE_FINISH, upload_info=info)
                await upload.finish()
                safe_emit(request.app, TUSEvent.POST_FINISH, upload_info=info)

            response_headers: dict[str, str] = {
                "Upload-Offset": str(info.offset),
            }
            if info.expires_at is not None:
                response_headers["Upload-Expires"] = _format_http_date(info.expires_at)

            return Response(content=None, status_code=204, headers=response_headers)

        @delete("/{upload_id:str}")
        async def terminate_upload(
            self, upload_id: str, request: Request, tus_storage: StorageBackend
        ) -> Response[None]:
            try:
                upload = await tus_storage.get_upload(upload_id)
            except FileNotFoundError:
                raise NotFoundException(detail="Upload not found")

            info = await upload.get_info()
            _check_expired(info)
            safe_emit(request.app, TUSEvent.PRE_TERMINATE, upload_info=info)

            await tus_storage.terminate_upload(upload_id)

            safe_emit(request.app, TUSEvent.POST_TERMINATE, upload_info=info)

            return Response(content=None, status_code=204)

    return TUSController
