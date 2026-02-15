from __future__ import annotations

import base64
import logging
import secrets
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from litestar.exceptions import HTTPException, ImproperlyConfiguredException

from litestar_tus.models import UploadMetadata

if TYPE_CHECKING:
    from litestar import Litestar


logger = logging.getLogger(__name__)


def generate_upload_id() -> str:
    return secrets.token_hex(16)


def parse_metadata_header(header: str) -> UploadMetadata:
    metadata: UploadMetadata = {}
    if not header.strip():
        return metadata
    for pair in header.split(","):
        pair = pair.strip()
        if not pair:
            continue
        parts = pair.split(None, 1)
        key = parts[0]
        if len(parts) == 2:
            metadata[key] = base64.b64decode(parts[1])
        else:
            metadata[key] = b""
    return metadata


def encode_metadata(metadata: UploadMetadata) -> str:
    pairs: list[str] = []
    for key, value in metadata.items():
        encoded = base64.b64encode(value).decode("ascii")
        pairs.append(f"{key} {encoded}")
    return ",".join(pairs)


def parse_concat_header(header: str, path_prefix: str) -> tuple[str, list[str]]:
    """Parse an ``Upload-Concat`` header value.

    Returns a ``(concat_type, partial_ids)`` tuple.  For ``"partial"`` the
    list is empty.  For ``"final;url1 url2 ..."`` the list contains the
    extracted upload IDs.
    """
    header = header.strip()
    if header == "partial":
        return ("partial", [])

    if not header.startswith("final;"):
        raise HTTPException(status_code=400, detail="Invalid Upload-Concat header")

    urls_part = header[len("final;") :].strip()
    if not urls_part:
        raise HTTPException(status_code=400, detail="Invalid Upload-Concat header")

    ids: list[str] = []
    for raw_url in urls_part.split():
        raw_url = raw_url.strip()
        if not raw_url:
            continue
        # Handle absolute URLs: strip scheme+host, keep path
        parsed = urlparse(raw_url)
        path = parsed.path if parsed.scheme else raw_url
        # Strip the path_prefix to extract the upload ID
        prefix = path_prefix.rstrip("/") + "/"
        if path.startswith(prefix):
            ids.append(path[len(prefix) :])
        else:
            raise HTTPException(status_code=400, detail="Invalid Upload-Concat header")

    if not ids:
        raise HTTPException(status_code=400, detail="Invalid Upload-Concat header")

    return ("final", ids)


def safe_emit(app: Litestar, event_id: str, **kwargs: Any) -> None:
    try:
        app.emit(event_id, **kwargs)
    except ImproperlyConfiguredException:
        logger.debug("TUS event handler skipped (no listeners): %s", event_id)
    except Exception:
        logger.exception("TUS event handler failed: %s", event_id)
