from __future__ import annotations

import base64
import secrets
from typing import TYPE_CHECKING, Any

from litestar_tus.models import UploadMetadata

if TYPE_CHECKING:
    from litestar import Litestar


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


def safe_emit(app: Litestar, event_id: str, **kwargs: Any) -> None:
    try:
        app.emit(event_id, **kwargs)
    except Exception:
        pass
