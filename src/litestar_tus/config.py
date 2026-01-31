from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from litestar_tus.protocols import StorageBackend

SUPPORTED_CHECKSUM_ALGORITHMS: tuple[str, ...] = ("sha1", "sha256", "md5")


@dataclass
class TUSConfig:
    storage_backend: StorageBackend | None = None
    upload_dir: Path | str = Path("./uploads")
    path_prefix: str = "/files"
    max_size: int | None = None
    extensions: tuple[str, ...] = (
        "creation",
        "creation-with-upload",
        "termination",
        "expiration",
        "checksum",
    )
    expiration_seconds: int | None = 86400
