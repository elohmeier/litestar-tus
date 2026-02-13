from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

UploadMetadata = dict[str, bytes]


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


@dataclass
class UploadInfo:
    id: str
    offset: int = 0
    size: int | None = None
    metadata: UploadMetadata = field(default_factory=dict)
    is_final: bool = False
    created_at: datetime = field(default_factory=_utcnow)
    expires_at: datetime | None = None
    storage_meta: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "offset": self.offset,
            "size": self.size,
            "metadata": {
                k: v.decode("utf-8", errors="surrogateescape")
                for k, v in self.metadata.items()
            },
            "is_final": self.is_final,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "storage_meta": self.storage_meta,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> UploadInfo:
        metadata_raw: dict[str, str] = data.get("metadata", {})
        metadata: UploadMetadata = {
            k: v.encode("utf-8", errors="surrogateescape")
            for k, v in metadata_raw.items()
        }

        created_at_raw = data.get("created_at")
        created_at = (
            datetime.fromisoformat(created_at_raw)
            if isinstance(created_at_raw, str)
            else _utcnow()
        )

        expires_at_raw = data.get("expires_at")
        expires_at = (
            datetime.fromisoformat(expires_at_raw)
            if isinstance(expires_at_raw, str)
            else None
        )

        storage_meta: dict[str, object] = data.get("storage_meta", {})

        return cls(
            id=str(data["id"]),
            offset=int(data.get("offset", 0)),
            size=int(data["size"]) if data.get("size") is not None else None,
            metadata=metadata,
            is_final=bool(data.get("is_final", False)),
            created_at=created_at,
            expires_at=expires_at,
            storage_meta=storage_meta,
        )


@dataclass(frozen=True)
class HookEvent:
    upload_info: UploadInfo
