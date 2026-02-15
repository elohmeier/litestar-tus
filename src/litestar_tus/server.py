"""Standalone TUS server with configurable storage backend.

Configure via environment variables (TUS_ prefix). S3 backend is activated
when TUS_S3_BUCKET is set; otherwise the file backend is used.

Usage::

    # File backend (default)
    TUS_UPLOAD_DIR=./data litestar-tus

    # S3 backend
    TUS_S3_BUCKET=my-bucket AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... litestar-tus

    # Or via Litestar CLI
    litestar --app litestar_tus.server:create_app run
"""

from __future__ import annotations

from typing import cast

from litestar import Litestar, get
from pydantic_settings import BaseSettings

from litestar_tus.config import TUSConfig
from litestar_tus.plugin import TUSPlugin
from litestar_tus.protocols import StorageBackend


class Settings(BaseSettings):
    model_config = {"env_prefix": "TUS_"}

    upload_dir: str = "./data"
    base_path: str = "/files/"
    max_size: int = 0
    behind_proxy: bool = False
    host: str = "0.0.0.0"
    port: int = 8080

    s3_bucket: str = ""
    s3_object_prefix: str = ""
    s3_endpoint: str = ""
    s3_part_size: int = 10 * 1024 * 1024  # 10 MiB


def create_app() -> Litestar:
    settings = Settings()

    # Normalize base_path: ensure leading slash, strip trailing slash
    base_path = "/" + settings.base_path.strip("/")

    if settings.s3_bucket:
        import boto3

        from litestar_tus.backends.s3 import S3StorageBackend

        client_kwargs: dict[str, str] = {}
        if settings.s3_endpoint:
            client_kwargs["endpoint_url"] = settings.s3_endpoint

        s3_client = boto3.client("s3", **client_kwargs)

        backend: StorageBackend = cast(
            StorageBackend,
            S3StorageBackend(
                client=s3_client,
                bucket=settings.s3_bucket,
                key_prefix=settings.s3_object_prefix,
                part_size=settings.s3_part_size,
            ),
        )
    else:
        from litestar_tus.backends.file import FileStorageBackend

        backend = cast(
            StorageBackend, FileStorageBackend(upload_dir=settings.upload_dir)
        )

    config = TUSConfig(
        storage_backend=backend,
        path_prefix=base_path,
        max_size=settings.max_size if settings.max_size > 0 else None,
    )

    @get("/health", exclude_from_auth=True)
    async def health_check() -> dict[str, str]:
        return {"status": "ok"}

    return Litestar(
        plugins=[TUSPlugin(config)],
        route_handlers=[health_check],
        request_max_body_size=settings.max_size
        if settings.max_size > 0
        else 100 * 1024**3,
    )


def main() -> None:
    import uvicorn

    settings = Settings()
    uvicorn.run(create_app(), host=settings.host, port=settings.port)
