"""Litestar TUS server with S3 (MinIO) backend for performance testing."""

import logging
import time
from typing import cast

import boto3
import uvicorn
from litestar import Litestar, Request
from litestar.middleware import AbstractMiddleware
from litestar.types import Message, Receive, Scope, Send

from litestar_tus import S3StorageBackend, TUSConfig, TUSPlugin
from litestar_tus.protocols import StorageBackend

logger = logging.getLogger("perf")


class TimingMiddleware(AbstractMiddleware):
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        start = time.perf_counter()
        status_code: int | None = None

        original_send = send

        async def timed_send(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await original_send(message)

        await self.app(scope, receive, timed_send)

        elapsed_ms = (time.perf_counter() - start) * 1000
        content_length = request.headers.get("content-length", "?")
        logger.info(
            "%s %s -> %s  %.1f ms  (body %s bytes)",
            request.method,
            request.url.path,
            status_code,
            elapsed_ms,
            content_length,
        )


s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
)

backend = S3StorageBackend(
    client=s3_client,
    bucket="bucket",
    key_prefix="perf-uploads/",
)

config = TUSConfig(
    path_prefix="/files",
    max_size=100 * 1024**3,
    storage_backend=cast(StorageBackend, backend),
)

# Litestar defaults to ~9.5 MiB; raise to match max_size so chunks aren't rejected
app = Litestar(
    plugins=[TUSPlugin(config)],
    middleware=[TimingMiddleware],
    request_max_body_size=100 * 1024**3,
)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )
    # Silence noisy libraries — set to DEBUG to see raw S3 HTTP traffic
    logging.getLogger("botocore").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)
    logging.getLogger("boto3").setLevel(logging.INFO)


if __name__ == "__main__":
    _configure_logging()
    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False, log_config=None)
