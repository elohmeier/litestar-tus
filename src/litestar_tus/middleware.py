from __future__ import annotations

from typing import Any, cast

from litestar.types import ASGIApp, Receive, Scope, Send

from litestar_tus.config import SUPPORTED_CHECKSUM_ALGORITHMS


class TUSMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        path_prefix: str = "/files",
        max_size: int | None = None,
        extensions: tuple[str, ...] = (),
    ) -> None:
        self.app = app
        self.path_prefix = path_prefix
        self.max_size = max_size
        self.extensions = extensions

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path: str = scope.get("path", "")
        if not path.startswith(self.path_prefix):
            await self.app(scope, receive, send)
            return

        method: str = scope.get("method", "").upper()

        # Handle OPTIONS directly in middleware (Litestar auto-handles OPTIONS
        # and may bypass the controller handler)
        if method == "OPTIONS":
            await self._send_options(send)
            return

        # Validate Tus-Resumable header for all other TUS requests
        headers = dict(scope.get("headers", []))
        tus_resumable = headers.get(b"tus-resumable", b"").decode()
        if tus_resumable != "1.0.0":
            await self._send_412(send)
            return

        # Inject Tus-Resumable header into all responses
        async def send_with_tus_header(message: Any) -> None:
            if message["type"] == "http.response.start":
                resp_headers = list(message.get("headers", []))
                resp_headers.append((b"tus-resumable", b"1.0.0"))
                message = {**message, "headers": resp_headers}
            await send(cast("Any", message))

        await self.app(scope, receive, cast("Send", send_with_tus_header))

    async def _send_options(self, send: Send) -> None:
        await send(
            cast(
                "Any",
                {
                    "type": "http.response.start",
                    "status": 204,
                    "headers": self._options_headers(),
                },
            )
        )
        await send(cast("Any", {"type": "http.response.body", "body": b""}))

    def _options_headers(self) -> list[tuple[bytes, bytes]]:
        headers: list[tuple[bytes, bytes]] = [
            (b"tus-resumable", b"1.0.0"),
            (b"tus-version", b"1.0.0"),
            (b"tus-extension", ",".join(self.extensions).encode()),
        ]
        if self.max_size is not None:
            headers.append((b"tus-max-size", str(self.max_size).encode()))
        if "checksum" in self.extensions:
            headers.append(
                (
                    b"tus-checksum-algorithm",
                    ",".join(SUPPORTED_CHECKSUM_ALGORITHMS).encode(),
                )
            )
        return headers

    @staticmethod
    async def _send_412(send: Send) -> None:
        await send(
            cast(
                "Any",
                {
                    "type": "http.response.start",
                    "status": 412,
                    "headers": [
                        (b"tus-version", b"1.0.0"),
                        (b"content-type", b"text/plain"),
                    ],
                },
            )
        )
        await send(
            cast(
                "Any",
                {"type": "http.response.body", "body": b"Unsupported TUS version"},
            )
        )
