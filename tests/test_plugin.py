from __future__ import annotations

from pathlib import Path

from litestar import Litestar
from litestar.testing import AsyncTestClient

from litestar_tus import TUSConfig, TUSEvent, TUSPlugin, UploadInfo
from litestar_tus.backends.file import FileStorageBackend


class TestPluginRegistration:
    def test_default_config(self) -> None:
        app = Litestar(plugins=[TUSPlugin()])
        route_paths = {r.path for r in app.routes}
        assert "/files/" in route_paths or "/files" in route_paths

    def test_custom_path_prefix(self) -> None:
        app = Litestar(plugins=[TUSPlugin(TUSConfig(path_prefix="/uploads"))])
        route_paths = {r.path for r in app.routes}
        assert any("/uploads" in p for p in route_paths)

    def test_custom_storage_backend(self, upload_dir: Path) -> None:
        backend = FileStorageBackend(upload_dir)
        config = TUSConfig(storage_backend=backend, path_prefix="/files")  # type: ignore[arg-type]
        app = Litestar(plugins=[TUSPlugin(config)])
        route_paths = {r.path for r in app.routes}
        assert any("/files" in p for p in route_paths)


class TestPluginDI:
    async def test_storage_injected(self, tus_client) -> None:
        resp = await tus_client.post(
            "/files/",
            headers={"Tus-Resumable": "1.0.0", "Upload-Length": "10"},
        )
        assert resp.status_code == 201


class TestPluginMiddleware:
    async def test_middleware_active(self, tus_client) -> None:
        # Request without Tus-Resumable header to TUS route
        resp = await tus_client.post("/files/", headers={"Upload-Length": "100"})
        assert resp.status_code == 412

    async def test_tus_resumable_in_response(self, tus_client) -> None:
        resp = await tus_client.options("/files/")
        assert resp.headers["tus-resumable"] == "1.0.0"


class TestPluginEvents:
    async def test_events_fire(self, upload_dir: Path) -> None:
        from litestar.events import listener

        received: list[str] = []

        @listener(TUSEvent.POST_CREATE)
        async def on_create(upload_info: UploadInfo) -> None:
            received.append("post_create")

        @listener(TUSEvent.POST_RECEIVE)
        async def on_receive(upload_info: UploadInfo) -> None:
            received.append("post_receive")

        @listener(TUSEvent.POST_FINISH)
        async def on_finish(upload_info: UploadInfo) -> None:
            received.append("post_finish")

        config = TUSConfig(
            upload_dir=upload_dir, path_prefix="/files", max_size=1024 * 1024
        )
        app = Litestar(
            plugins=[TUSPlugin(config)],
            listeners=[on_create, on_receive, on_finish],
        )

        async with AsyncTestClient(app) as client:
            data = b"hello"
            resp = await client.post(
                "/files/",
                headers={
                    "Tus-Resumable": "1.0.0",
                    "Upload-Length": str(len(data)),
                },
            )
            assert resp.status_code == 201
            location = resp.headers["location"]

            await client.patch(
                location,
                headers={
                    "Tus-Resumable": "1.0.0",
                    "Upload-Offset": "0",
                    "Content-Type": "application/offset+octet-stream",
                },
                content=data,
            )

            # Give the event system a moment to process
            import anyio

            await anyio.sleep(0.1)

        assert "post_create" in received
        assert "post_receive" in received
        assert "post_finish" in received
