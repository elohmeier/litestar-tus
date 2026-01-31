from __future__ import annotations

from pathlib import Path

from litestar import Litestar, Request
from litestar.testing import AsyncTestClient

from litestar_tus import TUSConfig, TUSPlugin
from litestar_tus._utils import parse_metadata_header


async def test_metadata_override_from_request(upload_dir: Path) -> None:
    async def metadata_override(
        request: Request, metadata: dict[str, bytes]
    ) -> dict[str, bytes]:
        metadata["user_id"] = request.headers.get("authorization", "").encode()
        return metadata

    config = TUSConfig(
        upload_dir=upload_dir,
        path_prefix="/files",
        max_size=1024 * 1024,
        metadata_override=metadata_override,
    )
    app = Litestar(plugins=[TUSPlugin(config)])

    async with AsyncTestClient(app) as client:
        resp = await client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": "10",
                "Upload-Metadata": "user_id dXNlcg==,foo Zm9v",
                "Authorization": "Bearer 123",
            },
        )
        assert resp.status_code == 201
        location = resp.headers["location"]

        head = await client.head(
            location,
            headers={"Tus-Resumable": "1.0.0"},
        )
        assert head.status_code == 200
        metadata = parse_metadata_header(head.headers["upload-metadata"])

    assert metadata["user_id"] == b"Bearer 123"
    assert metadata["foo"] == b"foo"
