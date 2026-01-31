from __future__ import annotations

import base64


class TestServerInfo:
    async def test_options_returns_capabilities(self, tus_client) -> None:
        resp = await tus_client.options("/files/")
        assert resp.status_code == 204
        assert resp.headers["tus-version"] == "1.0.0"
        assert "creation" in resp.headers["tus-extension"]
        assert "creation-with-upload" in resp.headers["tus-extension"]
        assert "termination" in resp.headers["tus-extension"]
        assert "expiration" in resp.headers["tus-extension"]
        assert resp.headers["tus-resumable"] == "1.0.0"

    async def test_options_includes_max_size(self, tus_client) -> None:
        resp = await tus_client.options("/files/")
        assert resp.headers["tus-max-size"] == str(1024 * 1024)


class TestMiddleware:
    async def test_missing_tus_resumable_returns_412(self, tus_client) -> None:
        resp = await tus_client.post("/files/", headers={"Upload-Length": "100"})
        assert resp.status_code == 412

    async def test_wrong_tus_resumable_returns_412(self, tus_client) -> None:
        resp = await tus_client.post(
            "/files/",
            headers={"Tus-Resumable": "0.2.0", "Upload-Length": "100"},
        )
        assert resp.status_code == 412

    async def test_options_does_not_require_tus_resumable(self, tus_client) -> None:
        resp = await tus_client.options("/files/")
        assert resp.status_code == 204

    async def test_non_tus_routes_unaffected(self, tus_client) -> None:
        resp = await tus_client.get("/not-a-tus-route")
        # Should not be 412 — just 404 because the route doesn't exist
        assert resp.status_code != 412


class TestCreateUpload:
    async def test_post_creates_upload(self, tus_client) -> None:
        resp = await tus_client.post(
            "/files/",
            headers={"Tus-Resumable": "1.0.0", "Upload-Length": "100"},
        )
        assert resp.status_code == 201
        assert "location" in resp.headers
        assert resp.headers["location"].startswith("/files/")
        assert resp.headers["upload-offset"] == "0"

    async def test_post_with_metadata(self, tus_client) -> None:
        fname = base64.b64encode(b"test.txt").decode()
        resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": "100",
                "Upload-Metadata": f"filename {fname}",
            },
        )
        assert resp.status_code == 201

    async def test_post_with_expiration(self, tus_client) -> None:
        resp = await tus_client.post(
            "/files/",
            headers={"Tus-Resumable": "1.0.0", "Upload-Length": "100"},
        )
        assert resp.status_code == 201
        assert "upload-expires" in resp.headers

    async def test_post_exceeds_max_size(self, tus_client) -> None:
        resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": str(1024 * 1024 + 1),
            },
        )
        assert resp.status_code == 413


class TestCreationWithUpload:
    async def test_post_with_body(self, tus_client) -> None:
        data = b"hello world"
        resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": str(len(data)),
                "Content-Type": "application/offset+octet-stream",
            },
            content=data,
        )
        assert resp.status_code == 201
        upload_offset = int(resp.headers["upload-offset"])
        assert upload_offset == len(data)

        # Verify with HEAD
        location = resp.headers["location"]
        head_resp = await tus_client.head(
            location,
            headers={"Tus-Resumable": "1.0.0"},
        )
        assert head_resp.status_code == 200
        assert int(head_resp.headers["upload-offset"]) == len(data)


class TestHeadUpload:
    async def test_head_returns_offset_and_length(self, tus_client) -> None:
        create_resp = await tus_client.post(
            "/files/",
            headers={"Tus-Resumable": "1.0.0", "Upload-Length": "100"},
        )
        location = create_resp.headers["location"]

        head_resp = await tus_client.head(
            location,
            headers={"Tus-Resumable": "1.0.0"},
        )
        assert head_resp.status_code == 200
        assert head_resp.headers["upload-offset"] == "0"
        assert head_resp.headers["upload-length"] == "100"
        assert head_resp.headers["cache-control"] == "no-store"

    async def test_head_not_found(self, tus_client) -> None:
        resp = await tus_client.head(
            "/files/nonexistent",
            headers={"Tus-Resumable": "1.0.0"},
        )
        assert resp.status_code == 404

    async def test_head_includes_metadata(self, tus_client) -> None:
        fname = base64.b64encode(b"test.txt").decode()
        create_resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": "100",
                "Upload-Metadata": f"filename {fname}",
            },
        )
        location = create_resp.headers["location"]

        head_resp = await tus_client.head(
            location,
            headers={"Tus-Resumable": "1.0.0"},
        )
        assert head_resp.status_code == 200
        assert "upload-metadata" in head_resp.headers
        assert "filename" in head_resp.headers["upload-metadata"]


class TestPatchUpload:
    async def test_patch_writes_chunk(self, tus_client) -> None:
        create_resp = await tus_client.post(
            "/files/",
            headers={"Tus-Resumable": "1.0.0", "Upload-Length": "11"},
        )
        location = create_resp.headers["location"]

        patch_resp = await tus_client.patch(
            location,
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": "0",
                "Content-Type": "application/offset+octet-stream",
            },
            content=b"hello world",
        )
        assert patch_resp.status_code == 204
        assert patch_resp.headers["upload-offset"] == "11"

    async def test_patch_wrong_offset(self, tus_client) -> None:
        create_resp = await tus_client.post(
            "/files/",
            headers={"Tus-Resumable": "1.0.0", "Upload-Length": "100"},
        )
        location = create_resp.headers["location"]

        resp = await tus_client.patch(
            location,
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": "50",
                "Content-Type": "application/offset+octet-stream",
            },
            content=b"data",
        )
        assert resp.status_code == 409

    async def test_patch_wrong_content_type(self, tus_client) -> None:
        create_resp = await tus_client.post(
            "/files/",
            headers={"Tus-Resumable": "1.0.0", "Upload-Length": "100"},
        )
        location = create_resp.headers["location"]

        resp = await tus_client.patch(
            location,
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": "0",
                "Content-Type": "application/octet-stream",
            },
            content=b"data",
        )
        assert resp.status_code == 415

    async def test_patch_not_found(self, tus_client) -> None:
        resp = await tus_client.patch(
            "/files/nonexistent",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": "0",
                "Content-Type": "application/offset+octet-stream",
            },
            content=b"data",
        )
        assert resp.status_code == 404


class TestFullLifecycle:
    async def test_create_patch_complete(self, tus_client) -> None:
        data = b"hello world!"
        # Create
        create_resp = await tus_client.post(
            "/files/",
            headers={"Tus-Resumable": "1.0.0", "Upload-Length": str(len(data))},
        )
        assert create_resp.status_code == 201
        location = create_resp.headers["location"]

        # Upload in two chunks
        chunk1 = data[:6]
        chunk2 = data[6:]

        patch1 = await tus_client.patch(
            location,
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": "0",
                "Content-Type": "application/offset+octet-stream",
            },
            content=chunk1,
        )
        assert patch1.status_code == 204
        assert patch1.headers["upload-offset"] == "6"

        patch2 = await tus_client.patch(
            location,
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": "6",
                "Content-Type": "application/offset+octet-stream",
            },
            content=chunk2,
        )
        assert patch2.status_code == 204
        assert patch2.headers["upload-offset"] == str(len(data))

        # Verify complete
        head_resp = await tus_client.head(
            location,
            headers={"Tus-Resumable": "1.0.0"},
        )
        assert head_resp.status_code == 200
        assert int(head_resp.headers["upload-offset"]) == len(data)


class TestDeleteUpload:
    async def test_delete_removes_upload(self, tus_client) -> None:
        create_resp = await tus_client.post(
            "/files/",
            headers={"Tus-Resumable": "1.0.0", "Upload-Length": "100"},
        )
        location = create_resp.headers["location"]

        del_resp = await tus_client.delete(
            location,
            headers={"Tus-Resumable": "1.0.0"},
        )
        assert del_resp.status_code == 204

        # HEAD after delete should return 404
        head_resp = await tus_client.head(
            location,
            headers={"Tus-Resumable": "1.0.0"},
        )
        assert head_resp.status_code == 404

    async def test_delete_not_found(self, tus_client) -> None:
        resp = await tus_client.delete(
            "/files/nonexistent",
            headers={"Tus-Resumable": "1.0.0"},
        )
        assert resp.status_code == 404
