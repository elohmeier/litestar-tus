from __future__ import annotations


class TestConcatenationOptions:
    async def test_options_includes_concatenation(self, tus_client) -> None:
        resp = await tus_client.options("/files/")
        assert resp.status_code == 204
        assert "concatenation" in resp.headers["tus-extension"]


class TestPartialUpload:
    async def test_create_partial_upload(self, tus_client) -> None:
        resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": "5",
                "Upload-Concat": "partial",
            },
        )
        assert resp.status_code == 201
        location = resp.headers["location"]

        head_resp = await tus_client.head(
            location,
            headers={"Tus-Resumable": "1.0.0"},
        )
        assert head_resp.status_code == 200
        assert head_resp.headers["upload-concat"] == "partial"

    async def test_partial_with_creation_with_upload(self, tus_client) -> None:
        data = b"hello"
        resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": str(len(data)),
                "Upload-Concat": "partial",
                "Content-Type": "application/offset+octet-stream",
            },
            content=data,
        )
        assert resp.status_code == 201
        assert int(resp.headers["upload-offset"]) == len(data)


class TestFinalUpload:
    async def test_create_final_from_two_partials(self, tus_client) -> None:
        # Create and upload partial 1
        data1 = b"hello"
        resp1 = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": str(len(data1)),
                "Upload-Concat": "partial",
            },
        )
        assert resp1.status_code == 201
        loc1 = resp1.headers["location"]

        await tus_client.patch(
            loc1,
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": "0",
                "Content-Type": "application/offset+octet-stream",
            },
            content=data1,
        )

        # Create and upload partial 2
        data2 = b" world"
        resp2 = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": str(len(data2)),
                "Upload-Concat": "partial",
            },
        )
        assert resp2.status_code == 201
        loc2 = resp2.headers["location"]

        await tus_client.patch(
            loc2,
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": "0",
                "Content-Type": "application/offset+octet-stream",
            },
            content=data2,
        )

        # Create final upload
        final_resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Concat": f"final;{loc1} {loc2}",
            },
        )
        assert final_resp.status_code == 201
        final_loc = final_resp.headers["location"]
        assert int(final_resp.headers["upload-offset"]) == len(data1) + len(data2)

        # Verify HEAD on final
        head_resp = await tus_client.head(
            final_loc,
            headers={"Tus-Resumable": "1.0.0"},
        )
        assert head_resp.status_code == 200
        assert head_resp.headers["upload-concat"].startswith("final;")
        assert int(head_resp.headers["upload-offset"]) == len(data1) + len(data2)
        assert int(head_resp.headers["upload-length"]) == len(data1) + len(data2)

    async def test_patch_final_returns_403(self, tus_client) -> None:
        # Create and complete a partial
        data = b"hello"
        resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": str(len(data)),
                "Upload-Concat": "partial",
                "Content-Type": "application/offset+octet-stream",
            },
            content=data,
        )
        loc = resp.headers["location"]

        # Create final
        final_resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Concat": f"final;{loc}",
            },
        )
        final_loc = final_resp.headers["location"]

        # Try to PATCH the final upload
        patch_resp = await tus_client.patch(
            final_loc,
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": "0",
                "Content-Type": "application/offset+octet-stream",
            },
            content=b"data",
        )
        assert patch_resp.status_code == 403

    async def test_final_with_body_returns_403(self, tus_client) -> None:
        data = b"hello"
        resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": str(len(data)),
                "Upload-Concat": "partial",
                "Content-Type": "application/offset+octet-stream",
            },
            content=data,
        )
        loc = resp.headers["location"]

        final_resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Concat": f"final;{loc}",
                "Content-Type": "application/offset+octet-stream",
            },
            content=b"should not be here",
        )
        assert final_resp.status_code == 403

    async def test_final_with_incomplete_partial_returns_400(self, tus_client) -> None:
        # Create partial but don't upload data
        resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": "100",
                "Upload-Concat": "partial",
            },
        )
        loc = resp.headers["location"]

        final_resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Concat": f"final;{loc}",
            },
        )
        assert final_resp.status_code == 400

    async def test_final_referencing_nonexistent_returns_404(self, tus_client) -> None:
        final_resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Concat": "final;/files/nonexistent-id",
            },
        )
        assert final_resp.status_code == 404

    async def test_final_exceeds_max_size_returns_413(self, tus_client) -> None:
        # max_size is 1 MiB from the fixture; create two partials that together exceed it
        size = 600 * 1024  # 600 KiB each, total = 1200 KiB > 1024 KiB
        data = b"x" * size

        locs = []
        for _ in range(2):
            resp = await tus_client.post(
                "/files/",
                headers={
                    "Tus-Resumable": "1.0.0",
                    "Upload-Length": str(size),
                    "Upload-Concat": "partial",
                    "Content-Type": "application/offset+octet-stream",
                },
                content=data,
            )
            assert resp.status_code == 201
            locs.append(resp.headers["location"])

        concat_value = f"final;{locs[0]} {locs[1]}"
        final_resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Concat": concat_value,
            },
        )
        assert final_resp.status_code == 413

    async def test_invalid_upload_concat_header_returns_400(self, tus_client) -> None:
        resp = await tus_client.post(
            "/files/",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Concat": "invalid-value",
            },
        )
        assert resp.status_code == 400
