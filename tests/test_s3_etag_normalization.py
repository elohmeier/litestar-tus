from __future__ import annotations

import anyio
import pytest
from botocore.exceptions import ClientError
from litestar_tus.backends.s3 import S3Upload
from litestar_tus.models import UploadInfo


class _DummyS3Client:
    def __init__(self) -> None:
        self.last_put_kwargs: dict[str, object] | None = None

    def put_object(self, **kwargs):  # type: ignore[no-untyped-def]
        self.last_put_kwargs = kwargs
        return {"ETag": '"etag-after-save"'}


async def test_save_info_strips_quotes_from_if_match_etag() -> None:
    client = _DummyS3Client()
    upload = S3Upload(
        UploadInfo(id="etag-normalization", size=1),
        client=client,
        bucket="test-bucket",
        key_prefix="uploads/",
        lock=anyio.Lock(),
        part_size=5 * 1024 * 1024,
        info_etag='"quoted-etag"',
    )

    await upload._save_info()

    assert client.last_put_kwargs is not None
    assert client.last_put_kwargs["IfMatch"] == "quoted-etag"


async def test_save_info_treats_precondition_failed_as_concurrent_modification() -> (
    None
):
    class _FailingDummyS3Client(_DummyS3Client):
        def put_object(self, **kwargs):  # type: ignore[no-untyped-def]
            self.last_put_kwargs = kwargs
            raise ClientError(
                {
                    "Error": {
                        "Code": "PreconditionFailed",
                    }
                },
                "PutObject",
            )

    upload = S3Upload(
        UploadInfo(id="etag-precondition", size=1),
        client=_FailingDummyS3Client(),
        bucket="test-bucket",
        key_prefix="uploads/",
        lock=anyio.Lock(),
        part_size=5 * 1024 * 1024,
        info_etag='"quoted-etag"',
    )

    with pytest.raises(ValueError, match="Concurrent modification detected"):
        await upload._save_info()
