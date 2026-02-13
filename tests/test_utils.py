from __future__ import annotations

import base64
from typing import cast

from litestar import Litestar
from litestar.exceptions import ImproperlyConfiguredException
from litestar_tus._utils import (
    encode_metadata,
    generate_upload_id,
    parse_metadata_header,
    safe_emit,
)


class TestGenerateUploadId:
    def test_returns_hex_string(self) -> None:
        uid = generate_upload_id()
        assert len(uid) == 32
        int(uid, 16)  # should not raise

    def test_unique(self) -> None:
        ids = {generate_upload_id() for _ in range(100)}
        assert len(ids) == 100


class TestParseMetadataHeader:
    def test_empty_string(self) -> None:
        assert parse_metadata_header("") == {}

    def test_single_key_value(self) -> None:
        value = base64.b64encode(b"hello.txt").decode()
        result = parse_metadata_header(f"filename {value}")
        assert result == {"filename": b"hello.txt"}

    def test_multiple_pairs(self) -> None:
        fname = base64.b64encode(b"test.pdf").decode()
        ftype = base64.b64encode(b"application/pdf").decode()
        result = parse_metadata_header(f"filename {fname},filetype {ftype}")
        assert result == {"filename": b"test.pdf", "filetype": b"application/pdf"}

    def test_key_without_value(self) -> None:
        result = parse_metadata_header("is_confidential")
        assert result == {"is_confidential": b""}

    def test_mixed_keys(self) -> None:
        fname = base64.b64encode(b"doc.txt").decode()
        result = parse_metadata_header(f"filename {fname},draft")
        assert result == {"filename": b"doc.txt", "draft": b""}


class TestEncodeMetadata:
    def test_roundtrip(self) -> None:
        original = {"filename": b"hello.txt", "type": b"text/plain"}
        encoded = encode_metadata(original)
        decoded = parse_metadata_header(encoded)
        assert decoded == original

    def test_empty(self) -> None:
        assert encode_metadata({}) == ""


class TestSafeEmit:
    def test_no_listeners_is_debug(self, caplog) -> None:
        class App:
            def emit(self, *_args, **_kwargs) -> None:
                raise ImproperlyConfiguredException("no listeners")

        caplog.set_level("DEBUG")

        safe_emit(cast(Litestar, App()), "tus.post_create")

        assert "TUS event handler skipped" in caplog.text

    def test_other_errors_are_logged(self, caplog) -> None:
        class App:
            def emit(self, *_args, **_kwargs) -> None:
                raise RuntimeError("boom")

        caplog.set_level("ERROR")

        safe_emit(cast(Litestar, App()), "tus.post_create")

        assert "TUS event handler failed" in caplog.text
