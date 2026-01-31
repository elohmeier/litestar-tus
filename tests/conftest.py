from __future__ import annotations

from pathlib import Path

import pytest
from litestar import Litestar
from litestar.testing import AsyncTestClient

from litestar_tus import TUSConfig, TUSPlugin
from litestar_tus.backends.file import FileStorageBackend


@pytest.fixture()
def upload_dir(tmp_path: Path) -> Path:
    d = tmp_path / "uploads"
    d.mkdir()
    return d


@pytest.fixture()
def file_backend(upload_dir: Path) -> FileStorageBackend:
    return FileStorageBackend(upload_dir)


@pytest.fixture()
def tus_config(upload_dir: Path) -> TUSConfig:
    return TUSConfig(upload_dir=upload_dir, path_prefix="/files", max_size=1024 * 1024)


@pytest.fixture()
def tus_app(tus_config: TUSConfig) -> Litestar:
    return Litestar(plugins=[TUSPlugin(tus_config)])


@pytest.fixture()
async def tus_client(tus_app: Litestar):
    async with AsyncTestClient(tus_app) as client:
        yield client
