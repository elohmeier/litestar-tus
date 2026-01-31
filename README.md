# litestar-tus

[![CI](https://github.com/elohmeier/litestar-tus/actions/workflows/ci.yaml/badge.svg)](https://github.com/elohmeier/litestar-tus/actions/workflows/ci.yaml)
[![Publish to PyPI](https://github.com/elohmeier/litestar-tus/actions/workflows/pypi.yaml/badge.svg)](https://github.com/elohmeier/litestar-tus/actions/workflows/pypi.yaml)
[![PyPI version](https://img.shields.io/pypi/v/litestar-tus)](https://pypi.org/project/litestar-tus/)

[TUS v1.0.0](https://tus.io/protocols/resumable-upload) resumable upload protocol plugin for [Litestar](https://litestar.dev) with pluggable storage backends.

## Installation

```bash
pip install litestar-tus

# With S3 support
pip install litestar-tus[s3]
```

## Quick Start

```python
from litestar import Litestar
from litestar_tus import TUSPlugin, TUSConfig

app = Litestar(
    plugins=[TUSPlugin(TUSConfig(path_prefix="/uploads", max_size=5 * 1024**3))]
)
```

This registers TUS protocol endpoints at `/uploads/` supporting resumable file uploads.

## Features

- **TUS v1.0.0** protocol compliance
- **Extensions**: creation, creation-with-upload, termination, expiration
- **Storage backends**: local filesystem (default) and S3 (via boto3)
- **Lifecycle events**: hook into upload creation, progress, completion, and termination via Litestar's event system
- **Streaming**: request bodies are streamed directly to storage without buffering

## Configuration

```python
TUSConfig(
    path_prefix="/uploads",       # URL prefix for TUS endpoints
    upload_dir="./uploads",       # Local storage directory (when using file backend)
    max_size=1024 * 1024 * 100,   # Maximum upload size in bytes
    expiration_seconds=86400,     # Upload expiration (default: 24h)
)
```

### S3 Backend

```python
import boto3
from litestar_tus import TUSConfig, TUSPlugin
from litestar_tus.backends.s3 import S3StorageBackend

s3_client = boto3.client("s3")
backend = S3StorageBackend(client=s3_client, bucket="my-bucket", key_prefix="uploads/")

app = Litestar(
    plugins=[TUSPlugin(TUSConfig(storage_backend=backend))]
)
```

## Events

Listen to upload lifecycle events:

```python
from litestar.events import listener
from litestar_tus import TUSEvent, UploadInfo

@listener(TUSEvent.POST_FINISH)
async def on_upload_complete(upload_info: UploadInfo) -> None:
    print(f"Upload {upload_info.id} completed ({upload_info.offset} bytes)")

app = Litestar(
    plugins=[TUSPlugin()],
    listeners=[on_upload_complete],
)
```

Available events: `PRE_CREATE`, `POST_CREATE`, `POST_RECEIVE`, `PRE_FINISH`, `POST_FINISH`, `PRE_TERMINATE`, `POST_TERMINATE`.

## License

MIT
