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
- **Extensions**: creation, creation-with-upload, termination, expiration, checksum
- **Storage backends**: local filesystem (default) and S3 (via boto3)
- **Concurrency safety**: POSIX file locks (file backend) and S3 conditional writes via ETags (S3 backend)
- **Checksum verification**: streaming SHA-1, SHA-256, and MD5 validation
- **Lifecycle events**: hook into upload creation, progress, completion, and termination via Litestar's event system
- **Streaming**: request bodies are streamed directly to storage — the S3 backend uses a rolling buffer that flushes multipart parts incrementally without buffering the full upload in memory

## Configuration

```python
TUSConfig(
    path_prefix="/uploads",       # URL prefix for TUS endpoints
    upload_dir="./uploads",       # Local storage directory (file backend)
    max_size=1024 * 1024 * 100,   # Maximum upload size in bytes (optional)
    expiration_seconds=86400,     # Upload expiration in seconds (default: 24h, None to disable)
    extensions=(                  # Protocol extensions to enable
        "creation",
        "creation-with-upload",
        "termination",
        "expiration",
        "checksum",
    ),
    storage_backend=None,         # Custom StorageBackend instance (default: FileStorageBackend)
    metadata_override=None,       # Optional hook to override Upload-Metadata based on the Request
)
```

## Storage Backends

### File Backend (default)

Stores uploads on the local filesystem under `upload_dir`. Each upload produces three files:

| File        | Purpose                                        |
| ----------- | ---------------------------------------------- |
| `<id>`      | Upload data                                    |
| `<id>.info` | JSON metadata (offset, size, expiration, etc.) |
| `<id>.lock` | POSIX advisory lock file                       |

Concurrency is handled with `fcntl.flock` — the lock file is acquired exclusively before every write, and metadata is re-read under the lock to prevent TOCTOU races.

**Limitations:** `fcntl.flock` is POSIX-only (Linux/macOS) and only guarantees exclusive access on a single node. NFS and other network filesystems do not reliably support `fcntl` advisory locks. For multi-worker or multi-node deployments, use the S3 backend instead.

### S3 Backend

Uses S3 multipart uploads with a rolling buffer to stream data into parts without full-stream buffering.

```python
import boto3
from litestar_tus import TUSConfig, TUSPlugin
from litestar_tus.backends.s3 import S3StorageBackend

s3_client = boto3.client("s3")
backend = S3StorageBackend(
    client=s3_client,
    bucket="my-bucket",
    key_prefix="uploads/",
    part_size=10 * 1024 * 1024,  # 10 MiB (default), minimum 5 MiB
)

app = Litestar(
    plugins=[TUSPlugin(TUSConfig(storage_backend=backend))]
)
```

Each upload produces these S3 objects:

| Object                 | Purpose                                              |
| ---------------------- | ---------------------------------------------------- |
| `<prefix><id>`         | Assembled upload data (after multipart completion)   |
| `<prefix><id>.info`    | JSON metadata                                        |
| `<prefix><id>.pending` | Temporary buffer for bytes not yet flushed as a part |

#### Rolling Buffer

Incoming data accumulates in a buffer. Whenever the buffer reaches `part_size`, a multipart part is flushed to S3. Leftover bytes smaller than `part_size` are persisted as a `.pending` object and prepended to the buffer on the next `write_chunk` call. On `finish()`, any remaining pending data is flushed as the final part and `complete_multipart_upload` is called.

#### Optimistic Concurrency Control

The S3 backend uses two layers of concurrency protection:

1. **Process-local `anyio.Lock`** — serializes concurrent writes to the same upload within a single worker process, avoiding unnecessary S3 round-trips.
2. **S3 conditional writes via ETags** — provides cross-process and cross-node safety. The ETag of the `.info` object is tracked and passed as `IfMatch` on every `put_object` call. If another process modified the `.info` object in the meantime, S3 returns `412 Precondition Failed` and the write is rejected with HTTP 409. New uploads use `IfNoneMatch: *` to prevent duplicate creation.

This means the S3 backend is safe to run with multiple worker processes without sticky sessions or external locks.

## Checksum Verification

When the `checksum` extension is enabled (default), clients can send an `Upload-Checksum` header with PATCH or creation-with-upload requests:

```
Upload-Checksum: sha256 <base64-encoded-digest>
```

Supported algorithms: `sha1`, `sha256`, `md5`.

The digest is computed incrementally as data streams through — no extra buffering pass required. A mismatch returns HTTP 460 per the TUS protocol specification.

## Expiration

When `expiration_seconds` is set (default: 86400 / 24 hours), each upload receives an `expires_at` timestamp. Expired uploads are rejected with HTTP 410 (Gone) on HEAD, PATCH, and DELETE requests. The `Upload-Expires` header is included in responses so clients know the deadline.

Note: expired uploads are not automatically cleaned up from storage. Implement a background job or use S3 lifecycle rules to remove stale objects.

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

Available events:

| Event            | When                                      |
| ---------------- | ----------------------------------------- |
| `PRE_CREATE`     | Before upload is created                  |
| `POST_CREATE`    | After upload is created                   |
| `POST_RECEIVE`   | After a data chunk is written             |
| `PRE_FINISH`     | Before completing (assembling) the upload |
| `POST_FINISH`    | After the upload is completed             |
| `PRE_TERMINATE`  | Before deleting an upload                 |
| `POST_TERMINATE` | After an upload is deleted                |

All events receive `upload_info: UploadInfo` as a keyword argument.

## Metadata Override

Override or inject `Upload-Metadata` using the incoming request before the upload is created:

```python
from litestar import Request
from litestar_tus import TUSConfig, TUSPlugin

async def metadata_override(request: Request, metadata: dict[str, bytes]) -> dict[str, bytes]:
    metadata["user_id"] = request.headers.get("authorization", "").encode()
    return metadata

app = Litestar(
    plugins=[TUSPlugin(TUSConfig(metadata_override=metadata_override))],
)
```

## License

MIT
