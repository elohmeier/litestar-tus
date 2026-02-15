# Repository Guidelines

## Project Structure & Module Organization

Source lives under `src/litestar_tus/`, with core API and controller logic in `controller.py`, protocol contracts in `protocols.py`, and storage implementations in `backends/` (e.g., `file.py`, `s3.py`). Tests are in `tests/` and follow a `test_*.py` naming pattern (for example `tests/test_s3_backend.py`). CI workflows live in `.github/workflows/`, and repository-level docs live in `README.md`.

## Build, Test, and Development Commands

This repo uses `uv` to run tools. Key commands mirror CI:

- `uv run ruff format --check .` — verify formatting.
- `uv run ruff check .` — run lint rules.
- `uv run ty check` — run type checks.
- `uv run pytest tests/ -v` — run the full test suite.

When editing `README.md`, run `dprint fmt README.md` to keep formatting consistent.

## Coding Style & Naming Conventions

Python code is formatted with Ruff (PEP 8 compatible). Use 4-space indentation, `snake_case` for functions/variables, and `PascalCase` for classes. Prefer explicit types for dependency-injected arguments and Protocols (`StorageBackend`) to improve typing in Litestar. Keep imports grouped and ordered by Ruff.

## Testing Guidelines

Tests are written with `pytest` (plus `pytest-asyncio`). Async tests should be `async def` and use fixtures from `tests/` or `pytest-databases` where needed. S3 tests rely on the MinIO service fixture provided by `pytest-databases`; ensure Docker is available if you run those locally.

## Commit & Pull Request Guidelines

Use Conventional Commits for all messages (for example: `feat: add S3 storage backend`, `fix: handle NoSuchKey errors`, `chore: update CI workflow`). Keep the subject concise and avoid trailing periods.

Pull requests should include:

- A short description of the change and any behavioral impact.
- Tests run (paste the exact command).
- Links to related issues if applicable.

## Docker Compose Services

`docker-compose.yaml` provides local infrastructure for S3-backend development and performance testing:

- **minio** — S3-compatible object store (MinIO) exposed on ports 9000 (API) and 9001 (console). Default credentials: `minioadmin`/`minioadmin`. Data is persisted in a Docker volume (`minio_data`).
- **minio-init** — One-shot init container that waits for MinIO, configures an `mc` alias, and creates the `bucket` bucket used by tests and the perf server.

Start the services with `docker compose up -d` and wait for the health check on `minio` before running anything that talks to S3.

## Performance Testing

The `perf/` directory contains a self-contained performance test that uploads a large file to the TUS server backed by MinIO.

### Components

- **`perf/server.py`** — Litestar app wired to the S3 backend pointing at `localhost:9000` (MinIO). Stores uploads under the `perf-uploads/` key prefix in `bucket`. Raises `request_max_body_size` to 100 GiB to avoid rejecting large chunks.
- **`perf/upload.mjs`** — Node.js script (uses `tus-js-client` v4) that generates a 5 GiB temp file filled with `0xAA` bytes, then uploads it with 50 MiB chunks and 5 parallel uploads. Prints live progress (percentage, throughput in MiB/s, elapsed time) and cleans up the temp file on completion or error.
- **`perf/package.json`** — Declares the `tus-js-client` dependency; install with `bun install` (or `npm install`) inside `perf/`.

### Running the performance test

1. Start MinIO: `docker compose up -d` (from the repo root).
2. Start the TUS server: `uv run python perf/server.py`.
3. In another terminal, run the upload: `cd perf && bun run upload` (or `node upload.mjs`).

The upload endpoint defaults to `http://localhost:8000/files/` and can be overridden via the `TUS_ENDPOINT` environment variable.

## Protocol Specification

The tus resumable upload protocol is defined at:
https://raw.githubusercontent.com/tus/tus-resumable-upload-protocol/refs/heads/main/protocol.md
