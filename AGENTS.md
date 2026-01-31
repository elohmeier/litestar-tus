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

## Protocol Specification

The tus resumable upload protocol is defined at:
https://raw.githubusercontent.com/tus/tus-resumable-upload-protocol/refs/heads/main/protocol.md
