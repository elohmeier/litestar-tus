FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --extra server --no-install-project --no-dev

COPY . .
RUN uv sync --locked --extra server --no-dev


FROM python:3.13-slim-bookworm

WORKDIR /app

RUN groupadd --gid 1000 tus && \
    useradd --uid 1000 --gid tus --create-home tus

COPY --from=builder /app /app

ENV PATH="/app/.venv/bin:$PATH"

USER tus

EXPOSE 8080

CMD ["litestar-tus"]
