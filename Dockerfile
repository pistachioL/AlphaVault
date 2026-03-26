# syntax=docker/dockerfile:1.7
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PATH="/app/.venv/bin:/root/.local/bin:${PATH}" \
    PORT=8080

WORKDIR /app

FROM base AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl unzip \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip \
    && python -m pip install uv

COPY pyproject.toml uv.lock /app/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install -p /app/.venv/bin/python gunicorn uvicorn

COPY . /app

RUN uv run reflex export --frontend-only --no-zip
RUN test -d /app/.web/build/client

FROM base AS runtime

ENV __REFLEX_SKIP_COMPILE=true \
    REFLEX_DIR=/root/.local/share/reflex

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl unzip \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app /app
COPY --from=builder /root/.local/share/reflex /root/.local/share/reflex

RUN chmod +x /app/entrypoint.sh

EXPOSE 8080

CMD ["sh", "/app/entrypoint.sh"]
