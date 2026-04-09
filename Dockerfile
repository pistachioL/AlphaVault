# syntax=docker/dockerfile:1.7
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PATH="/app/.venv/bin:/root/.local/bin:${PATH}" \
    WEB_CONCURRENCY=1 \
    PORT=8080

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/*

FROM base AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl unzip \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip \
    && python -m pip install uv

COPY pyproject.toml uv.lock /app/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project \
    && uv pip install -p /app/.venv/bin/python gunicorn "uvicorn[standard]" \
    && uv cache prune --ci

COPY . /app

RUN uv run reflex export --frontend-only --no-zip \
    && test -d /app/.web/build/client \
    && rm -rf /app/.web/node_modules \
        /app/.web/.cache \
        /app/.web/.next \
        /app/.web/.turbo \
        /app/.web/.vite

FROM base AS runtime

ENV __REFLEX_SKIP_COMPILE=true

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/.web /app/.web
COPY --from=builder /app/alphavault /app/alphavault
COPY --from=builder /app/alphavault_reflex /app/alphavault_reflex
COPY --from=builder /app/entrypoint.sh /app/entrypoint.sh
COPY --from=builder /app/supervisord.conf /app/supervisord.conf
COPY --from=builder /app/startup_healthcheck.py /app/startup_healthcheck.py
COPY --from=builder /app/weibo_rss_turso_worker.py /app/weibo_rss_turso_worker.py
COPY --from=builder /app/rxconfig.py /app/rxconfig.py

RUN chmod +x /app/entrypoint.sh

EXPOSE 8080

CMD ["sh", "/app/entrypoint.sh"]
