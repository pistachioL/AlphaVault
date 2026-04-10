from __future__ import annotations

import os
from typing import Any

from alphavault.constants import (
    ENV_REDIS_QUEUE_KEY,
    ENV_REDIS_URL,
)


DEFAULT_REDIS_QUEUE_KEY = "alphavault:rss_spool"


def try_get_redis() -> tuple[Any, str]:
    redis_url = os.getenv(ENV_REDIS_URL, "").strip()
    if not redis_url:
        return None, ""
    try:
        import redis  # type: ignore
    except Exception as err:
        print(f"[redis] disabled import_error {type(err).__name__}: {err}", flush=True)
        return None, ""
    try:
        client = redis.Redis.from_url(redis_url, decode_responses=True)
        client.ping()
    except Exception as err:
        print(f"[redis] disabled connect_error {type(err).__name__}: {err}", flush=True)
        return None, ""
    key = os.getenv(ENV_REDIS_QUEUE_KEY, "").strip() or DEFAULT_REDIS_QUEUE_KEY
    return client, key


__all__ = [
    "DEFAULT_REDIS_QUEUE_KEY",
    "try_get_redis",
]
