from __future__ import annotations

import os
from typing import Any

from alphavault.logging_config import get_logger
from alphavault.constants import (
    ENV_REDIS_QUEUE_KEY,
    ENV_REDIS_URL,
)


DEFAULT_REDIS_QUEUE_KEY = "alphavault:rss_spool"
logger = get_logger(__name__)


def try_get_redis() -> tuple[Any, str]:
    redis_url = os.getenv(ENV_REDIS_URL, "").strip()
    if not redis_url:
        return None, ""
    try:
        import redis  # type: ignore
    except Exception as err:
        logger.warning("[redis] disabled import_error %s: %s", type(err).__name__, err)
        return None, ""
    try:
        client = redis.Redis.from_url(redis_url, decode_responses=True)
        client.ping()
    except Exception as err:
        logger.warning(
            "[redis] disabled connect_error %s: %s",
            type(err).__name__,
            err,
        )
        return None, ""
    key = os.getenv(ENV_REDIS_QUEUE_KEY, "").strip() or DEFAULT_REDIS_QUEUE_KEY
    return client, key


__all__ = [
    "DEFAULT_REDIS_QUEUE_KEY",
    "try_get_redis",
]
