from __future__ import annotations

import os
import time
from typing import Any

from alphavault.logging_config import get_logger
from alphavault.constants import (
    ENV_REDIS_QUEUE_KEY,
    ENV_REDIS_URL,
)


DEFAULT_REDIS_QUEUE_KEY = "alphavault:rss_spool"
REDIS_CONNECT_ERROR_RETRY_SECONDS = 60.0
REDIS_SOCKET_TIMEOUT_SECONDS = 2.0

_redis_connect_error_deadline_by_url: dict[str, float] = {}
logger = get_logger(__name__)


def _resolve_redis_queue_key() -> str:
    return os.getenv(ENV_REDIS_QUEUE_KEY, "").strip() or DEFAULT_REDIS_QUEUE_KEY


def _redis_connect_error_is_cached(redis_url: str) -> bool:
    retry_at = _redis_connect_error_deadline_by_url.get(redis_url, 0.0)
    return retry_at > time.monotonic()


def _cache_redis_connect_error(redis_url: str) -> None:
    _redis_connect_error_deadline_by_url[redis_url] = (
        time.monotonic() + REDIS_CONNECT_ERROR_RETRY_SECONDS
    )


def _clear_cached_redis_connect_error(redis_url: str) -> None:
    _redis_connect_error_deadline_by_url.pop(redis_url, None)


def try_get_redis() -> tuple[Any, str]:
    redis_url = os.getenv(ENV_REDIS_URL, "").strip()
    if not redis_url:
        return None, ""
    if _redis_connect_error_is_cached(redis_url):
        return None, ""
    try:
        import redis  # type: ignore
    except Exception as err:
        logger.warning(
            "[redis] disabled import_error %s: %s",
            type(err).__name__,
            err,
        )
        _cache_redis_connect_error(redis_url)
        return None, ""
    try:
        client = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=REDIS_SOCKET_TIMEOUT_SECONDS,
            socket_timeout=REDIS_SOCKET_TIMEOUT_SECONDS,
        )
        client.ping()
    except Exception as err:
        logger.warning(
            "[redis] disabled connect_error %s: %s",
            type(err).__name__,
            err,
        )
        _cache_redis_connect_error(redis_url)
        return None, ""
    _clear_cached_redis_connect_error(redis_url)
    return client, _resolve_redis_queue_key()


__all__ = [
    "DEFAULT_REDIS_QUEUE_KEY",
    "REDIS_CONNECT_ERROR_RETRY_SECONDS",
    "try_get_redis",
]
