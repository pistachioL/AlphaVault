from __future__ import annotations

from pathlib import Path
from typing import Any

from alphavault.logging_config import get_logger

logger = get_logger(__name__)


def log_spool_and_redis(
    *,
    spool_dir: Path,
    redis_client: Any,
    redis_queue_key: str,
) -> None:
    logger.debug("[spool] dir=%s", spool_dir)
    if redis_client:
        logger.debug("[redis] enabled key=%s", redis_queue_key)


def build_source_spool_dir(
    *,
    base_spool_dir: Path,
    source_name: str,
    multi_source: bool,
) -> Path:
    path = base_spool_dir if not multi_source else (base_spool_dir / source_name)
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as err:
        logger.error("[spool] dir_error %s %s: %s", path, type(err).__name__, err)
    return path


def build_source_redis_queue_key(
    *,
    base_queue_key: str,
    source_name: str,
    multi_source: bool,
) -> str:
    del multi_source
    resolved = str(base_queue_key or "").strip()
    resolved_source_name = str(source_name or "").strip()
    if not resolved:
        return ""
    if not resolved_source_name:
        return resolved
    return f"{resolved}:{resolved_source_name}"


def log_source_runtime(
    *,
    source: Any,
    redis_client: Any,
    rss_interval_seconds: float,
    rss_feed_sleep_seconds: float,
) -> None:
    cfg = getattr(source, "config", None)
    name = str(getattr(cfg, "name", "") or "").strip()
    platform = str(getattr(cfg, "platform", "") or "").strip()
    rss_urls = getattr(cfg, "rss_urls", None)
    database_url = str(getattr(cfg, "database_url", "") or "").strip()
    spool_dir = getattr(source, "spool_dir", None)
    redis_queue_key = str(getattr(source, "redis_queue_key", "") or "").strip()
    rss_count = len(rss_urls) if isinstance(rss_urls, list) else 0
    logger.debug(
        "[source] name=%s platform=%s rss=%s rss_interval=%ss rss_feed_sleep=%.1fs db=%s",
        name,
        platform,
        rss_count,
        int(max(1.0, float(rss_interval_seconds))),
        float(max(0.0, float(rss_feed_sleep_seconds))),
        database_url,
    )
    if isinstance(spool_dir, Path):
        log_spool_and_redis(
            spool_dir=spool_dir,
            redis_client=redis_client,
            redis_queue_key=redis_queue_key,
        )


__all__ = [
    "build_source_redis_queue_key",
    "build_source_spool_dir",
    "log_source_runtime",
    "log_spool_and_redis",
]
