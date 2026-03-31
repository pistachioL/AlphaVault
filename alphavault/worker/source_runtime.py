from __future__ import annotations

from pathlib import Path
from typing import Any


def log_spool_and_redis(
    *,
    verbose: bool,
    spool_dir: Path,
    redis_client: Any,
    redis_queue_key: str,
) -> None:
    if not verbose:
        return
    print(f"[spool] dir={spool_dir}", flush=True)
    if redis_client:
        print(f"[redis] enabled key={redis_queue_key}", flush=True)


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
        print(f"[spool] dir_error {path} {type(err).__name__}: {err}", flush=True)
    return path


def build_source_redis_queue_key(
    *,
    base_queue_key: str,
    source_name: str,
    multi_source: bool,
) -> str:
    resolved = str(base_queue_key or "").strip()
    if not resolved:
        return ""
    if not multi_source:
        return resolved
    return f"{resolved}:{str(source_name or '').strip()}"


def log_source_runtime(
    *,
    verbose: bool,
    source: Any,
    redis_client: Any,
    rss_interval_seconds: float,
    rss_feed_sleep_seconds: float,
) -> None:
    if not verbose:
        return
    cfg = getattr(source, "config", None)
    name = str(getattr(cfg, "name", "") or "").strip()
    platform = str(getattr(cfg, "platform", "") or "").strip()
    rss_urls = getattr(cfg, "rss_urls", None)
    database_url = str(getattr(cfg, "database_url", "") or "").strip()
    spool_dir = getattr(source, "spool_dir", None)
    redis_queue_key = str(getattr(source, "redis_queue_key", "") or "").strip()
    rss_count = len(rss_urls) if isinstance(rss_urls, list) else 0
    print(
        f"[source] name={name} platform={platform} rss={rss_count} "
        f"rss_interval={int(max(1.0, float(rss_interval_seconds)))}s "
        f"rss_feed_sleep={float(max(0.0, float(rss_feed_sleep_seconds))):.1f}s "
        f"db={database_url}",
        flush=True,
    )
    if isinstance(spool_dir, Path):
        log_spool_and_redis(
            verbose=verbose,
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
