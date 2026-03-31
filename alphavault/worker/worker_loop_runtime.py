from __future__ import annotations

from concurrent.futures import Future
from datetime import datetime, timedelta
import os
from typing import Any

from alphavault.constants import (
    ENV_WORKER_STOCK_ALIAS_SYNC_INTERVAL_SECONDS,
    ENV_WORKER_STOCK_HOT_CACHE_INTERVAL_SECONDS,
)
from alphavault.db.turso_db import ensure_turso_engine
from alphavault.worker.cli import RSSSourceConfig
from alphavault.worker.redis_queue import try_get_redis
from alphavault.worker.runtime_models import WorkerSourceConfig, WorkerSourceRuntime
from alphavault.worker.source_runtime import (
    build_source_redis_queue_key,
    build_source_spool_dir,
)
from alphavault.worker.spool import ensure_spool_dir


def seconds_until_next_active_start(
    now_dt: datetime,
    active_hours: tuple[int, int],
) -> float:
    start_hour, end_hour = active_hours
    today_start = now_dt.replace(hour=start_hour, minute=0, second=0, microsecond=0)

    if start_hour <= end_hour:
        next_dt = (
            today_start if now_dt.hour < start_hour else today_start + timedelta(days=1)
        )
    else:
        next_dt = today_start

    return max(1.0, (next_dt - now_dt).total_seconds())


def resolve_stock_alias_sync_interval_seconds() -> float:
    raw_value = os.getenv(ENV_WORKER_STOCK_ALIAS_SYNC_INTERVAL_SECONDS, "").strip()
    if not raw_value:
        return 1800.0
    try:
        seconds = float(raw_value)
    except Exception:
        return 1800.0
    return max(60.0, seconds)


def resolve_stock_hot_cache_interval_seconds() -> float:
    raw_value = os.getenv(ENV_WORKER_STOCK_HOT_CACHE_INTERVAL_SECONDS, "").strip()
    if not raw_value:
        return 60.0
    try:
        seconds = float(raw_value)
    except Exception:
        return 60.0
    return max(15.0, seconds)


def build_source_runtimes(
    *,
    source_configs: list[RSSSourceConfig],
) -> tuple[list[WorkerSourceRuntime], Any, str]:
    base_spool_dir = ensure_spool_dir()
    redis_client, base_redis_queue_key = try_get_redis()

    multi_source = len(source_configs) > 1
    sources: list[WorkerSourceRuntime] = []
    for cfg in source_configs:
        config = WorkerSourceConfig(
            name=cfg.name,
            platform=cfg.platform,
            rss_urls=list(cfg.rss_urls),
            author=cfg.author,
            user_id=cfg.user_id,
            database_url=cfg.database_url,
            auth_token=cfg.auth_token,
        )
        runtime = WorkerSourceRuntime(
            config=config,
            engine=ensure_turso_engine(config.database_url, config.auth_token),
            spool_dir=build_source_spool_dir(
                base_spool_dir=base_spool_dir,
                source_name=config.name,
                multi_source=multi_source,
            ),
            redis_queue_key=build_source_redis_queue_key(
                base_queue_key=base_redis_queue_key,
                source_name=config.name,
                multi_source=multi_source,
            ),
            rss_next_ingest_at=0.0 if config.rss_urls else float("inf"),
        )
        sources.append(runtime)

    return sources, redis_client, str(base_redis_queue_key or "")


def rss_inflight_now(inflight_futures: set[Future]) -> int:
    try:
        snapshot = tuple(inflight_futures)
    except RuntimeError:
        snapshot = tuple(inflight_futures)
    return sum(1 for fut in snapshot if not fut.done())


__all__ = [
    "build_source_runtimes",
    "resolve_stock_alias_sync_interval_seconds",
    "resolve_stock_hot_cache_interval_seconds",
    "rss_inflight_now",
    "seconds_until_next_active_start",
]
