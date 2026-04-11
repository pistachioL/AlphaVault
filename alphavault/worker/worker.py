"""
Worker entrypoint.

`weibo_rss_worker.py` imports `main()` from here.
"""

from __future__ import annotations

import os
from typing import Optional

from alphavault.constants import (
    DEFAULT_RSS_FEED_SLEEP_SECONDS,
    ENV_RSS_ACTIVE_HOURS,
    ENV_RSS_FEED_SLEEP_SECONDS,
    ENV_RSS_INTERVAL_SECONDS,
)
from alphavault.rss.utils import RateLimiter, env_float, parse_active_hours
from alphavault.logging_config import configure_logging
from alphavault.worker import worker_loop
from alphavault.worker.cli import (
    _parse_worker_active_hours_from_args,
    _resolve_worker_interval_seconds,
    parse_args,
    resolve_rss_source_configs,
)
from alphavault.worker.runtime_models import _build_config


def _resolve_rss_active_hours_from_env() -> Optional[tuple[int, int]]:
    raw_value = os.getenv(ENV_RSS_ACTIVE_HOURS, "").strip()
    if not raw_value:
        return None
    return parse_active_hours(raw_value)


def _resolve_rss_interval_seconds() -> float:
    rss_interval_seconds = env_float(ENV_RSS_INTERVAL_SECONDS)
    if rss_interval_seconds is None or rss_interval_seconds <= 0:
        rss_interval_seconds = 600.0
    return max(1.0, float(rss_interval_seconds))


def _resolve_rss_feed_sleep_seconds() -> float:
    raw_value = env_float(ENV_RSS_FEED_SLEEP_SECONDS)
    if raw_value is None:
        return float(DEFAULT_RSS_FEED_SLEEP_SECONDS)
    return max(0.0, float(raw_value))


def main() -> None:
    args = parse_args()
    configure_logging(level=getattr(args, "log_level", ""))
    source_configs = list(resolve_rss_source_configs(args))

    worker_active_hours = _parse_worker_active_hours_from_args(args)
    worker_interval_seconds = _resolve_worker_interval_seconds(args)

    config = _build_config(args)
    limiter = RateLimiter(config.ai_rpm)
    ai_cap = max(1, int(getattr(args, "ai_max_inflight", 1) or 1))

    rss_active_hours = _resolve_rss_active_hours_from_env()
    rss_interval_seconds = _resolve_rss_interval_seconds()
    rss_feed_sleep_seconds = _resolve_rss_feed_sleep_seconds()

    sources, redis_client, _base_redis_queue_key = worker_loop.build_source_runtimes(
        source_configs=source_configs,
    )
    worker_loop.run_worker_forever(
        args=args,
        sources=sources,
        config=config,
        limiter=limiter,
        ai_cap=ai_cap,
        redis_client=redis_client,
        rss_active_hours=rss_active_hours,
        rss_interval_seconds=rss_interval_seconds,
        rss_feed_sleep_seconds=rss_feed_sleep_seconds,
        worker_active_hours=worker_active_hours,
        worker_interval_seconds=worker_interval_seconds,
    )


__all__ = [
    "main",
]
