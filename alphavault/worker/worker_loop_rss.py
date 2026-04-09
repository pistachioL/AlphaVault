from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import threading

from alphavault.db.postgres_db import PostgresEngine
from alphavault.rss.utils import CST, in_active_hours
from alphavault.worker.ingest import ingest_rss_many_once
from alphavault.worker.worker_loop_models import SourceTickContext
from alphavault.worker.worker_loop_runtime import seconds_until_next_active_start


def maybe_schedule_rss_ingest(
    *,
    source,
    active_engine: PostgresEngine | None,
    platform: str,
    ctx: SourceTickContext,
    rss_executor: ThreadPoolExecutor,
    wakeup_event: threading.Event,
) -> None:
    if active_engine is None or not source.config.rss_urls:
        return
    if getattr(source, "rss_ingest_future", None) is not None:
        return
    next_ingest_at = float(getattr(source, "rss_next_ingest_at", 0.0) or 0.0)
    if ctx.now < next_ingest_at:
        return

    now_dt = datetime.now(CST)
    if ctx.rss_active_hours is not None and not in_active_hours(
        now_dt, ctx.rss_active_hours
    ):
        source.rss_next_ingest_at = ctx.now + seconds_until_next_active_start(
            now_dt,
            ctx.rss_active_hours,
        )
        return

    source.rss_ingest_future = rss_executor.submit(
        ingest_rss_many_once,
        rss_urls=source.config.rss_urls,
        engine=active_engine,
        spool_dir=source.spool_dir,
        redis_client=ctx.redis_client,
        redis_queue_key=source.redis_queue_key,
        platform=platform,
        author=source.config.author,
        user_id=source.config.user_id,
        limit=ctx.limit_or_none,
        rss_timeout=float(getattr(ctx.args, "rss_timeout", 60.0) or 60.0),
        rss_retries=int(getattr(ctx.args, "rss_retries", 5) or 5),
        rss_feed_sleep_seconds=float(ctx.rss_feed_sleep_seconds),
        verbose=ctx.verbose,
    )
    source.rss_ingest_future.add_done_callback(lambda _f: wakeup_event.set())


__all__ = ["maybe_schedule_rss_ingest"]
