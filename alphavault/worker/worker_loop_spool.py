from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading
from typing import Any

from alphavault.db.postgres_db import PostgresEngine
from alphavault.worker import periodic_jobs
from alphavault.worker.spool import flush_spool_to_turso
from alphavault.worker.worker_constants import (
    SPOOL_FLUSH_MAX_ITEMS_PER_RUN,
    SPOOL_FLUSH_RETRY_INTERVAL_SECONDS,
)
from alphavault.worker.worker_loop_models import SourceTickContext


def submit_spool_flush_job(
    sync_engine: PostgresEngine,
    *,
    spool_dir: Path,
    redis_client: Any,
    redis_queue_key: str,
    verbose: bool,
) -> dict[str, int | bool]:
    flushed, has_error = flush_spool_to_turso(
        spool_dir=spool_dir,
        engine=sync_engine,
        max_items=int(SPOOL_FLUSH_MAX_ITEMS_PER_RUN),
        verbose=bool(verbose),
        redis_client=redis_client,
        redis_queue_key=redis_queue_key,
        delete_spool_on_redis_push=False,
    )
    has_more = bool(
        (not has_error) and int(flushed) >= int(SPOOL_FLUSH_MAX_ITEMS_PER_RUN)
    )
    return {
        "flushed": int(flushed),
        "has_error": bool(has_error),
        "has_more": bool(has_more),
    }


def maybe_schedule_spool_flush(
    *,
    source,
    active_engine: PostgresEngine | None,
    ctx: SourceTickContext,
    spool_executor: ThreadPoolExecutor,
    wakeup_event: threading.Event,
) -> None:
    if active_engine is None:
        return
    if getattr(source, "spool_flush_future", None) is not None:
        return
    if not periodic_jobs.should_start_spool_flush(source=source):
        return
    if ctx.now < float(getattr(source, "spool_flush_next_at", 0.0) or 0.0):
        return

    periodic_jobs.mark_spool_flush_started(source=source)
    source.spool_flush_future = spool_executor.submit(
        submit_spool_flush_job,
        active_engine,
        spool_dir=source.spool_dir,
        redis_client=ctx.redis_client,
        redis_queue_key=str(source.redis_queue_key or ""),
        verbose=ctx.verbose,
    )
    source.spool_flush_future.add_done_callback(lambda _f: wakeup_event.set())
    source.spool_flush_next_at = ctx.now + float(SPOOL_FLUSH_RETRY_INTERVAL_SECONDS)


__all__ = [
    "maybe_schedule_spool_flush",
    "submit_spool_flush_job",
]
