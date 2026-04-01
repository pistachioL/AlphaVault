from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
import threading
import time

from alphavault.db.turso_db import TursoEngine
from alphavault.db.turso_queue import select_due_post_uids, try_mark_ai_running
from alphavault.worker import periodic_jobs
from alphavault.worker import scheduler
from alphavault.worker.post_processor import process_one_post_uid
from alphavault.worker.redis_payload_runtime import process_one_redis_payload
from alphavault.worker.redis_queue import (
    redis_ai_ack_processing,
    redis_ai_pop_to_processing,
)
from alphavault.worker.turso_runtime import (
    maybe_dispose_turso_engine_on_transient_error,
)
from alphavault.worker.worker_loop_models import SourceTickContext

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _schedule_ai_from_redis(**kwargs):  # type: ignore[no-untyped-def]
    return scheduler.schedule_ai_from_redis(
        **kwargs,
        prune_inflight_futures_fn=periodic_jobs.prune_inflight_futures,
        compute_rss_available_slots_fn=scheduler.compute_rss_available_slots,
        pop_to_processing_fn=redis_ai_pop_to_processing,
        ack_processing_fn=redis_ai_ack_processing,
        process_one_redis_payload_fn=process_one_redis_payload,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
    )


def schedule_ai_for_source(
    *,
    source,
    active_engine: TursoEngine | None,
    platform: str,
    ctx: SourceTickContext,
    ai_executor: ThreadPoolExecutor,
    inflight_futures: set[Future],
    inflight_owner_by_future: dict[Future, str],
    wakeup_event: threading.Event,
) -> bool:
    source_name = str(getattr(source.config, "name", "") or "").strip()
    scheduled, schedule_error = scheduler.schedule_ai(
        executor=ai_executor,
        engine=active_engine,
        platform=platform,
        ai_cap=int(ctx.ai_cap),
        low_inflight_now_get=(
            ctx.low_priority_gate.inflight
            if ctx.low_priority_gate is not None
            else (lambda: 0)
        ),
        inflight_futures=inflight_futures,
        inflight_owner_by_future=inflight_owner_by_future,
        inflight_owner=platform,
        wakeup_event=wakeup_event,
        config=ctx.config,
        limiter=ctx.limiter,
        verbose=ctx.verbose,
        redis_client=ctx.redis_client,
        redis_queue_key=str(source.redis_queue_key or ""),
        source_name=source_name,
        spool_dir=source.spool_dir,
        schedule_ai_from_redis_fn=_schedule_ai_from_redis,
        prune_inflight_futures_fn=periodic_jobs.prune_inflight_futures,
        compute_rss_available_slots_fn=scheduler.compute_rss_available_slots,
        select_due_post_uids_fn=select_due_post_uids,
        dedup_post_uids_fn=scheduler.dedup_post_uids,
        try_mark_ai_running_fn=try_mark_ai_running,
        process_one_post_uid_fn=process_one_post_uid,
        maybe_dispose_turso_engine_on_transient_error_fn=maybe_dispose_turso_engine_on_transient_error,
        now_epoch_fn=lambda: int(time.time()),
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
    )
    _ = scheduled
    if schedule_error and ctx.verbose:
        source_name = str(getattr(source.config, "name", "") or "").strip()
        print(f"[ai:{source_name}] schedule_error=1", flush=True)
    return bool(schedule_error)


__all__ = ["schedule_ai_for_source"]
