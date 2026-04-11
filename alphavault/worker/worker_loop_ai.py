from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
import threading

from alphavault.db.postgres_db import PostgresEngine
from alphavault.worker import periodic_jobs
from alphavault.worker import scheduler
from alphavault.worker.redis_payload_runtime import process_one_redis_payload
from alphavault.worker.redis_stream_queue import (
    build_redis_ai_consumer_name,
    redis_ai_ack,
    redis_ai_claim_stuck_messages,
    redis_ai_consumer_snapshot,
    redis_ai_move_due_retries_to_stream,
    redis_ai_pressure_snapshot,
    redis_ai_read_group_messages,
)
from alphavault.worker.worker_loop_models import SourceTickContext

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _schedule_ai_from_stream(**kwargs):  # type: ignore[no-untyped-def]
    return scheduler.schedule_ai_from_stream(
        **kwargs,
        prune_inflight_futures_fn=periodic_jobs.prune_inflight_futures,
        compute_rss_available_slots_fn=scheduler.compute_rss_available_slots,
        move_due_retry_to_stream_fn=redis_ai_move_due_retries_to_stream,
        claim_stuck_messages_fn=redis_ai_claim_stuck_messages,
        read_group_messages_fn=redis_ai_read_group_messages,
        pressure_snapshot_fn=redis_ai_pressure_snapshot,
        consumer_snapshot_fn=redis_ai_consumer_snapshot,
        ack_message_fn=redis_ai_ack,
        process_one_redis_payload_fn=process_one_redis_payload,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
    )


def schedule_ai_for_source(
    *,
    source,
    active_engine: PostgresEngine | None,
    platform: str,
    ctx: SourceTickContext,
    ai_executor: ThreadPoolExecutor,
    inflight_futures: set[Future],
    inflight_owner_by_future: dict[Future, str],
    wakeup_event: threading.Event,
) -> bool:
    source_name = str(getattr(source.config, "name", "") or "").strip()
    inflight_owner = source_name or str(platform or "").strip()
    consumer_name = build_redis_ai_consumer_name(inflight_owner)
    scheduled, schedule_error = scheduler.schedule_ai(
        executor=ai_executor,
        engine=active_engine,
        platform=platform,
        ai_cap=int(ctx.ai_cap),
        low_inflight_now_get=lambda: 0,
        inflight_futures=inflight_futures,
        inflight_owner_by_future=inflight_owner_by_future,
        inflight_owner=inflight_owner,
        consumer_name=consumer_name,
        wakeup_event=wakeup_event,
        config=ctx.config,
        limiter=ctx.limiter,
        verbose=ctx.verbose,
        redis_client=ctx.redis_client,
        redis_queue_key=str(source.redis_queue_key or ""),
        source_name=source_name,
        stuck_seconds=max(60, int(ctx.stuck_seconds)),
        schedule_ai_from_stream_fn=_schedule_ai_from_stream,
    )
    _ = scheduled
    if schedule_error and ctx.verbose:
        source_name = str(getattr(source.config, "name", "") or "").strip()
        print(f"[ai:{source_name}] schedule_error=1", flush=True)
    return bool(schedule_error)


__all__ = ["schedule_ai_for_source"]
