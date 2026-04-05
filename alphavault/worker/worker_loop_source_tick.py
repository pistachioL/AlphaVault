from __future__ import annotations

import time

from alphavault.rss.utils import CST
from alphavault.worker.assertion_outbox_consumer import rebuild_local_cache_from_outbox
from alphavault.worker import cycle_runner
from alphavault.worker import maintenance
from alphavault.worker import periodic_jobs
from alphavault.worker.job_state import WORKER_PROGRESS_STAGE_CYCLE
from alphavault.worker.progress_state import save_worker_progress_state
from alphavault.worker.redis_queue import redis_ai_move_due_delayed_to_ready
from alphavault.worker.worker_constants import REDIS_AI_DUE_MAINTENANCE_MAX_ITEMS
from alphavault.worker.worker_loop_ai import schedule_ai_for_source
from alphavault.worker.worker_loop_jobs import collect_finished_jobs
from alphavault.worker.worker_loop_low_priority import schedule_low_priority_jobs
from alphavault.worker.worker_loop_maintenance import run_maintenance_if_due
from alphavault.worker.worker_loop_models import (
    SourceTickContext,
    SourceTickExecutors,
    SourceTickState,
)
from alphavault.worker.worker_loop_redis_enqueue import maybe_schedule_redis_enqueue
from alphavault.worker.worker_loop_rss import maybe_schedule_rss_ingest
from alphavault.worker.worker_loop_spool import maybe_schedule_spool_flush
from alphavault.worker.worker_loop_turso import ensure_source_turso_ready

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
LOCAL_CACHE_REBUILD_RETRY_SECONDS = 300.0


def _resolve_source_identity(source) -> tuple[str, str]:
    source_name = str(source.config.name or "").strip()
    platform = str(source.config.platform or "").strip()
    return source_name, platform


def _run_redis_due_maintenance(*, source, ctx: SourceTickContext) -> None:
    maintenance.maybe_run_redis_due_maintenance(
        source=source,
        redis_client=ctx.redis_client,
        worker_interval_seconds=float(ctx.worker_interval_seconds),
        verbose=ctx.verbose,
        now_fn=time.time,
        move_due_delayed_to_ready_fn=redis_ai_move_due_delayed_to_ready,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        max_items=int(REDIS_AI_DUE_MAINTENANCE_MAX_ITEMS),
    )


def _schedule_rss_and_spool(
    *,
    source,
    active_engine,
    ctx: SourceTickContext,
    execs: SourceTickExecutors,
    state: SourceTickState,
) -> None:
    platform = str(source.config.platform or "").strip()
    maybe_schedule_rss_ingest(
        source=source,
        active_engine=active_engine,
        platform=platform,
        ctx=ctx,
        rss_executor=execs.rss_executor,
        wakeup_event=state.wakeup_event,
    )
    maybe_schedule_spool_flush(
        source=source,
        active_engine=active_engine,
        ctx=ctx,
        spool_executor=execs.spool_executor,
        wakeup_event=state.wakeup_event,
    )
    maybe_schedule_redis_enqueue(
        source=source,
        ctx=ctx,
        redis_enqueue_executor=execs.redis_enqueue_executor,
        wakeup_event=state.wakeup_event,
    )


def _collect_source_finished_jobs(
    *,
    source,
    source_name: str,
    ctx: SourceTickContext,
) -> tuple[dict[str, bool], bool]:
    return collect_finished_jobs(
        source=source,
        source_name=source_name,
        now=float(ctx.now),
        verbose=ctx.verbose,
        rss_interval_seconds=float(ctx.rss_interval_seconds),
    )


def _ensure_active_engine(
    *,
    source,
    source_name: str,
    ctx: SourceTickContext,
):
    return ensure_source_turso_ready(
        source=source,
        source_name=source_name,
        now=float(ctx.now),
        verbose=ctx.verbose,
    )


def _ensure_local_cache_ready(
    *,
    source,
    active_engine,
    source_name: str,
    ctx: SourceTickContext,
) -> None:
    if active_engine is None:
        return
    if bool(getattr(source, "local_cache_ready", False)):
        return
    next_at = float(getattr(source, "local_cache_rebuild_next_at", 0.0) or 0.0)
    if float(ctx.now) < next_at:
        return
    stats = rebuild_local_cache_from_outbox(
        active_engine,
        source_name=str(source_name or "").strip(),
        verbose=ctx.verbose,
    )
    if not bool(stats.get("has_error", False)):
        source.local_cache_ready = True
        source.local_cache_rebuild_next_at = 0.0
        return
    source.local_cache_rebuild_next_at = float(ctx.now) + float(
        LOCAL_CACHE_REBUILD_RETRY_SECONDS
    )


def _run_source_maintenance(
    *,
    source,
    source_name: str,
    active_engine,
    ctx: SourceTickContext,
) -> bool:
    platform = str(source.config.platform or "").strip()
    return run_maintenance_if_due(
        source=source,
        active_engine=active_engine,
        source_name=source_name,
        platform=platform,
        ctx=ctx,
    )


def _run_source_ai_schedule(
    *,
    source,
    active_engine,
    ctx: SourceTickContext,
    execs: SourceTickExecutors,
    state: SourceTickState,
) -> bool:
    platform = str(source.config.platform or "").strip()
    return schedule_ai_for_source(
        source=source,
        active_engine=active_engine,
        platform=platform,
        ctx=ctx,
        ai_executor=execs.ai_executor,
        inflight_futures=state.inflight_futures,
        inflight_owner_by_future=state.inflight_owner_by_future,
        wakeup_event=state.wakeup_event,
    )


def _run_source_low_priority(
    *,
    source,
    active_engine,
    ctx: SourceTickContext,
    execs: SourceTickExecutors,
    state: SourceTickState,
) -> None:
    schedule_low_priority_jobs(
        source=source,
        active_engine=active_engine,
        ctx=ctx,
        execs=execs,
        wakeup_event=state.wakeup_event,
        inflight_futures=state.inflight_futures,
    )


def _any_inflight(*, source, state: SourceTickState) -> bool:
    return bool(
        cycle_runner.should_wait_with_event(
            ai_inflight=bool(state.inflight_futures),
            any_alias_inflight=bool(
                getattr(source, "alias_sync_future", None) is not None
            ),
            any_backfill_inflight=bool(
                getattr(source, "backfill_cache_future", None) is not None
            ),
            any_relation_inflight=bool(
                getattr(source, "relation_cache_future", None) is not None
            ),
            any_stock_hot_inflight=bool(
                getattr(source, "stock_hot_cache_future", None) is not None
            ),
            any_redis_enqueue_inflight=bool(
                getattr(source, "redis_enqueue_future", None) is not None
            ),
            any_rss_inflight=bool(
                getattr(source, "rss_ingest_future", None) is not None
            ),
            any_spool_flush_inflight=bool(
                getattr(source, "spool_flush_future", None) is not None
            ),
        )
    )


def _save_cycle_progress(
    *,
    source,
    ctx: SourceTickContext,
    any_inflight: bool,
    rss_enqueue_error: bool,
    errors: dict[str, bool],
) -> None:
    next_run_at = periodic_jobs.format_epoch_to_cst(
        ctx.maintenance_next_at,
        cst_tz=CST,
    )
    source_turso_error = cycle_runner.build_source_turso_error(
        maintenance_error=bool(errors["maintenance_error"]),
        spool_flush_error=bool(errors["spool_flush_error"]),
        schedule_error=bool(errors["schedule_error"]),
        alias_sync_error=bool(errors["alias_sync_error"]),
        backfill_cache_error=bool(errors["backfill_cache_error"]),
        relation_cache_error=bool(errors["relation_cache_error"]),
        stock_hot_error=bool(errors["stock_hot_error"]),
    )
    save_worker_progress_state(
        source=source,
        stage=WORKER_PROGRESS_STAGE_CYCLE,
        payload={
            "status": "running" if any_inflight else "idle",
            "running": bool(any_inflight),
            "next_run_at": next_run_at,
            "turso_error": bool(source_turso_error),
            "rss_error": bool(rss_enqueue_error or errors["redis_enqueue_error"]),
        },
        verbose=ctx.verbose,
    )


def run_source_tick(
    *,
    source,
    ctx: SourceTickContext,
    execs: SourceTickExecutors,
    state: SourceTickState,
) -> bool:
    source_name, _platform = _resolve_source_identity(source)
    active_engine = _ensure_active_engine(
        source=source, source_name=source_name, ctx=ctx
    )
    _ensure_local_cache_ready(
        source=source,
        active_engine=active_engine,
        source_name=source_name,
        ctx=ctx,
    )
    errors, rss_enqueue_error = _collect_source_finished_jobs(
        source=source, source_name=source_name, ctx=ctx
    )
    _run_redis_due_maintenance(source=source, ctx=ctx)
    _schedule_rss_and_spool(
        source=source,
        active_engine=active_engine,
        ctx=ctx,
        execs=execs,
        state=state,
    )
    errors["maintenance_error"] = _run_source_maintenance(
        source=source,
        source_name=source_name,
        active_engine=active_engine,
        ctx=ctx,
    )
    errors["schedule_error"] = _run_source_ai_schedule(
        source=source,
        active_engine=active_engine,
        ctx=ctx,
        execs=execs,
        state=state,
    )
    _run_source_low_priority(
        source=source,
        active_engine=active_engine,
        ctx=ctx,
        execs=execs,
        state=state,
    )

    any_inflight = _any_inflight(source=source, state=state)
    _save_cycle_progress(
        source=source,
        ctx=ctx,
        any_inflight=any_inflight,
        rss_enqueue_error=rss_enqueue_error,
        errors=errors,
    )
    return bool(any_inflight)


__all__ = ["run_source_tick"]
