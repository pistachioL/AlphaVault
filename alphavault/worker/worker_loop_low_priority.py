from __future__ import annotations

import threading

from alphavault.worker import periodic_jobs
from alphavault.worker import scheduler
from alphavault.worker.research_backfill_cache import sync_stock_backfill_cache
from alphavault.worker.research_relation_candidates_cache import (
    sync_relation_candidates_cache,
)
from alphavault.worker.research_stock_cache import sync_stock_hot_cache
from alphavault.worker.stock_alias_sync import sync_stock_alias_relations
from alphavault.worker.worker_constants import BACKFILL_CACHE_FALLBACK_INTERVAL_SECONDS
from alphavault.worker.worker_loop_models import SourceTickContext, SourceTickExecutors
from alphavault.worker.worker_loop_runtime import (
    resolve_stock_alias_sync_interval_seconds,
    resolve_stock_hot_cache_interval_seconds,
    rss_inflight_now,
)


def _build_on_alias_data_loaded(source):
    def _on_alias_data_loaded(new_rows, _stock_relations, new_max_id, src=source):  # type: ignore[no-untyped-def]
        import pandas as _pd

        if src.alias_assertions_snapshot is None:
            src.alias_assertions_snapshot = new_rows
        elif hasattr(new_rows, "empty") and not new_rows.empty:
            src.alias_assertions_snapshot = _pd.concat(
                [src.alias_assertions_snapshot, new_rows],
                ignore_index=True,
            )
        if int(new_max_id) > int(getattr(src, "alias_last_assertion_id", 0) or 0):
            src.alias_last_assertion_id = int(new_max_id)

    return _on_alias_data_loaded


def _build_should_continue_low_priority(
    *,
    ctx: SourceTickContext,
    inflight_futures: set,
):
    return scheduler.build_low_priority_should_continue(
        ai_cap=int(ctx.ai_cap),
        rss_inflight_now_get=lambda: int(rss_inflight_now(inflight_futures)),
        low_inflight_now_get=(
            ctx.low_priority_gate.inflight
            if ctx.low_priority_gate is not None
            else None
        ),
        has_due_ai_pending_get=ctx.due_ai_pending_get,
    )


def _schedule_alias_sync(
    *,
    source,
    active_engine,
    ctx: SourceTickContext,
    execs: SourceTickExecutors,
    wakeup_event: threading.Event,
    interval_seconds: float,
    should_continue_low_priority,
) -> None:
    on_alias_data_loaded = _build_on_alias_data_loaded(source)
    source.alias_sync_future, source.alias_sync_next_at, _ = (
        periodic_jobs.maybe_start_periodic_job(
            executor=execs.alias_executor,
            future=source.alias_sync_future,
            active_engine=active_engine,
            trigger=ctx.now >= float(getattr(source, "alias_sync_next_at", 0.0) or 0.0),
            now=float(ctx.now),
            next_run_at=float(getattr(source, "alias_sync_next_at", 0.0) or 0.0),
            interval_seconds=float(interval_seconds),
            wakeup_event=wakeup_event,
            submit_fn=sync_stock_alias_relations,
            submit_kwargs={
                "ai_runtime_config": ctx.alias_ai_runtime_config,
                "ai_max_inflight": max(1, int(ctx.ai_cap)),
                "should_continue": should_continue_low_priority,
                "acquire_low_priority_slot": (
                    ctx.low_priority_gate.try_acquire
                    if ctx.low_priority_gate is not None
                    else None
                ),
                "release_low_priority_slot": (
                    ctx.low_priority_gate.release
                    if ctx.low_priority_gate is not None
                    else None
                ),
                "cached_assertions": getattr(source, "alias_assertions_snapshot", None),
                "last_assertion_id": int(
                    getattr(source, "alias_last_assertion_id", 0) or 0
                ),
                "on_data_loaded": on_alias_data_loaded,
                "verbose": ctx.verbose,
            },
        )
    )


def _schedule_backfill_cache(
    *,
    source,
    active_engine,
    ctx: SourceTickContext,
    execs: SourceTickExecutors,
    wakeup_event: threading.Event,
    inflight_futures: set,
    should_continue_low_priority,
) -> None:
    low_budget = scheduler.compute_low_priority_budget(
        ai_cap=int(ctx.ai_cap),
        rss_inflight_now=int(rss_inflight_now(inflight_futures)),
    )
    max_stocks = scheduler.compute_backfill_max_stocks_per_run(
        low_budget=int(low_budget)
    )
    source.backfill_cache_future, source.backfill_cache_next_at, _ = (
        periodic_jobs.maybe_start_periodic_job(
            executor=execs.backfill_executor,
            future=source.backfill_cache_future,
            active_engine=active_engine,
            trigger=ctx.now
            >= float(getattr(source, "backfill_cache_next_at", 0.0) or 0.0),
            now=float(ctx.now),
            next_run_at=float(getattr(source, "backfill_cache_next_at", 0.0) or 0.0),
            interval_seconds=float(BACKFILL_CACHE_FALLBACK_INTERVAL_SECONDS),
            wakeup_event=wakeup_event,
            submit_fn=sync_stock_backfill_cache,
            submit_kwargs={
                "max_stocks_per_run": int(max_stocks),
                "should_continue": should_continue_low_priority,
                "verbose": ctx.verbose,
            },
        )
    )


def _schedule_relation_cache(
    *,
    source,
    active_engine,
    ctx: SourceTickContext,
    execs: SourceTickExecutors,
    wakeup_event: threading.Event,
    interval_seconds: float,
    should_continue_low_priority,
) -> None:
    source.relation_cache_future, source.relation_cache_next_at, _ = (
        periodic_jobs.maybe_start_periodic_job(
            executor=execs.relation_executor,
            future=source.relation_cache_future,
            active_engine=active_engine,
            trigger=ctx.now
            >= float(getattr(source, "relation_cache_next_at", 0.0) or 0.0),
            now=float(ctx.now),
            next_run_at=float(getattr(source, "relation_cache_next_at", 0.0) or 0.0),
            interval_seconds=float(interval_seconds),
            wakeup_event=wakeup_event,
            submit_fn=sync_relation_candidates_cache,
            submit_kwargs={
                "limiter": ctx.limiter,
                "ai_enabled": True,
                "max_stocks_per_run": max(1, int(ctx.ai_cap)),
                "max_sectors_per_run": max(1, int(ctx.ai_cap)),
                "ai_max_inflight": max(1, int(ctx.ai_cap)),
                "should_continue": should_continue_low_priority,
                "acquire_low_priority_slot": (
                    ctx.low_priority_gate.try_acquire
                    if ctx.low_priority_gate is not None
                    else None
                ),
                "release_low_priority_slot": (
                    ctx.low_priority_gate.release
                    if ctx.low_priority_gate is not None
                    else None
                ),
                "verbose": ctx.verbose,
            },
        )
    )


def _schedule_stock_hot_cache(
    *,
    source,
    active_engine,
    ctx: SourceTickContext,
    execs: SourceTickExecutors,
    wakeup_event: threading.Event,
    interval_seconds: float,
    should_continue_low_priority,
) -> None:
    source.stock_hot_cache_future, source.stock_hot_cache_next_at, _ = (
        periodic_jobs.maybe_start_periodic_job(
            executor=execs.stock_hot_executor,
            future=source.stock_hot_cache_future,
            active_engine=active_engine,
            trigger=ctx.now
            >= float(getattr(source, "stock_hot_cache_next_at", 0.0) or 0.0),
            now=float(ctx.now),
            next_run_at=float(getattr(source, "stock_hot_cache_next_at", 0.0) or 0.0),
            interval_seconds=float(interval_seconds),
            wakeup_event=wakeup_event,
            submit_fn=sync_stock_hot_cache,
            submit_kwargs={
                "should_continue": should_continue_low_priority,
                "verbose": ctx.verbose,
            },
        )
    )


def schedule_low_priority_jobs(
    *,
    source,
    active_engine,
    ctx: SourceTickContext,
    execs: SourceTickExecutors,
    wakeup_event: threading.Event,
    inflight_futures: set,
) -> None:
    if active_engine is None:
        return

    alias_interval = float(resolve_stock_alias_sync_interval_seconds())
    should_continue_low_priority = _build_should_continue_low_priority(
        ctx=ctx,
        inflight_futures=inflight_futures,
    )

    common = {
        "source": source,
        "active_engine": active_engine,
        "ctx": ctx,
        "execs": execs,
        "wakeup_event": wakeup_event,
        "should_continue_low_priority": should_continue_low_priority,
    }
    _schedule_alias_sync(**common, interval_seconds=float(alias_interval))
    _schedule_backfill_cache(**common, inflight_futures=inflight_futures)
    _schedule_relation_cache(**common, interval_seconds=float(alias_interval))
    stock_hot_interval = float(resolve_stock_hot_cache_interval_seconds())
    _schedule_stock_hot_cache(**common, interval_seconds=float(stock_hot_interval))


__all__ = ["schedule_low_priority_jobs"]
