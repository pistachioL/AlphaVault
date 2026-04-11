from __future__ import annotations

import threading

from alphavault.worker import periodic_jobs
from alphavault.worker import scheduler
from alphavault.worker.research_stock_cache import sync_stock_hot_cache
from alphavault.worker.worker_loop_models import SourceTickContext, SourceTickExecutors
from alphavault.worker.worker_loop_runtime import (
    resolve_stock_hot_cache_interval_seconds,
    rss_inflight_now,
)


def _build_should_continue_low_priority(
    *,
    ctx: SourceTickContext,
    inflight_futures: set,
):
    return scheduler.build_low_priority_should_continue(
        ai_cap=int(ctx.ai_cap),
        rss_inflight_now_get=lambda: int(rss_inflight_now(inflight_futures)),
        has_due_ai_pending_get=ctx.due_ai_pending_get,
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
    stock_hot_interval = float(resolve_stock_hot_cache_interval_seconds())
    _schedule_stock_hot_cache(**common, interval_seconds=float(stock_hot_interval))


__all__ = ["schedule_low_priority_jobs"]
