from __future__ import annotations

from contextlib import ExitStack, contextmanager
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterator
from dataclasses import dataclass
import threading
import time
from typing import Any, Callable, Optional

from alphavault.rss.utils import RateLimiter, sleep_until_active
from alphavault.worker import periodic_jobs
from alphavault.worker.progress_state import has_due_ai_posts
from alphavault.worker.runtime_cache import memoize_bool_with_ttl
from alphavault.worker.runtime_models import LLMConfig, WorkerSourceRuntime
from alphavault.worker.source_runtime import log_source_runtime
from alphavault.worker.worker_constants import DUE_AI_CHECK_CACHE_TTL_SECONDS
from alphavault.worker.worker_loop_models import (
    SourceTickContext,
    SourceTickExecutors,
    SourceTickState,
)
from alphavault.worker.worker_loop_source_tick import (
    PreparedSourceTick,
    finalize_source_tick,
    prepare_source_tick,
    run_prepared_source_maintenance,
)


@dataclass(frozen=True)
class WorkerLoopSettings:
    verbose: bool
    limit_or_none: int | None
    stuck_seconds: int


@dataclass(frozen=True)
class WorkerLoopContext:
    args: Any
    sources: list[WorkerSourceRuntime]
    settings: WorkerLoopSettings
    config: LLMConfig
    limiter: RateLimiter
    ai_cap: int
    redis_client: Any
    rss_active_hours: Optional[tuple[int, int]]
    rss_interval_seconds: float
    rss_feed_sleep_seconds: float
    worker_active_hours: Optional[tuple[int, int]]
    worker_interval_seconds: float
    due_ai_cached_by_source: dict[str, Callable[[], bool]]


@contextmanager
def _open_executors(*, ai_cap: int, source_count: int) -> Iterator[SourceTickExecutors]:
    max_source_workers = max(1, int(source_count))
    with ExitStack() as stack:
        ai_executor = stack.enter_context(ThreadPoolExecutor(max_workers=int(ai_cap)))
        stock_hot_executor = stack.enter_context(
            ThreadPoolExecutor(max_workers=int(max_source_workers))
        )
        redis_enqueue_executor = stack.enter_context(
            ThreadPoolExecutor(max_workers=int(max_source_workers))
        )
        spool_executor = stack.enter_context(
            ThreadPoolExecutor(max_workers=int(max_source_workers))
        )
        rss_executor = stack.enter_context(
            ThreadPoolExecutor(max_workers=int(max_source_workers))
        )
        yield SourceTickExecutors(
            ai_executor=ai_executor,
            stock_hot_executor=stock_hot_executor,
            redis_enqueue_executor=redis_enqueue_executor,
            spool_executor=spool_executor,
            rss_executor=rss_executor,
        )


def _build_due_ai_cached_by_source(
    *,
    sources: list[WorkerSourceRuntime],
    redis_client,
    verbose: bool,
) -> dict[str, Callable[[], bool]]:
    cached: dict[str, Callable[[], bool]] = {}
    for src in sources:
        source_name = str(src.config.name or "").strip()

        def _resolver(source: WorkerSourceRuntime = src) -> bool:
            return has_due_ai_posts(
                engine=(
                    source.engine
                    if bool(getattr(source, "turso_ready", False))
                    else None
                ),
                platform=str(source.config.platform or ""),
                verbose=verbose,
                redis_client=redis_client,
                redis_queue_key=str(source.redis_queue_key or ""),
            )

        cached[source_name] = memoize_bool_with_ttl(
            resolver=_resolver,
            ttl_seconds=float(DUE_AI_CHECK_CACHE_TTL_SECONDS),
        )
    return cached


def _build_settings(args) -> WorkerLoopSettings:
    verbose = bool(getattr(args, "verbose", False))
    limit = int(getattr(args, "limit", 0) or 0)
    limit_or_none = limit if limit > 0 else None
    stuck_seconds = int(getattr(args, "ai_stuck_seconds", 3600) or 3600)
    return WorkerLoopSettings(
        verbose=bool(verbose),
        limit_or_none=limit_or_none,
        stuck_seconds=int(stuck_seconds),
    )


def _compute_maintenance(
    *,
    maintenance_next_at: float,
    now: float,
    worker_interval_seconds: float,
) -> tuple[bool, float, float]:
    do_maintenance = bool(now >= maintenance_next_at)
    next_at = float(maintenance_next_at)
    if do_maintenance:
        next_at = now + float(worker_interval_seconds)
    next_in = max(0.0, next_at - time.time())
    return bool(do_maintenance), float(next_at), float(next_in)


def _build_tick_ctx(
    *,
    loop_ctx: WorkerLoopContext,
    source: WorkerSourceRuntime,
    worker_interval_seconds: float,
    maintenance_next_at: float,
    now: float,
    do_maintenance: bool,
) -> SourceTickContext:
    source_name = str(source.config.name or "").strip()
    return SourceTickContext(
        args=loop_ctx.args,
        config=loop_ctx.config,
        limiter=loop_ctx.limiter,
        redis_client=loop_ctx.redis_client,
        ai_cap=int(loop_ctx.ai_cap),
        limit_or_none=loop_ctx.settings.limit_or_none,
        stuck_seconds=int(loop_ctx.settings.stuck_seconds),
        verbose=bool(loop_ctx.settings.verbose),
        rss_active_hours=loop_ctx.rss_active_hours,
        rss_interval_seconds=float(loop_ctx.rss_interval_seconds),
        rss_feed_sleep_seconds=float(loop_ctx.rss_feed_sleep_seconds),
        worker_interval_seconds=float(worker_interval_seconds),
        maintenance_next_at=float(maintenance_next_at),
        now=float(now),
        do_maintenance=bool(do_maintenance),
        due_ai_pending_get=loop_ctx.due_ai_cached_by_source.get(source_name),
    )


def _run_sources_once(
    *,
    loop_ctx: WorkerLoopContext,
    worker_interval_seconds: float,
    maintenance_next_at: float,
    now: float,
    do_maintenance: bool,
    execs: SourceTickExecutors,
    state: SourceTickState,
) -> bool:
    prepared_ticks: list[
        tuple[WorkerSourceRuntime, SourceTickContext, PreparedSourceTick]
    ] = []
    for source in loop_ctx.sources:
        tick_ctx = _build_tick_ctx(
            loop_ctx=loop_ctx,
            source=source,
            worker_interval_seconds=float(worker_interval_seconds),
            maintenance_next_at=float(maintenance_next_at),
            now=float(now),
            do_maintenance=bool(do_maintenance),
        )
        prepared_ticks.append(
            (
                source,
                tick_ctx,
                prepare_source_tick(
                    source=source,
                    ctx=tick_ctx,
                    execs=execs,
                    state=state,
                ),
            )
        )

    prepared_ticks = [
        (
            source,
            tick_ctx,
            run_prepared_source_maintenance(
                source=source,
                prepared=prepared,
                ctx=tick_ctx,
                state=state,
            ),
        )
        for source, tick_ctx, prepared in prepared_ticks
    ]

    any_inflight = False
    for source, tick_ctx, prepared in prepared_ticks:
        source_inflight = bool(
            finalize_source_tick(
                source=source,
                prepared=prepared,
                ctx=tick_ctx,
                execs=execs,
                state=state,
            )
        )
        any_inflight = bool(any_inflight or source_inflight)
    return bool(any_inflight)


def _wait_after_tick(
    *,
    any_inflight: bool,
    wakeup_event: threading.Event,
    next_maintenance_in: float,
) -> None:
    wait_seconds = float(min(5.0, max(0.1, float(next_maintenance_in))))
    if any_inflight:
        wakeup_event.wait(timeout=wait_seconds)
    else:
        time.sleep(wait_seconds)


def _run_worker_loop_tick(
    *,
    loop_ctx: WorkerLoopContext,
    maintenance_next_at: float,
    execs: SourceTickExecutors,
    state: SourceTickState,
) -> tuple[bool, float, float]:
    if loop_ctx.worker_active_hours is not None:
        sleep_until_active(
            loop_ctx.worker_active_hours, verbose=loop_ctx.settings.verbose
        )

    state.wakeup_event.clear()
    periodic_jobs.prune_inflight_futures(
        state.inflight_futures, state.inflight_owner_by_future
    )
    now = float(time.time())

    do_maintenance, maintenance_next_at, next_maintenance_in = _compute_maintenance(
        maintenance_next_at=float(maintenance_next_at),
        now=float(now),
        worker_interval_seconds=float(loop_ctx.worker_interval_seconds),
    )
    any_inflight = _run_sources_once(
        loop_ctx=loop_ctx,
        worker_interval_seconds=float(loop_ctx.worker_interval_seconds),
        maintenance_next_at=float(maintenance_next_at),
        now=float(now),
        do_maintenance=bool(do_maintenance),
        execs=execs,
        state=state,
    )
    return (
        bool(any_inflight),
        float(maintenance_next_at),
        float(next_maintenance_in),
    )


def _run_worker_loop_forever(
    *,
    loop_ctx: WorkerLoopContext,
) -> None:
    state = SourceTickState(
        wakeup_event=threading.Event(),
        inflight_futures=set(),
        inflight_owner_by_future={},
    )
    maintenance_next_at = 0.0
    with _open_executors(
        ai_cap=int(loop_ctx.ai_cap),
        source_count=len(loop_ctx.sources),
    ) as execs:
        while True:
            any_inflight, maintenance_next_at, next_maintenance_in = (
                _run_worker_loop_tick(
                    loop_ctx=loop_ctx,
                    maintenance_next_at=float(maintenance_next_at),
                    execs=execs,
                    state=state,
                )
            )
            _wait_after_tick(
                any_inflight=bool(any_inflight),
                wakeup_event=state.wakeup_event,
                next_maintenance_in=float(next_maintenance_in),
            )


def run_worker_forever(
    *,
    args,
    sources: list[WorkerSourceRuntime],
    config: LLMConfig,
    limiter: RateLimiter,
    ai_cap: int,
    redis_client,
    rss_active_hours: Optional[tuple[int, int]],
    rss_interval_seconds: float,
    rss_feed_sleep_seconds: float,
    worker_active_hours: Optional[tuple[int, int]],
    worker_interval_seconds: float,
) -> None:
    settings = _build_settings(args)
    for src in sources:
        log_source_runtime(
            verbose=settings.verbose,
            source=src,
            redis_client=redis_client,
            rss_interval_seconds=float(rss_interval_seconds),
            rss_feed_sleep_seconds=float(rss_feed_sleep_seconds),
        )

    due_ai_cached_by_source = _build_due_ai_cached_by_source(
        sources=sources,
        redis_client=redis_client,
        verbose=settings.verbose,
    )
    loop_ctx = WorkerLoopContext(
        args=args,
        sources=sources,
        settings=settings,
        config=config,
        limiter=limiter,
        ai_cap=int(ai_cap),
        redis_client=redis_client,
        rss_active_hours=rss_active_hours,
        rss_interval_seconds=float(rss_interval_seconds),
        rss_feed_sleep_seconds=float(rss_feed_sleep_seconds),
        worker_active_hours=worker_active_hours,
        worker_interval_seconds=float(worker_interval_seconds),
        due_ai_cached_by_source=due_ai_cached_by_source,
    )
    _run_worker_loop_forever(
        loop_ctx=loop_ctx,
    )


__all__ = ["run_worker_forever"]
