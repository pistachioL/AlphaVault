from __future__ import annotations

from contextlib import ExitStack, contextmanager
from concurrent.futures import Future, ThreadPoolExecutor
from collections.abc import Iterator
from dataclasses import dataclass
import threading
import time
from typing import Any, Callable, Optional

from alphavault.ai.analyze import (
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_MODEL,
)
from alphavault.infra.ai.runtime_config import AiRuntimeConfig
from alphavault.rss.utils import RateLimiter, sleep_until_active
from alphavault.worker import periodic_jobs
from alphavault.worker import scheduler
from alphavault.worker.progress_state import has_due_ai_posts
from alphavault.worker.runtime_cache import memoize_bool_with_ttl
from alphavault.worker.runtime_models import LLMConfig, WorkerSourceRuntime
from alphavault.worker.source_runtime import log_source_runtime
from alphavault.worker.worker_constants import DUE_AI_CHECK_CACHE_TTL_SECONDS
from alphavault.worker.worker_loop_runtime import rss_inflight_now
from alphavault.worker.worker_loop_models import (
    SourceTickContext,
    SourceTickExecutors,
    SourceTickState,
)
from alphavault.worker.worker_loop_source_tick import run_source_tick


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
    alias_ai_runtime_config: AiRuntimeConfig


@contextmanager
def _open_executors(*, ai_cap: int, source_count: int) -> Iterator[SourceTickExecutors]:
    max_source_workers = max(1, int(source_count))
    with ExitStack() as stack:
        ai_executor = stack.enter_context(ThreadPoolExecutor(max_workers=int(ai_cap)))
        alias_executor = stack.enter_context(
            ThreadPoolExecutor(max_workers=int(max_source_workers))
        )
        relation_executor = stack.enter_context(
            ThreadPoolExecutor(max_workers=int(max_source_workers))
        )
        backfill_executor = stack.enter_context(
            ThreadPoolExecutor(max_workers=int(max_source_workers))
        )
        stock_hot_executor = stack.enter_context(
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
            alias_executor=alias_executor,
            relation_executor=relation_executor,
            backfill_executor=backfill_executor,
            stock_hot_executor=stock_hot_executor,
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


def _build_alias_ai_runtime_config(*, config: LLMConfig) -> AiRuntimeConfig:
    return AiRuntimeConfig(
        api_key=str(config.api_key or "").strip(),
        model=str(config.model or "").strip() or DEFAULT_MODEL,
        base_url=str(config.base_url or "").strip(),
        api_mode=str(config.api_mode or DEFAULT_AI_MODE).strip() or DEFAULT_AI_MODE,
        temperature=float(config.ai_temperature),
        reasoning_effort=str(config.ai_reasoning_effort or "").strip()
        or DEFAULT_AI_REASONING_EFFORT,
        timeout_seconds=float(config.ai_timeout_seconds),
        retries=int(config.ai_retries),
    )


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


def _ensure_low_priority_gate(
    *,
    low_priority_gate: scheduler.LowPriorityAiSlotGate | None,
    inflight_futures: set[Future],
    ai_cap: int,
) -> scheduler.LowPriorityAiSlotGate:
    if low_priority_gate is not None:
        return low_priority_gate
    return scheduler.LowPriorityAiSlotGate(
        cap_getter=lambda: scheduler.compute_low_priority_budget(
            ai_cap=int(ai_cap),
            rss_inflight_now=int(rss_inflight_now(inflight_futures)),
        )
    )


def _build_tick_ctx(
    *,
    loop_ctx: WorkerLoopContext,
    source: WorkerSourceRuntime,
    worker_interval_seconds: float,
    maintenance_next_at: float,
    now: float,
    do_maintenance: bool,
    low_priority_gate: scheduler.LowPriorityAiSlotGate | None,
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
        alias_ai_runtime_config=loop_ctx.alias_ai_runtime_config,
        low_priority_gate=low_priority_gate,
        due_ai_pending_get=loop_ctx.due_ai_cached_by_source.get(source_name),
    )


def _run_sources_once(
    *,
    loop_ctx: WorkerLoopContext,
    worker_interval_seconds: float,
    maintenance_next_at: float,
    now: float,
    do_maintenance: bool,
    low_priority_gate: scheduler.LowPriorityAiSlotGate | None,
    execs: SourceTickExecutors,
    state: SourceTickState,
) -> bool:
    any_inflight = False
    for source in loop_ctx.sources:
        tick_ctx = _build_tick_ctx(
            loop_ctx=loop_ctx,
            source=source,
            worker_interval_seconds=float(worker_interval_seconds),
            maintenance_next_at=float(maintenance_next_at),
            now=float(now),
            do_maintenance=bool(do_maintenance),
            low_priority_gate=low_priority_gate,
        )
        any_inflight = any_inflight or bool(
            run_source_tick(source=source, ctx=tick_ctx, execs=execs, state=state)
        )
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
    low_priority_gate: scheduler.LowPriorityAiSlotGate | None,
    execs: SourceTickExecutors,
    state: SourceTickState,
) -> tuple[bool, float, scheduler.LowPriorityAiSlotGate | None, float]:
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
    low_priority_gate = _ensure_low_priority_gate(
        low_priority_gate=low_priority_gate,
        inflight_futures=state.inflight_futures,
        ai_cap=int(loop_ctx.ai_cap),
    )
    any_inflight = _run_sources_once(
        loop_ctx=loop_ctx,
        worker_interval_seconds=float(loop_ctx.worker_interval_seconds),
        maintenance_next_at=float(maintenance_next_at),
        now=float(now),
        do_maintenance=bool(do_maintenance),
        low_priority_gate=low_priority_gate,
        execs=execs,
        state=state,
    )
    return (
        bool(any_inflight),
        float(maintenance_next_at),
        low_priority_gate,
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
    low_priority_gate: scheduler.LowPriorityAiSlotGate | None = None
    with _open_executors(
        ai_cap=int(loop_ctx.ai_cap),
        source_count=len(loop_ctx.sources),
    ) as execs:
        while True:
            (
                any_inflight,
                maintenance_next_at,
                low_priority_gate,
                next_maintenance_in,
            ) = _run_worker_loop_tick(
                loop_ctx=loop_ctx,
                maintenance_next_at=float(maintenance_next_at),
                low_priority_gate=low_priority_gate,
                execs=execs,
                state=state,
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
    alias_ai_runtime_config = _build_alias_ai_runtime_config(config=config)
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
        alias_ai_runtime_config=alias_ai_runtime_config,
    )
    _run_worker_loop_forever(
        loop_ctx=loop_ctx,
    )


__all__ = ["run_worker_forever"]
