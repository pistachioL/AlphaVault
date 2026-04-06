from __future__ import annotations

from alphavault.worker import cycle_runner
from alphavault.worker import periodic_jobs
from alphavault.worker.turso_runtime import (
    maybe_dispose_turso_engine_on_transient_error,
)

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _collect_rss_ingest(
    *,
    source,
    source_name: str,
    now: float,
    verbose: bool,
    rss_interval_seconds: float,
) -> bool:
    source.rss_ingest_future, rss_accepted, rss_finished, rss_enqueue_error = (
        cycle_runner.collect_rss_ingest_result(
            source_name=source_name,
            future=getattr(source, "rss_ingest_future", None),
            engine=source.engine,
            verbose=verbose,
            maybe_dispose_turso_engine_on_transient_error_fn=maybe_dispose_turso_engine_on_transient_error,
            fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        )
    )
    if rss_finished:
        source.rss_next_ingest_at = now + float(rss_interval_seconds)
        if verbose and (rss_accepted > 0 or rss_enqueue_error):
            print(
                f"[rss:{source_name}] done accepted={int(rss_accepted)} "
                f"enqueue_error={1 if rss_enqueue_error else 0}",
                flush=True,
            )
    return bool(rss_enqueue_error)


def _collect_spool_flush(
    *,
    source,
    source_name: str,
    now: float,
    verbose: bool,
) -> bool:
    (
        source.spool_flush_future,
        spool_stats,
        spool_finished,
        spool_exception_error,
    ) = cycle_runner.collect_periodic_job_result(
        job_name=f"spool:{source_name}",
        future=getattr(source, "spool_flush_future", None),
        engine=source.engine,
        verbose=verbose,
        maybe_dispose_turso_engine_on_transient_error_fn=maybe_dispose_turso_engine_on_transient_error,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
    )
    if not spool_finished:
        return False

    flushed = int(spool_stats.get("flushed", 0) or 0)
    has_more = bool(spool_stats.get("has_more", False))
    has_error = bool(spool_stats.get("has_error", False)) or bool(spool_exception_error)
    periodic_jobs.mark_spool_flush_retry(
        source=source,
        has_more=has_more,
        has_error=has_error,
    )
    if cycle_runner.should_fast_retry_for_periodic_job(
        has_more=has_more,
        attempted=flushed,
    ):
        source.spool_flush_next_at = float(now)
    if verbose and (flushed > 0 or has_error):
        print(
            f"[spool:{source_name}] flush_done flushed={flushed} "
            f"has_more={1 if has_more else 0} "
            f"ok={0 if has_error else 1}",
            flush=True,
        )
    return bool(has_error)


def _collect_redis_enqueue(
    *,
    source,
    source_name: str,
    now: float,
    verbose: bool,
) -> bool:
    (
        source.redis_enqueue_future,
        redis_stats,
        redis_finished,
        redis_exception_error,
    ) = cycle_runner.collect_periodic_job_result(
        job_name=f"redis_enqueue:{source_name}",
        future=getattr(source, "redis_enqueue_future", None),
        engine=source.engine,
        verbose=verbose,
        maybe_dispose_turso_engine_on_transient_error_fn=maybe_dispose_turso_engine_on_transient_error,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
    )
    if not redis_finished:
        return False

    attempted = int(redis_stats.get("attempted", 0) or 0)
    pushed = int(redis_stats.get("pushed", 0) or 0)
    duplicates = int(redis_stats.get("duplicates", 0) or 0)
    has_more = bool(redis_stats.get("has_more", False))
    has_error = bool(redis_stats.get("has_error", False)) or bool(redis_exception_error)
    periodic_jobs.mark_redis_enqueue_retry(
        source=source,
        has_more=has_more,
        has_error=has_error,
    )
    if cycle_runner.should_fast_retry_for_periodic_job(
        has_more=has_more,
        attempted=attempted,
    ):
        source.redis_enqueue_next_at = float(now)
    if verbose and (attempted > 0 or has_error):
        print(
            f"[redis:{source_name}] enqueue_done attempted={attempted} "
            f"pushed={pushed} duplicates={duplicates} "
            f"has_more={1 if has_more else 0} "
            f"ok={0 if has_error else 1}",
            flush=True,
        )
    return bool(has_error)


def _collect_periodic_job(
    *,
    job_name: str,
    source,
    future_attr: str,
    next_at_attr: str,
    now: float,
    verbose: bool,
    attempted_key: str,
) -> bool:
    future = getattr(source, future_attr, None)
    next_future, stats, finished, has_error = cycle_runner.collect_periodic_job_result(
        job_name=job_name,
        future=future,
        engine=source.engine,
        verbose=verbose,
        maybe_dispose_turso_engine_on_transient_error_fn=maybe_dispose_turso_engine_on_transient_error,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
    )
    setattr(source, future_attr, next_future)
    if not finished:
        return bool(has_error)

    has_more = bool(stats.get("has_more", False))
    attempted = stats.get(attempted_key, 0)
    if cycle_runner.should_fast_retry_for_periodic_job(
        has_more=has_more,
        attempted=attempted,
    ):
        setattr(source, next_at_attr, float(now))
    return bool(has_error)


def collect_finished_jobs(
    *,
    source,
    source_name: str,
    now: float,
    verbose: bool,
    rss_interval_seconds: float,
) -> tuple[dict[str, bool], bool]:
    errors = {
        "maintenance_error": False,
        "redis_enqueue_error": False,
        "spool_flush_error": False,
        "schedule_error": False,
        "stock_hot_error": False,
    }

    rss_enqueue_error = _collect_rss_ingest(
        source=source,
        source_name=source_name,
        now=float(now),
        verbose=bool(verbose),
        rss_interval_seconds=float(rss_interval_seconds),
    )
    errors["spool_flush_error"] = _collect_spool_flush(
        source=source,
        source_name=source_name,
        now=float(now),
        verbose=bool(verbose),
    )
    errors["redis_enqueue_error"] = _collect_redis_enqueue(
        source=source,
        source_name=source_name,
        now=float(now),
        verbose=bool(verbose),
    )
    errors["stock_hot_error"] = _collect_periodic_job(
        job_name=f"stock_hot_cache:{source_name}",
        source=source,
        future_attr="stock_hot_cache_future",
        next_at_attr="stock_hot_cache_next_at",
        now=float(now),
        verbose=bool(verbose),
        attempted_key="processed",
    )

    return errors, bool(rss_enqueue_error)


__all__ = ["collect_finished_jobs"]
