from __future__ import annotations

from alphavault.logging_config import get_logger
from alphavault.worker import cycle_runner
from alphavault.worker import periodic_jobs
from alphavault.worker.source_db_runtime import (
    maybe_dispose_source_db_engine_on_transient_error,
)

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
logger = get_logger(__name__)


def _collect_rss_ingest(
    *,
    source,
    source_name: str,
    now: float,
    rss_interval_seconds: float,
) -> bool:
    source.rss_ingest_future, rss_accepted, rss_finished, rss_enqueue_error = (
        cycle_runner.collect_rss_ingest_result(
            source_name=source_name,
            future=getattr(source, "rss_ingest_future", None),
            engine=source.engine,
            maybe_dispose_source_db_engine_on_transient_error_fn=maybe_dispose_source_db_engine_on_transient_error,
            fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        )
    )
    if rss_finished:
        source.rss_next_ingest_at = now + float(rss_interval_seconds)
        if rss_accepted > 0 or rss_enqueue_error:
            logger.info(
                "[rss:%s] done accepted=%s enqueue_error=%s",
                source_name,
                int(rss_accepted),
                1 if rss_enqueue_error else 0,
            )
    return bool(rss_enqueue_error)


def _collect_spool_flush(
    *,
    source,
    source_name: str,
    now: float,
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
        maybe_dispose_source_db_engine_on_transient_error_fn=maybe_dispose_source_db_engine_on_transient_error,
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
    if flushed > 0 or has_error:
        logger.info(
            "[spool:%s] flush_done flushed=%s has_more=%s ok=%s",
            source_name,
            flushed,
            1 if has_more else 0,
            0 if has_error else 1,
        )
    return bool(has_error)


def _collect_redis_enqueue(
    *,
    source,
    source_name: str,
    now: float,
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
        maybe_dispose_source_db_engine_on_transient_error_fn=maybe_dispose_source_db_engine_on_transient_error,
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
    if attempted > 0 or has_error:
        logger.info(
            "[redis:%s] enqueue_done attempted=%s pushed=%s duplicates=%s has_more=%s ok=%s",
            source_name,
            attempted,
            pushed,
            duplicates,
            1 if has_more else 0,
            0 if has_error else 1,
        )
    return bool(has_error)


def collect_finished_jobs(
    *,
    source,
    source_name: str,
    now: float,
    rss_interval_seconds: float,
) -> tuple[dict[str, bool], bool]:
    errors = {
        "maintenance_error": False,
        "redis_enqueue_error": False,
        "spool_flush_error": False,
        "schedule_error": False,
    }

    rss_enqueue_error = _collect_rss_ingest(
        source=source,
        source_name=source_name,
        now=float(now),
        rss_interval_seconds=float(rss_interval_seconds),
    )
    errors["spool_flush_error"] = _collect_spool_flush(
        source=source,
        source_name=source_name,
        now=float(now),
    )
    errors["redis_enqueue_error"] = _collect_redis_enqueue(
        source=source,
        source_name=source_name,
        now=float(now),
    )

    return errors, bool(rss_enqueue_error)


__all__ = ["collect_finished_jobs"]
