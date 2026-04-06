from __future__ import annotations

from concurrent.futures import Future
from typing import Any, Callable


def collect_periodic_job_result(
    *,
    job_name: str,
    future: Future | None,
    engine: Any,
    verbose: bool,
    maybe_dispose_turso_engine_on_transient_error_fn: Callable[..., None],
    fatal_exceptions: tuple[type[BaseException], ...],
) -> tuple[Future | None, dict[str, int | bool], bool, bool]:
    if future is None or not future.done():
        return future, {}, False, False
    try:
        raw = future.result()
    except BaseException as err:
        if isinstance(err, fatal_exceptions):
            raise
        maybe_dispose_turso_engine_on_transient_error_fn(
            engine=engine,
            err=err,
            verbose=bool(verbose),
        )
        if verbose:
            print(f"[{job_name}] sync_error {type(err).__name__}: {err}", flush=True)
        return None, {}, True, True
    stats = raw if isinstance(raw, dict) else {}
    return None, stats, True, False


def collect_rss_ingest_result(
    *,
    source_name: str,
    future: Future | None,
    engine: Any,
    verbose: bool,
    maybe_dispose_turso_engine_on_transient_error_fn: Callable[..., None],
    fatal_exceptions: tuple[type[BaseException], ...],
) -> tuple[Future | None, int, bool, bool]:
    if future is None or not future.done():
        return future, 0, False, False
    try:
        raw = future.result()
    except BaseException as err:
        if isinstance(err, fatal_exceptions):
            raise
        maybe_dispose_turso_engine_on_transient_error_fn(
            engine=engine,
            err=err,
            verbose=bool(verbose),
        )
        if verbose:
            print(
                f"[rss:{source_name}] ingest_error {type(err).__name__}: {err}",
                flush=True,
            )
        return None, 0, True, True

    accepted = 0
    ingest_enqueue_error = False
    if isinstance(raw, tuple) and len(raw) >= 2:
        try:
            accepted = max(0, int(raw[0]))
        except Exception:
            accepted = 0
        ingest_enqueue_error = bool(raw[1])
    return None, accepted, True, ingest_enqueue_error


def should_fast_retry_for_periodic_job(
    *,
    has_more: bool,
    attempted: int | None = None,
) -> bool:
    if attempted is None:
        return bool(has_more)
    try:
        attempted_count = int(attempted)
    except Exception:
        attempted_count = 0
    return bool(has_more) and attempted_count > 0


def build_source_turso_error(
    *,
    maintenance_error: bool,
    spool_flush_error: bool,
    schedule_error: bool,
    stock_hot_error: bool,
) -> bool:
    return bool(
        maintenance_error or spool_flush_error or schedule_error or stock_hot_error
    )


def should_wait_with_event(
    *,
    ai_inflight: bool,
    any_stock_hot_inflight: bool,
    any_redis_enqueue_inflight: bool,
    any_rss_inflight: bool,
    any_spool_flush_inflight: bool,
) -> bool:
    return bool(
        ai_inflight
        or any_stock_hot_inflight
        or any_redis_enqueue_inflight
        or any_rss_inflight
        or any_spool_flush_inflight
    )
