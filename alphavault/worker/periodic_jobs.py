from __future__ import annotations

from concurrent.futures import Future
from datetime import datetime
from typing import Any, Callable


def mark_spool_item_ingested(*, source: Any, wakeup_event: Any) -> None:
    with source.spool_state_lock:
        source.spool_seq_written += 1
    wakeup_event.set()


def should_start_spool_flush(*, source: Any) -> bool:
    with source.spool_state_lock:
        return bool(
            source.spool_need_retry
            or int(source.spool_seq_written) > int(source.spool_seq_scheduled)
        )


def mark_spool_flush_started(*, source: Any) -> None:
    with source.spool_state_lock:
        source.spool_seq_scheduled = int(source.spool_seq_written)
        source.spool_need_retry = False


def mark_spool_flush_retry(*, source: Any, has_more: bool, has_error: bool) -> None:
    if not (bool(has_more) or bool(has_error)):
        return
    with source.spool_state_lock:
        source.spool_need_retry = True


def maybe_start_periodic_job(
    *,
    executor: Any,
    future: Future | None,
    active_engine: Any,
    trigger: bool,
    now: float,
    next_run_at: float,
    interval_seconds: float,
    wakeup_event: Any,
    submit_fn: Callable[..., dict[str, int | bool]],
    submit_kwargs: dict[str, Any] | None = None,
) -> tuple[Future | None, float, bool]:
    engine_for_job = active_engine
    if (
        not trigger
        or engine_for_job is None
        or future is not None
        or now < float(next_run_at)
    ):
        return future, next_run_at, False
    new_future = executor.submit(submit_fn, engine_for_job, **(submit_kwargs or {}))
    new_future.add_done_callback(lambda _f: wakeup_event.set())
    if bool(interval_seconds) and float(interval_seconds) > 0:
        next_at = now + float(interval_seconds)
    else:
        next_at = now
    return new_future, next_at, True


def prune_inflight_futures(
    inflight_futures: set[Future],
    inflight_owner_by_future: dict[Future, str],
) -> None:
    done = {f for f in inflight_futures if f.done()}
    if not done:
        return
    inflight_futures.difference_update(done)
    for fut in done:
        inflight_owner_by_future.pop(fut, None)


def format_epoch_to_cst(value: float, *, cst_tz: Any) -> str:
    ts = float(value or 0.0)
    if ts <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ts, tz=cst_tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""
