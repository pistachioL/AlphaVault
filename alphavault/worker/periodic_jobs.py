from __future__ import annotations

from concurrent.futures import Future
from datetime import datetime
from typing import Any

from alphavault.logging_config import get_logger

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
logger = get_logger(__name__)


def should_start_redis_enqueue(*, source: Any) -> bool:
    with source.redis_enqueue_state_lock:
        return bool(
            source.redis_enqueue_need_retry or bool(source.redis_enqueue_pending)
        )


def mark_redis_enqueue_started(*, source: Any) -> None:
    with source.redis_enqueue_state_lock:
        source.redis_enqueue_need_retry = False


def pop_next_redis_enqueue_payload(*, source: Any) -> dict[str, Any] | None:
    with source.redis_enqueue_state_lock:
        if not source.redis_enqueue_pending:
            return None
        return dict(source.redis_enqueue_pending.popleft())


def restore_redis_enqueue_payload(
    *,
    source: Any,
    payload: dict[str, Any],
    wakeup_event: Any | None = None,
) -> None:
    with source.redis_enqueue_state_lock:
        source.redis_enqueue_pending.appendleft(dict(payload))
    if wakeup_event is not None:
        wakeup_event.set()


def mark_redis_enqueue_retry(*, source: Any, has_more: bool, has_error: bool) -> None:
    if not (bool(has_more) or bool(has_error)):
        return
    with source.redis_enqueue_state_lock:
        source.redis_enqueue_need_retry = True


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


def prune_inflight_futures(
    inflight_futures: set[Future],
    inflight_owner_by_future: dict[Future, str],
) -> None:
    done = {f for f in inflight_futures if f.done()}
    if not done:
        return
    for fut in done:
        inflight_futures.discard(fut)
        owner = str(inflight_owner_by_future.pop(fut, "") or "").strip()
        try:
            fut.result()
        except BaseException as err:
            if isinstance(err, _FATAL_BASE_EXCEPTIONS):
                raise
            logger.warning(
                "[ai] future_error owner=%s %s: %s",
                owner or "(unknown)",
                type(err).__name__,
                err,
            )


def format_epoch_to_cst(value: float, *, cst_tz: Any) -> str:
    ts = float(value or 0.0)
    if ts <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ts, tz=cst_tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""
