from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence

from alphavault.worker.backoff import backoff_seconds


def compute_redis_due_maintenance_delay_seconds(
    *,
    consecutive_empty_checks: int,
    worker_interval_seconds: float,
) -> float:
    retries = max(1, int(consecutive_empty_checks))
    worker_interval = max(1.0, float(worker_interval_seconds))
    retry_delay = float(backoff_seconds(retries))
    return float(min(retry_delay, worker_interval))


def maybe_run_redis_due_maintenance(
    *,
    source: Any,
    redis_client: Any,
    worker_interval_seconds: float,
    verbose: bool,
    now_fn: Callable[[], float],
    move_due_retry_to_stream_fn: Callable[..., int],
    fatal_exceptions: tuple[type[BaseException], ...],
    max_items: int,
) -> tuple[int, bool]:
    queue_key = str(getattr(source, "redis_queue_key", "") or "").strip()
    if not redis_client or not queue_key:
        return 0, False
    now = float(now_fn())
    next_at = float(getattr(source, "redis_due_maintenance_next_at", 0.0))
    if now < next_at:
        return 0, False
    try:
        moved_due = move_due_retry_to_stream_fn(
            redis_client,
            queue_key,
            now_epoch=int(now),
            max_items=int(max_items),
            verbose=bool(verbose),
        )
    except BaseException as err:
        if isinstance(err, fatal_exceptions):
            raise
        empty_checks = int(getattr(source, "redis_due_maintenance_empty_checks", 0)) + 1
        source.redis_due_maintenance_empty_checks = int(empty_checks)
        source.redis_due_maintenance_next_at = now + float(
            compute_redis_due_maintenance_delay_seconds(
                consecutive_empty_checks=empty_checks,
                worker_interval_seconds=worker_interval_seconds,
            )
        )
        if verbose:
            print(
                f"[redis] ai_retry_to_stream_error queue={queue_key} "
                f"{type(err).__name__}: {err}",
                flush=True,
            )
        return 0, True
    moved = int(max(0, int(moved_due)))
    if moved > 0:
        source.redis_due_maintenance_empty_checks = 0
        source.redis_due_maintenance_next_at = now
        return moved, False
    empty_checks = int(getattr(source, "redis_due_maintenance_empty_checks", 0)) + 1
    source.redis_due_maintenance_empty_checks = int(empty_checks)
    source.redis_due_maintenance_next_at = now + float(
        compute_redis_due_maintenance_delay_seconds(
            consecutive_empty_checks=empty_checks,
            worker_interval_seconds=worker_interval_seconds,
        )
    )
    return 0, False


def run_turso_maintenance(
    *,
    engine: Any,
    redis_client: Any,
    redis_queue_key: str,
    verbose: bool,
    now_fn: Callable[[], float],
    move_due_retry_to_stream_fn: Callable[..., int],
    claim_stuck_messages_fn: Callable[..., Sequence[Mapping[str, object]]],
    fatal_exceptions: tuple[type[BaseException], ...],
    redis_ai_requeue_max_items: int,
    claim_min_idle_ms: int,
    consumer_name: str,
) -> tuple[int, int, bool]:
    if engine is None or not redis_client or not str(redis_queue_key or "").strip():
        return 0, 0, False

    recovered = 0
    flushed_redis = 0
    has_error = False
    now_epoch = int(now_fn())

    try:
        flushed_redis = int(
            move_due_retry_to_stream_fn(
                redis_client,
                str(redis_queue_key),
                now_epoch=now_epoch,
                max_items=int(redis_ai_requeue_max_items),
                verbose=bool(verbose),
            )
        )
    except BaseException as err:
        if isinstance(err, fatal_exceptions):
            raise
        has_error = True
        if verbose:
            print(
                f"[redis] retry_to_stream_error {type(err).__name__}: {err}",
                flush=True,
            )

    try:
        recovered = len(
            claim_stuck_messages_fn(
                redis_client,
                str(redis_queue_key),
                consumer_name=str(consumer_name or "").strip(),
                min_idle_ms=max(1, int(claim_min_idle_ms)),
                count=int(redis_ai_requeue_max_items),
            )
        )
    except BaseException as err:
        if isinstance(err, fatal_exceptions):
            raise
        has_error = True
        if verbose:
            print(
                f"[redis] claim_stuck_error {type(err).__name__}: {err}",
                flush=True,
            )

    return int(recovered), int(flushed_redis), bool(has_error)


__all__ = [
    "compute_redis_due_maintenance_delay_seconds",
    "maybe_run_redis_due_maintenance",
    "run_turso_maintenance",
]
