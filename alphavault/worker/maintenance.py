from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

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
    move_due_delayed_to_ready_fn: Callable[..., int],
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
        moved_due = move_due_delayed_to_ready_fn(
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
                f"[redis] ai_due_to_ready_error queue={queue_key} "
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


def should_run_maintenance_recovery(
    *,
    source: Any,
    maintenance_recovery_interval_cycles: int,
) -> bool:
    cycle_count = (
        max(0, int(getattr(source, "maintenance_recovery_cycle_count", 0))) + 1
    )
    source.maintenance_recovery_cycle_count = int(cycle_count)
    if cycle_count <= 1:
        return True
    if bool(getattr(source, "maintenance_recovery_force_next", False)):
        return True
    interval_cycles = max(1, int(maintenance_recovery_interval_cycles))
    return bool(cycle_count % interval_cycles == 0)


def update_maintenance_recovery_state(
    *,
    source: Any,
    recovered: int,
    maintenance_error: bool,
) -> None:
    source.maintenance_recovery_force_next = bool(
        int(max(0, int(recovered))) > 0 or bool(maintenance_error)
    )


def run_turso_maintenance(
    *,
    engine: Any,
    platform: str,
    spool_dir: Path | str,
    redis_client: Any,
    redis_queue_key: str,
    verbose: bool,
    do_recovery: bool,
    now_fn: Callable[[], float],
    recover_spool_to_turso_and_redis_fn: Callable[..., tuple[int, int, int, bool]],
    maybe_dispose_turso_engine_on_transient_error_fn: Callable[..., None],
    redis_ai_requeue_processing_without_lease_fn: Callable[..., int],
    fatal_exceptions: tuple[type[BaseException], ...],
    redis_ai_requeue_max_items: int,
) -> tuple[int, int, bool]:
    if engine is None:
        return 0, 0, False
    del platform
    del now_fn

    turso_error = False
    recovered = 0
    flushed_redis = 0
    if bool(do_recovery):
        try:
            (
                handled_spool,
                spool_queued,
                deleted_done_spool,
                spool_error,
            ) = recover_spool_to_turso_and_redis_fn(
                spool_dir=Path(spool_dir),
                engine=engine,
                max_items=int(redis_ai_requeue_max_items),
                verbose=bool(verbose),
                redis_client=redis_client,
                redis_queue_key=redis_queue_key,
            )
            del deleted_done_spool
            recovered += int(handled_spool)
            flushed_redis += int(spool_queued)
            turso_error = bool(turso_error or spool_error)
        except BaseException as err:
            if isinstance(err, fatal_exceptions):
                raise
            maybe_dispose_turso_engine_on_transient_error_fn(
                engine=engine,
                err=err,
                verbose=bool(verbose),
            )
            turso_error = True
            if verbose:
                print(f"[ai] recover_error {type(err).__name__}: {err}", flush=True)

    flush_redis_error = False
    if redis_client and str(redis_queue_key or "").strip():
        try:
            flushed_redis += int(
                redis_ai_requeue_processing_without_lease_fn(
                    redis_client,
                    redis_queue_key,
                    max_items=int(redis_ai_requeue_max_items),
                    verbose=bool(verbose),
                )
            )
        except BaseException as err:
            if isinstance(err, fatal_exceptions):
                raise
            flush_redis_error = True
            if verbose:
                print(f"[redis] flush_error {type(err).__name__}: {err}", flush=True)
    turso_error = bool(turso_error or flush_redis_error)
    return recovered, flushed_redis, turso_error
