from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from alphavault.worker.redis_queue import (
    REDIS_PUSH_STATUS_DUPLICATE,
    REDIS_PUSH_STATUS_ERROR,
    REDIS_PUSH_STATUS_PUSHED,
)
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


def should_requeue_unprocessed_posts_from_db(
    *,
    source: Any,
    do_recovery: bool,
    redis_queue_has_pending_work: bool,
    source_has_running_jobs: bool,
) -> bool:
    if not bool(do_recovery):
        return False
    cycle_count = max(0, int(getattr(source, "maintenance_recovery_cycle_count", 0)))
    if cycle_count <= 1:
        return True
    return not bool(redis_queue_has_pending_work) and not bool(source_has_running_jobs)


def has_pending_ai_queue_work(
    *,
    redis_client: Any,
    redis_queue_key: str,
    verbose: bool,
    pending_count_fn: Callable[..., int],
    fatal_exceptions: tuple[type[BaseException], ...],
) -> bool:
    resolved_queue_key = str(redis_queue_key or "").strip()
    if not redis_client or not resolved_queue_key:
        return False
    try:
        return bool(pending_count_fn(redis_client, resolved_queue_key))
    except BaseException as err:
        if isinstance(err, fatal_exceptions):
            raise
        if verbose:
            print(
                f"[redis] ai_pending_count_error queue={resolved_queue_key} "
                f"{type(err).__name__}: {err}",
                flush=True,
            )
        return True


def requeue_unprocessed_posts_to_redis(
    *,
    engine: Any,
    platform: str,
    redis_client: Any,
    redis_queue_key: str,
    verbose: bool,
    max_items: int,
    load_unprocessed_posts_for_requeue_fn: Callable[..., list[dict[str, object]]],
    redis_try_push_ai_dedup_status_fn: Callable[..., str],
    resolve_redis_dedup_ttl_seconds_fn: Callable[[], int],
    clear_stale_ai_dedup_fn: Callable[..., bool] | None,
    fatal_exceptions: tuple[type[BaseException], ...],
) -> tuple[int, bool]:
    if engine is None:
        return 0, False
    if max_items <= 0:
        return 0, False
    if not redis_client or not str(redis_queue_key or "").strip():
        return 0, False

    resolved_platform = str(platform or "").strip().lower() or None
    try:
        rows = load_unprocessed_posts_for_requeue_fn(
            engine,
            limit=max(0, int(max_items)),
            platform=resolved_platform,
        )
    except BaseException as err:
        if isinstance(err, fatal_exceptions):
            raise
        if verbose:
            print(
                f"[redis] load_unprocessed_posts_error {type(err).__name__}: {err}",
                flush=True,
            )
        return 0, True

    pushed = 0
    ttl_seconds = max(1, int(resolve_redis_dedup_ttl_seconds_fn()))
    for row in rows:
        payload = dict(row or {})
        post_uid = str(payload.get("post_uid") or "").strip()
        if not post_uid:
            continue

        try:
            status = redis_try_push_ai_dedup_status_fn(
                redis_client,
                redis_queue_key,
                post_uid=post_uid,
                payload=payload,
                ttl_seconds=ttl_seconds,
                verbose=bool(verbose),
            )
        except BaseException as err:
            if isinstance(err, fatal_exceptions):
                raise
            if verbose:
                print(
                    f"[redis] unprocessed_requeue_error post_uid={post_uid} "
                    f"{type(err).__name__}: {err}",
                    flush=True,
                )
            return pushed, True

        if (
            status == REDIS_PUSH_STATUS_DUPLICATE
            and clear_stale_ai_dedup_fn is not None
        ):
            try:
                cleared = bool(
                    clear_stale_ai_dedup_fn(
                        redis_client,
                        redis_queue_key,
                        post_uid=post_uid,
                        verbose=bool(verbose),
                    )
                )
            except BaseException as err:
                if isinstance(err, fatal_exceptions):
                    raise
                if verbose:
                    print(
                        f"[redis] clear_stale_dedup_error post_uid={post_uid} "
                        f"{type(err).__name__}: {err}",
                        flush=True,
                    )
                return pushed, True
            if cleared:
                try:
                    status = redis_try_push_ai_dedup_status_fn(
                        redis_client,
                        redis_queue_key,
                        post_uid=post_uid,
                        payload=payload,
                        ttl_seconds=ttl_seconds,
                        verbose=bool(verbose),
                    )
                except BaseException as err:
                    if isinstance(err, fatal_exceptions):
                        raise
                    if verbose:
                        print(
                            f"[redis] stale_dedup_retry_error post_uid={post_uid} "
                            f"{type(err).__name__}: {err}",
                            flush=True,
                        )
                    return pushed, True

        if status == REDIS_PUSH_STATUS_ERROR:
            return pushed, True
        if status == REDIS_PUSH_STATUS_PUSHED:
            pushed += 1
    return pushed, False


def run_turso_maintenance(
    *,
    engine: Any,
    platform: str,
    spool_dir: Path | str,
    redis_client: Any,
    redis_queue_key: str,
    verbose: bool,
    do_recovery: bool,
    do_db_requeue: bool = False,
    now_fn: Callable[[], float],
    recover_spool_to_turso_and_redis_fn: Callable[..., tuple[int, int, int, bool]],
    load_unprocessed_posts_for_requeue_fn: Callable[..., list[dict[str, object]]]
    | None = None,
    redis_try_push_ai_dedup_status_fn: Callable[..., str] | None = None,
    resolve_redis_dedup_ttl_seconds_fn: Callable[[], int] | None = None,
    clear_stale_ai_dedup_fn: Callable[..., bool] | None = None,
    maybe_dispose_turso_engine_on_transient_error_fn: Callable[..., None],
    redis_ai_requeue_processing_without_lease_fn: Callable[..., int],
    fatal_exceptions: tuple[type[BaseException], ...],
    redis_ai_requeue_max_items: int,
) -> tuple[int, int, bool]:
    if engine is None:
        return 0, 0, False
    del now_fn

    platform_name = str(platform or "").strip().lower() or None

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
        if (
            bool(do_recovery)
            and bool(do_db_requeue)
            and flushed_redis <= 0
            and load_unprocessed_posts_for_requeue_fn is not None
            and redis_try_push_ai_dedup_status_fn is not None
            and resolve_redis_dedup_ttl_seconds_fn is not None
        ):
            requeued_posts, requeue_posts_error = requeue_unprocessed_posts_to_redis(
                engine=engine,
                platform=platform_name or "",
                redis_client=redis_client,
                redis_queue_key=redis_queue_key,
                verbose=bool(verbose),
                max_items=int(redis_ai_requeue_max_items),
                load_unprocessed_posts_for_requeue_fn=load_unprocessed_posts_for_requeue_fn,
                redis_try_push_ai_dedup_status_fn=redis_try_push_ai_dedup_status_fn,
                resolve_redis_dedup_ttl_seconds_fn=resolve_redis_dedup_ttl_seconds_fn,
                clear_stale_ai_dedup_fn=clear_stale_ai_dedup_fn,
                fatal_exceptions=fatal_exceptions,
            )
            flushed_redis += int(requeued_posts)
            flush_redis_error = bool(flush_redis_error or requeue_posts_error)
    turso_error = bool(turso_error or flush_redis_error)
    return recovered, flushed_redis, turso_error
