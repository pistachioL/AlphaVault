from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading
from typing import Any

from alphavault.worker import periodic_jobs
from alphavault.worker.spool import _try_push_payload_to_ai_ready
from alphavault.worker.worker_constants import (
    REDIS_ENQUEUE_MAX_ITEMS_PER_RUN,
    REDIS_ENQUEUE_RETRY_INTERVAL_SECONDS,
)
from alphavault.worker.worker_loop_models import SourceTickContext


def submit_redis_enqueue_job(
    *,
    source,
    redis_client: Any,
    redis_queue_key: str,
    verbose: bool,
) -> dict[str, int | bool]:
    attempted = 0
    pushed = 0
    duplicates = 0
    has_error = False
    max_items = max(1, int(REDIS_ENQUEUE_MAX_ITEMS_PER_RUN))

    for _ in range(max_items):
        payload = periodic_jobs.pop_next_redis_enqueue_payload(source=source)
        if payload is None:
            break
        post_uid = str(payload.get("post_uid") or "")
        if not post_uid:
            continue
        attempted += 1
        pushed_count, push_error = _try_push_payload_to_ai_ready(
            redis_client=redis_client,
            redis_queue_key=redis_queue_key,
            post_uid=post_uid,
            payload=payload,
            verbose=bool(verbose),
        )
        if push_error:
            periodic_jobs.restore_redis_enqueue_payload(
                source=source,
                payload=payload,
            )
            has_error = True
            break
        if pushed_count > 0:
            pushed += int(pushed_count)
            continue
        duplicates += 1

    has_more = periodic_jobs.should_start_redis_enqueue(source=source)
    return {
        "attempted": int(attempted),
        "pushed": int(pushed),
        "duplicates": int(duplicates),
        "has_more": bool(has_more),
        "has_error": bool(has_error),
    }


def maybe_schedule_redis_enqueue(
    *,
    source,
    ctx: SourceTickContext,
    redis_enqueue_executor: ThreadPoolExecutor,
    wakeup_event: threading.Event,
) -> None:
    if (
        not ctx.redis_client
        or not str(getattr(source, "redis_queue_key", "") or "").strip()
    ):
        return
    if getattr(source, "redis_enqueue_future", None) is not None:
        return
    if not periodic_jobs.should_start_redis_enqueue(source=source):
        return
    if ctx.now < float(getattr(source, "redis_enqueue_next_at", 0.0) or 0.0):
        return

    periodic_jobs.mark_redis_enqueue_started(source=source)
    source.redis_enqueue_future = redis_enqueue_executor.submit(
        submit_redis_enqueue_job,
        source=source,
        redis_client=ctx.redis_client,
        redis_queue_key=str(source.redis_queue_key or ""),
        verbose=ctx.verbose,
    )
    source.redis_enqueue_future.add_done_callback(lambda _f: wakeup_event.set())
    source.redis_enqueue_next_at = ctx.now + float(REDIS_ENQUEUE_RETRY_INTERVAL_SECONDS)


__all__ = [
    "maybe_schedule_redis_enqueue",
    "submit_redis_enqueue_job",
]
