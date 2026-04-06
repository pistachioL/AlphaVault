from __future__ import annotations

from concurrent.futures import Future
import json
from pathlib import Path
from typing import Any, Callable, Sequence


def compute_low_priority_budget(*, ai_cap: int, rss_inflight_now: int) -> int:
    return max(0, int(ai_cap) - max(0, int(rss_inflight_now)))


def compute_rss_available_slots(
    *,
    ai_cap: int,
    rss_inflight_now: int,
    low_inflight_now: int,
) -> int:
    remaining = (
        int(ai_cap) - max(0, int(rss_inflight_now)) - max(0, int(low_inflight_now))
    )
    return max(0, remaining)


def build_low_priority_should_continue(
    *,
    ai_cap: int,
    rss_inflight_now_get: Callable[[], int],
    low_inflight_now_get: Callable[[], int] | None = None,
    has_due_ai_pending_get: Callable[[], bool] | None = None,
) -> Callable[[], bool]:
    def _should_continue() -> bool:
        rss_now = max(0, int(rss_inflight_now_get()))
        low_now = (
            max(0, int(low_inflight_now_get()))
            if low_inflight_now_get is not None
            else 0
        )
        available = compute_rss_available_slots(
            ai_cap=int(ai_cap),
            rss_inflight_now=int(rss_now),
            low_inflight_now=int(low_now),
        )
        if available > 0:
            return True
        if has_due_ai_pending_get is None:
            return False
        return not bool(has_due_ai_pending_get())

    return _should_continue


def dedup_post_uids(post_uids: Sequence[object]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in post_uids:
        post_uid = str(value or "").strip()
        if not post_uid or post_uid in seen:
            continue
        seen.add(post_uid)
        deduped.append(post_uid)
    return deduped


def schedule_ai_from_redis(
    *,
    executor: Any,
    engine: Any,
    ai_cap: int,
    low_inflight_now_get: Callable[[], int],
    inflight_futures: set[Future],
    inflight_owner_by_future: dict[Future, str],
    inflight_owner: str,
    wakeup_event: Any,
    config: Any,
    limiter: Any,
    verbose: bool,
    redis_client: Any,
    redis_queue_key: str,
    source_name: str = "",
    spool_dir: Path | str,
    lease_seconds: int = 3600,
    prune_inflight_futures_fn: Callable[[set[Future], dict[Future, str]], None],
    compute_rss_available_slots_fn: Callable[..., int],
    pop_to_processing_fn: Callable[..., str | None],
    ack_processing_fn: Callable[..., None],
    process_one_redis_payload_fn: Callable[..., None],
    fatal_exceptions: tuple[type[BaseException], ...],
) -> tuple[int, bool]:
    prune_inflight_futures_fn(inflight_futures, inflight_owner_by_future)
    rss_inflight_now = int(len(inflight_futures))
    try:
        low_inflight_now = max(0, int(low_inflight_now_get()))
    except Exception:
        low_inflight_now = 0
    available = compute_rss_available_slots_fn(
        ai_cap=int(ai_cap),
        rss_inflight_now=int(rss_inflight_now),
        low_inflight_now=int(low_inflight_now),
    )
    if available <= 0:
        return 0, False

    scheduled = 0
    for _ in range(max(1, int(available))):
        if scheduled >= available:
            break
        try:
            msg = pop_to_processing_fn(redis_client, redis_queue_key)
        except BaseException as err:
            if isinstance(err, fatal_exceptions):
                raise
            if verbose:
                print(
                    f"[ai] redis_pop_error owner={inflight_owner} {type(err).__name__}: {err}",
                    flush=True,
                )
            return scheduled, True
        if not msg:
            break
        try:
            payload = json.loads(msg)
        except Exception as err:
            if verbose:
                print(f"[ai] redis_bad_payload {type(err).__name__}: {err}", flush=True)
            try:
                ack_processing_fn(redis_client, redis_queue_key, str(msg))
            except Exception:
                pass
            continue
        if not isinstance(payload, dict):
            try:
                ack_processing_fn(redis_client, redis_queue_key, str(msg))
            except Exception:
                pass
            continue
        if not str(payload.get("post_uid") or "").strip():
            try:
                ack_processing_fn(redis_client, redis_queue_key, str(msg))
            except Exception:
                pass
            continue

        fut = executor.submit(
            process_one_redis_payload_fn,
            engine=engine,
            payload=payload,
            processing_msg=str(msg),
            redis_client=redis_client,
            redis_queue_key=redis_queue_key,
            source_name=str(source_name or "").strip(),
            spool_dir=spool_dir,
            config=config,
            limiter=limiter,
            verbose=bool(verbose),
            lease_seconds=max(1, int(lease_seconds)),
        )
        fut.add_done_callback(lambda _f: wakeup_event.set())
        inflight_futures.add(fut)
        inflight_owner_by_future[fut] = str(inflight_owner or "").strip()
        scheduled += 1
    return scheduled, False


def schedule_ai(
    *,
    executor: Any,
    engine: Any,
    platform: str,
    ai_cap: int,
    low_inflight_now_get: Callable[[], int],
    inflight_futures: set[Future],
    inflight_owner_by_future: dict[Future, str],
    inflight_owner: str,
    wakeup_event: Any,
    config: Any,
    limiter: Any,
    verbose: bool,
    redis_client: Any,
    redis_queue_key: str,
    source_name: str = "",
    spool_dir: Path | None,
    lease_seconds: int = 3600,
    schedule_ai_from_redis_fn: Callable[..., tuple[int, bool]],
) -> tuple[int, bool]:
    if engine is None:
        return 0, False
    has_redis_queue = bool(redis_client) and bool(str(redis_queue_key or "").strip())
    if not has_redis_queue or spool_dir is None:
        if verbose:
            print(
                f"[ai] redis_required owner={inflight_owner} platform={platform}",
                flush=True,
            )
        return 0, True
    return schedule_ai_from_redis_fn(
        executor=executor,
        engine=engine,
        ai_cap=ai_cap,
        low_inflight_now_get=low_inflight_now_get,
        inflight_futures=inflight_futures,
        inflight_owner_by_future=inflight_owner_by_future,
        inflight_owner=inflight_owner,
        wakeup_event=wakeup_event,
        config=config,
        limiter=limiter,
        verbose=bool(verbose),
        redis_client=redis_client,
        redis_queue_key=str(redis_queue_key),
        source_name=str(source_name or "").strip(),
        spool_dir=spool_dir,
        lease_seconds=max(1, int(lease_seconds)),
    )


__all__ = [
    "build_low_priority_should_continue",
    "compute_low_priority_budget",
    "compute_rss_available_slots",
    "dedup_post_uids",
    "schedule_ai",
    "schedule_ai_from_redis",
]
