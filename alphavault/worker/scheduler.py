from __future__ import annotations

from concurrent.futures import Future
import json
import time
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


def schedule_ai_from_stream(
    *,
    executor: Any,
    engine: Any,
    ai_cap: int,
    low_inflight_now_get: Callable[[], int],
    inflight_futures: set[Future],
    inflight_owner_by_future: dict[Future, str],
    inflight_owner: str,
    consumer_name: str,
    wakeup_event: Any,
    config: Any,
    limiter: Any,
    verbose: bool,
    redis_client: Any,
    redis_queue_key: str,
    source_name: str = "",
    prune_inflight_futures_fn: Callable[[set[Future], dict[Future, str]], None],
    compute_rss_available_slots_fn: Callable[..., int],
    move_due_retry_to_stream_fn: Callable[..., int],
    claim_stuck_messages_fn: Callable[..., list[dict[str, str]]],
    read_group_messages_fn: Callable[..., list[dict[str, str]]],
    ack_message_fn: Callable[..., None],
    process_one_redis_payload_fn: Callable[..., None],
    fatal_exceptions: tuple[type[BaseException], ...],
    stuck_seconds: int,
) -> tuple[int, bool]:
    prune_inflight_futures_fn(inflight_futures, inflight_owner_by_future)
    rss_inflight_now = int(len(inflight_futures))
    has_error = False
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

    messages: list[dict[str, str]] = []
    try:
        move_due_retry_to_stream_fn(
            redis_client,
            redis_queue_key,
            now_epoch=int(time.time()),
            max_items=int(available),
            verbose=bool(verbose),
        )
    except BaseException as err:
        if isinstance(err, fatal_exceptions):
            raise
        has_error = True
        if verbose:
            print(
                f"[ai] redis_retry_to_stream_error owner={inflight_owner} "
                f"{type(err).__name__}: {err}",
                flush=True,
            )
    try:
        messages.extend(
            claim_stuck_messages_fn(
                redis_client,
                redis_queue_key,
                consumer_name=str(consumer_name or "").strip(),
                min_idle_ms=max(1, int(stuck_seconds)) * 1000,
                count=int(available),
            )
        )
    except BaseException as err:
        if isinstance(err, fatal_exceptions):
            raise
        if verbose:
            print(
                f"[ai] redis_claim_error owner={inflight_owner} {type(err).__name__}: {err}",
                flush=True,
            )
        return 0, True

    remaining = max(0, int(available) - len(messages))
    if remaining > 0:
        try:
            messages.extend(
                read_group_messages_fn(
                    redis_client,
                    redis_queue_key,
                    consumer_name=str(consumer_name or "").strip(),
                    count=int(remaining),
                )
            )
        except BaseException as err:
            if isinstance(err, fatal_exceptions):
                raise
            if verbose:
                print(
                    f"[ai] redis_read_error owner={inflight_owner} {type(err).__name__}: {err}",
                    flush=True,
                )
            return 0, True

    scheduled = 0
    for message in messages:
        if scheduled >= available:
            break
        message_id = str(message.get("message_id") or "").strip()
        payload_text = str(message.get("payload") or "").strip()
        if not message_id or not payload_text:
            continue
        try:
            payload = json.loads(payload_text)
        except Exception as err:
            if verbose:
                print(
                    f"[ai] redis_bad_payload message_id={message_id} "
                    f"{type(err).__name__}: {err}",
                    flush=True,
                )
            try:
                ack_message_fn(redis_client, redis_queue_key, str(message_id))
            except Exception:
                pass
            continue
        if not isinstance(payload, dict):
            try:
                ack_message_fn(redis_client, redis_queue_key, str(message_id))
            except Exception:
                pass
            continue
        if not str(payload.get("post_uid") or "").strip():
            try:
                ack_message_fn(redis_client, redis_queue_key, str(message_id))
            except Exception:
                pass
            continue

        fut = executor.submit(
            process_one_redis_payload_fn,
            engine=engine,
            payload=payload,
            message_id=str(message_id),
            redis_client=redis_client,
            redis_queue_key=redis_queue_key,
            source_name=str(source_name or "").strip(),
            config=config,
            limiter=limiter,
            verbose=bool(verbose),
            max_retry_count=max(0, int(getattr(config, "ai_retries", 0) or 0)),
        )
        fut.add_done_callback(lambda _f: wakeup_event.set())
        inflight_futures.add(fut)
        inflight_owner_by_future[fut] = str(inflight_owner or "").strip()
        scheduled += 1
    return scheduled, bool(has_error)


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
    consumer_name: str,
    wakeup_event: Any,
    config: Any,
    limiter: Any,
    verbose: bool,
    redis_client: Any,
    redis_queue_key: str,
    source_name: str = "",
    schedule_ai_from_stream_fn: Callable[..., tuple[int, bool]],
    stuck_seconds: int = 3600,
) -> tuple[int, bool]:
    if engine is None:
        return 0, False
    has_redis_queue = bool(redis_client) and bool(str(redis_queue_key or "").strip())
    if not has_redis_queue:
        if verbose:
            print(
                f"[ai] redis_required owner={inflight_owner} platform={platform}",
                flush=True,
            )
        return 0, True
    return schedule_ai_from_stream_fn(
        executor=executor,
        engine=engine,
        ai_cap=ai_cap,
        low_inflight_now_get=low_inflight_now_get,
        inflight_futures=inflight_futures,
        inflight_owner_by_future=inflight_owner_by_future,
        inflight_owner=inflight_owner,
        consumer_name=consumer_name,
        wakeup_event=wakeup_event,
        config=config,
        limiter=limiter,
        verbose=bool(verbose),
        redis_client=redis_client,
        redis_queue_key=str(redis_queue_key),
        source_name=str(source_name or "").strip(),
        stuck_seconds=max(1, int(stuck_seconds)),
    )


__all__ = [
    "build_low_priority_should_continue",
    "compute_low_priority_budget",
    "compute_rss_available_slots",
    "dedup_post_uids",
    "schedule_ai",
    "schedule_ai_from_stream",
]
