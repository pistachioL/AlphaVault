from __future__ import annotations

from concurrent.futures import Future
import json
import time
from typing import Any, Callable, Sequence

AI_TRACE_LOG_PREFIX = "[ai_trace]"


def _trace_log_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    return " ".join(str(value or "").split()).strip()


def _append_trace_field(parts: list[str], key: str, value: object) -> None:
    text = _trace_log_value(value)
    if not text:
        return
    parts.append(f"{key}={text}")


def _build_ai_trace_id(*, consumer_name: str, message_id: str, post_uid: str) -> str:
    parts = [
        _trace_log_value(consumer_name),
        _trace_log_value(message_id),
        _trace_log_value(post_uid),
    ]
    return "|".join(part for part in parts if part)


def _build_ai_trace_log_line(
    *,
    stage: str,
    trace_id: str = "",
    owner: str = "",
    consumer: str = "",
    queue: str = "",
    source: str = "",
    message_id: str = "",
    post_uid: str = "",
    delivery: str = "",
    available: int | None = None,
    claimed: int | None = None,
    read: int | None = None,
    pending: int | None = None,
    unread: int | None = None,
    retry: int | None = None,
    backlog: int | None = None,
    inflight: int | None = None,
    consumer_pending: int | None = None,
    consumer_idle_ms: int | None = None,
) -> str:
    parts = [AI_TRACE_LOG_PREFIX, f"stage={_trace_log_value(stage)}"]
    _append_trace_field(parts, "trace_id", trace_id)
    _append_trace_field(parts, "owner", owner)
    _append_trace_field(parts, "consumer", consumer)
    _append_trace_field(parts, "queue", queue)
    _append_trace_field(parts, "source", source)
    _append_trace_field(parts, "message_id", message_id)
    _append_trace_field(parts, "post_uid", post_uid)
    _append_trace_field(parts, "delivery", delivery)
    if available is not None:
        _append_trace_field(parts, "available", int(available))
    if claimed is not None:
        _append_trace_field(parts, "claimed", int(claimed))
    if read is not None:
        _append_trace_field(parts, "read", int(read))
    if pending is not None:
        _append_trace_field(parts, "pending", int(pending))
    if unread is not None:
        _append_trace_field(parts, "unread", int(unread))
    if retry is not None:
        _append_trace_field(parts, "retry", int(retry))
    if backlog is not None:
        _append_trace_field(parts, "backlog", int(backlog))
    if inflight is not None:
        _append_trace_field(parts, "inflight", int(inflight))
    if consumer_pending is not None:
        _append_trace_field(parts, "consumer_pending", int(consumer_pending))
    if consumer_idle_ms is not None:
        _append_trace_field(parts, "consumer_idle_ms", int(consumer_idle_ms))
    return " ".join(parts)


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
    pressure_snapshot_fn: Callable[[Any, str], dict[str, int]] | None = None,
    consumer_snapshot_fn: Callable[..., dict[str, int]] | None = None,
) -> tuple[int, bool]:
    resolved_owner = str(inflight_owner or "").strip()
    resolved_consumer_name = str(consumer_name or "").strip()
    resolved_queue_key = str(redis_queue_key or "").strip()
    resolved_source_name = str(source_name or "").strip()
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
    pressure_snapshot = {
        "pending_count": 0,
        "unread_count": 0,
        "retry_count": 0,
        "total_backlog": 0,
    }
    if pressure_snapshot_fn is not None:
        try:
            pressure_snapshot = pressure_snapshot_fn(redis_client, redis_queue_key)
        except BaseException as err:
            if isinstance(err, fatal_exceptions):
                raise
            if verbose:
                print(
                    f"[ai] redis_pressure_snapshot_error owner={inflight_owner} "
                    f"{type(err).__name__}: {err}",
                    flush=True,
                )
    consumer_snapshot = {
        "consumer_pending_count": 0,
        "consumer_idle_ms": 0,
    }
    if consumer_snapshot_fn is not None:
        try:
            consumer_snapshot = consumer_snapshot_fn(
                redis_client,
                redis_queue_key,
                consumer_name=resolved_consumer_name,
            )
        except BaseException as err:
            if isinstance(err, fatal_exceptions):
                raise
            if verbose:
                print(
                    f"[ai] redis_consumer_snapshot_error owner={inflight_owner} "
                    f"{type(err).__name__}: {err}",
                    flush=True,
                )
    if verbose and int(pressure_snapshot.get("total_backlog") or 0) > 0:
        print(
            _build_ai_trace_log_line(
                stage="poll_snapshot",
                owner=resolved_owner,
                consumer=resolved_consumer_name,
                queue=resolved_queue_key,
                source=resolved_source_name,
                available=int(available),
                inflight=int(rss_inflight_now),
                pending=int(pressure_snapshot.get("pending_count") or 0),
                unread=int(pressure_snapshot.get("unread_count") or 0),
                retry=int(pressure_snapshot.get("retry_count") or 0),
                backlog=int(pressure_snapshot.get("total_backlog") or 0),
                consumer_pending=int(
                    consumer_snapshot.get("consumer_pending_count") or 0
                ),
                consumer_idle_ms=int(consumer_snapshot.get("consumer_idle_ms") or 0),
            ),
            flush=True,
        )
    if available <= 0:
        return 0, False

    messages: list[tuple[str, dict[str, str]]] = []
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
        claimed_messages = claim_stuck_messages_fn(
            redis_client,
            redis_queue_key,
            consumer_name=resolved_consumer_name,
            min_idle_ms=max(1, int(stuck_seconds)) * 1000,
            count=int(available),
        )
        if verbose and claimed_messages:
            print(
                _build_ai_trace_log_line(
                    stage="redis_claim_done",
                    owner=resolved_owner,
                    consumer=resolved_consumer_name,
                    queue=resolved_queue_key,
                    source=resolved_source_name,
                    available=int(available),
                    claimed=len(claimed_messages),
                ),
                flush=True,
            )
        messages.extend(("claim", message) for message in claimed_messages)
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
            read_messages = read_group_messages_fn(
                redis_client,
                redis_queue_key,
                consumer_name=resolved_consumer_name,
                count=int(remaining),
            )
            if verbose and read_messages:
                print(
                    _build_ai_trace_log_line(
                        stage="redis_read_done",
                        owner=resolved_owner,
                        consumer=resolved_consumer_name,
                        queue=resolved_queue_key,
                        source=resolved_source_name,
                        available=int(available),
                        read=len(read_messages),
                    ),
                    flush=True,
                )
            messages.extend(("read", message) for message in read_messages)
        except BaseException as err:
            if isinstance(err, fatal_exceptions):
                raise
            if verbose:
                print(
                    f"[ai] redis_read_error owner={inflight_owner} {type(err).__name__}: {err}",
                    flush=True,
                )
            return 0, True

    if (
        verbose
        and not messages
        and int(pressure_snapshot.get("total_backlog") or 0) > 0
    ):
        print(
            _build_ai_trace_log_line(
                stage="poll_no_messages",
                owner=resolved_owner,
                consumer=resolved_consumer_name,
                queue=resolved_queue_key,
                source=resolved_source_name,
                available=int(available),
                inflight=int(rss_inflight_now),
                pending=int(pressure_snapshot.get("pending_count") or 0),
                unread=int(pressure_snapshot.get("unread_count") or 0),
                retry=int(pressure_snapshot.get("retry_count") or 0),
                backlog=int(pressure_snapshot.get("total_backlog") or 0),
                consumer_pending=int(
                    consumer_snapshot.get("consumer_pending_count") or 0
                ),
                consumer_idle_ms=int(consumer_snapshot.get("consumer_idle_ms") or 0),
            ),
            flush=True,
        )

    scheduled = 0
    for delivery, message in messages:
        if scheduled >= available:
            break
        message_id = str(message.get("message_id") or "").strip()
        payload_text = str(message.get("payload") or "").strip()
        if not message_id or not payload_text:
            continue
        try:
            payload = json.loads(payload_text)
        except Exception as err:
            trace_id = _build_ai_trace_id(
                consumer_name=resolved_consumer_name,
                message_id=str(message_id),
                post_uid="",
            )
            if verbose:
                print(
                    " ".join(
                        [
                            _build_ai_trace_log_line(
                                stage="bad_payload_ack",
                                trace_id=trace_id,
                                owner=resolved_owner,
                                consumer=resolved_consumer_name,
                                queue=resolved_queue_key,
                                source=resolved_source_name,
                                message_id=str(message_id),
                                delivery=str(delivery),
                            ),
                            f"{type(err).__name__}: {err}",
                        ]
                    ),
                    flush=True,
                )
            try:
                ack_message_fn(redis_client, redis_queue_key, str(message_id))
            except Exception:
                pass
            continue
        if not isinstance(payload, dict):
            trace_id = _build_ai_trace_id(
                consumer_name=resolved_consumer_name,
                message_id=str(message_id),
                post_uid="",
            )
            if verbose:
                print(
                    _build_ai_trace_log_line(
                        stage="bad_payload_ack",
                        trace_id=trace_id,
                        owner=resolved_owner,
                        consumer=resolved_consumer_name,
                        queue=resolved_queue_key,
                        source=resolved_source_name,
                        message_id=str(message_id),
                        delivery=str(delivery),
                    ),
                    flush=True,
                )
            try:
                ack_message_fn(redis_client, redis_queue_key, str(message_id))
            except Exception:
                pass
            continue
        resolved_post_uid = str(payload.get("post_uid") or "").strip()
        if not resolved_post_uid:
            trace_id = _build_ai_trace_id(
                consumer_name=resolved_consumer_name,
                message_id=str(message_id),
                post_uid="",
            )
            if verbose:
                print(
                    _build_ai_trace_log_line(
                        stage="bad_payload_ack",
                        trace_id=trace_id,
                        owner=resolved_owner,
                        consumer=resolved_consumer_name,
                        queue=resolved_queue_key,
                        source=resolved_source_name,
                        message_id=str(message_id),
                        delivery=str(delivery),
                    ),
                    flush=True,
                )
            try:
                ack_message_fn(redis_client, redis_queue_key, str(message_id))
            except Exception:
                pass
            continue
        trace_id = _build_ai_trace_id(
            consumer_name=resolved_consumer_name,
            message_id=str(message_id),
            post_uid=resolved_post_uid,
        )
        if verbose:
            print(
                _build_ai_trace_log_line(
                    stage="submit_prepare",
                    trace_id=trace_id,
                    owner=resolved_owner,
                    consumer=resolved_consumer_name,
                    queue=resolved_queue_key,
                    source=resolved_source_name,
                    message_id=str(message_id),
                    post_uid=resolved_post_uid,
                    delivery=str(delivery),
                ),
                flush=True,
            )

        fut = executor.submit(
            process_one_redis_payload_fn,
            engine=engine,
            payload=payload,
            message_id=str(message_id),
            redis_client=redis_client,
            redis_queue_key=redis_queue_key,
            consumer_name=resolved_consumer_name,
            source_name=str(source_name or "").strip(),
            config=config,
            limiter=limiter,
            verbose=bool(verbose),
            trace_id=trace_id,
            max_retry_count=max(0, int(getattr(config, "ai_retries", 0) or 0)),
        )
        fut.add_done_callback(lambda _f: wakeup_event.set())
        inflight_futures.add(fut)
        inflight_owner_by_future[fut] = resolved_owner
        if verbose:
            print(
                _build_ai_trace_log_line(
                    stage="submit_done",
                    trace_id=trace_id,
                    owner=resolved_owner,
                    consumer=resolved_consumer_name,
                    queue=resolved_queue_key,
                    source=resolved_source_name,
                    message_id=str(message_id),
                    post_uid=resolved_post_uid,
                    delivery=str(delivery),
                ),
                flush=True,
            )
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
