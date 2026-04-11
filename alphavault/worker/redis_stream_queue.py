from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from alphavault.constants import (
    DEFAULT_REDIS_AI_QUEUE_MAXLEN,
    DEFAULT_REDIS_DEDUP_TTL_SECONDS,
    ENV_REDIS_AI_QUEUE_MAXLEN,
    ENV_REDIS_DEDUP_TTL_SECONDS,
)


REDIS_PUSH_STATUS_PUSHED = "pushed"
REDIS_PUSH_STATUS_DUPLICATE = "duplicate"
REDIS_PUSH_STATUS_ERROR = "error"
REDIS_AI_CONSUMER_GROUP = "alphavault-ai"


def _sha1_short(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:20]


def _redis_dedup_key(queue_key: str, post_uid: str) -> str:
    return f"{queue_key}:dedup:{_sha1_short(post_uid)}"


def redis_ai_stream_key(queue_key: str) -> str:
    return f"{queue_key}:ai:stream"


def redis_ai_retry_key(queue_key: str) -> str:
    return f"{queue_key}:ai:retry"


def build_redis_ai_consumer_name(owner: str) -> str:
    resolved_owner = str(owner or "").strip() or "worker"
    return f"{resolved_owner}:{os.getpid()}"


def _env_int_or_default(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return max(minimum, int(default))
    try:
        value = int(raw)
    except Exception:
        return max(minimum, int(default))
    return max(minimum, int(value))


REDIS_DEDUP_TTL_SECONDS = _env_int_or_default(
    ENV_REDIS_DEDUP_TTL_SECONDS,
    int(DEFAULT_REDIS_DEDUP_TTL_SECONDS),
    minimum=1,
)
REDIS_AI_QUEUE_MAXLEN = _env_int_or_default(
    ENV_REDIS_AI_QUEUE_MAXLEN,
    int(DEFAULT_REDIS_AI_QUEUE_MAXLEN),
    minimum=100,
)


def resolve_redis_dedup_ttl_seconds() -> int:
    return int(REDIS_DEDUP_TTL_SECONDS)


def resolve_redis_ai_queue_maxlen() -> int:
    return int(REDIS_AI_QUEUE_MAXLEN)


def redis_ensure_ai_consumer_group(client, queue_key: str) -> None:
    if not client or not str(queue_key or "").strip():
        return
    try:
        client.xgroup_create(
            redis_ai_stream_key(queue_key),
            REDIS_AI_CONSUMER_GROUP,
            id="0",
            mkstream=True,
        )
    except Exception as err:
        if "BUSYGROUP" not in str(err):
            raise


def redis_ai_reset_consumer_group(client, queue_key: str, *, verbose: bool) -> bool:
    """
    Reset consumer group metadata (pending list, consumers) for single-worker restarts.

    This keeps the stream messages but makes them readable again after a restart.
    """
    resolved_queue_key = str(queue_key or "").strip()
    if not client or not resolved_queue_key:
        return False

    stream_key = redis_ai_stream_key(resolved_queue_key)
    destroyed = 0
    try:
        destroyed = int(client.xgroup_destroy(stream_key, REDIS_AI_CONSUMER_GROUP) or 0)
    except Exception:
        destroyed = 0

    redis_ensure_ai_consumer_group(client, resolved_queue_key)
    if verbose:
        print(
            f"[redis] ai_group_reset queue={resolved_queue_key} destroyed={destroyed}",
            flush=True,
        )
    return True


def _xadd_payload(
    client,
    *,
    queue_key: str,
    payload_text: str,
    post_uid: str,
    queue_maxlen: int | None,
) -> str:
    del queue_maxlen
    return str(
        client.xadd(
            redis_ai_stream_key(queue_key),
            {"payload": payload_text, "post_uid": str(post_uid or "").strip()},
            approximate=True,
        )
    )


def _stream_backlog_count(client, queue_key: str) -> int:
    if not client or not str(queue_key or "").strip():
        return 0
    return max(
        0,
        int(client.xlen(redis_ai_stream_key(queue_key)) or 0),
    ) + max(
        0,
        int(client.zcard(redis_ai_retry_key(queue_key)) or 0),
    )


def redis_try_push_ai_message_status(
    client,
    queue_key: str,
    *,
    post_uid: str,
    payload: dict[str, Any],
    ttl_seconds: int,
    queue_maxlen: int | None = None,
    verbose: bool,
) -> str:
    resolved_queue_key = str(queue_key or "").strip()
    resolved_post_uid = str(post_uid or "").strip()
    if not client or not resolved_queue_key or not resolved_post_uid:
        return REDIS_PUSH_STATUS_ERROR

    payload_text = json.dumps(payload, ensure_ascii=False)
    dedup_key = _redis_dedup_key(resolved_queue_key, resolved_post_uid)
    try:
        ok = client.set(dedup_key, "1", nx=True, ex=max(1, int(ttl_seconds)))
    except Exception as err:
        if verbose:
            print(f"[redis] dedup_set_error {type(err).__name__}: {err}", flush=True)
        return REDIS_PUSH_STATUS_ERROR
    if not ok:
        return REDIS_PUSH_STATUS_DUPLICATE

    try:
        resolved_queue_maxlen = int(queue_maxlen or 0)
        if (
            resolved_queue_maxlen > 0
            and _stream_backlog_count(client, resolved_queue_key)
            >= resolved_queue_maxlen
        ):
            client.delete(dedup_key)
            return REDIS_PUSH_STATUS_ERROR
        redis_ensure_ai_consumer_group(client, resolved_queue_key)
        _xadd_payload(
            client,
            queue_key=resolved_queue_key,
            payload_text=payload_text,
            post_uid=resolved_post_uid,
            queue_maxlen=queue_maxlen,
        )
    except Exception as err:
        try:
            client.delete(dedup_key)
        except Exception:
            pass
        if verbose:
            print(f"[redis] stream_push_error {type(err).__name__}: {err}", flush=True)
        return REDIS_PUSH_STATUS_ERROR
    return REDIS_PUSH_STATUS_PUSHED


def redis_force_push_ai_message(
    client,
    queue_key: str,
    *,
    payload: dict[str, Any],
) -> str:
    resolved_queue_key = str(queue_key or "").strip()
    if not client or not resolved_queue_key:
        raise RuntimeError("missing_redis_queue")
    payload_text = json.dumps(payload, ensure_ascii=False)
    post_uid = str(payload.get("post_uid") or "").strip()
    redis_ensure_ai_consumer_group(client, resolved_queue_key)
    return _xadd_payload(
        client,
        queue_key=resolved_queue_key,
        payload_text=payload_text,
        post_uid=post_uid,
        queue_maxlen=resolve_redis_ai_queue_maxlen(),
    )


def _extract_stream_messages(entries: object) -> list[dict[str, str]]:
    resolved: list[dict[str, str]] = []
    if not isinstance(entries, list):
        return resolved
    for item in entries:
        if not isinstance(item, tuple) or len(item) != 2:
            continue
        _stream_name, messages = item
        if not isinstance(messages, list):
            continue
        for message in messages:
            if not isinstance(message, tuple) or len(message) != 2:
                continue
            message_id, fields = message
            if not isinstance(fields, dict):
                continue
            payload = str(fields.get("payload") or "").strip()
            if not payload:
                continue
            resolved.append(
                {
                    "message_id": str(message_id or "").strip(),
                    "payload": payload,
                }
            )
    return resolved


def redis_ai_read_group_messages(
    client,
    queue_key: str,
    *,
    consumer_name: str,
    count: int,
) -> list[dict[str, str]]:
    if not client or not str(queue_key or "").strip() or count <= 0:
        return []
    redis_ensure_ai_consumer_group(client, queue_key)
    entries = client.xreadgroup(
        REDIS_AI_CONSUMER_GROUP,
        str(consumer_name or "").strip(),
        {redis_ai_stream_key(queue_key): ">"},
        count=max(1, int(count)),
    )
    return _extract_stream_messages(entries)


def redis_ai_claim_stuck_messages(
    client,
    queue_key: str,
    *,
    consumer_name: str,
    min_idle_ms: int,
    count: int,
) -> list[dict[str, str]]:
    if not client or not str(queue_key or "").strip() or count <= 0:
        return []
    redis_ensure_ai_consumer_group(client, queue_key)
    _next_id, entries, _deleted_ids = client.xautoclaim(
        redis_ai_stream_key(queue_key),
        REDIS_AI_CONSUMER_GROUP,
        str(consumer_name or "").strip(),
        max(1, int(min_idle_ms)),
        start_id="0-0",
        count=max(1, int(count)),
    )
    return _extract_stream_messages([(redis_ai_stream_key(queue_key), entries)])


def redis_ai_ack(client, queue_key: str, message_id: str) -> int:
    if (
        not client
        or not str(queue_key or "").strip()
        or not str(message_id or "").strip()
    ):
        return 0
    pipe = client.pipeline(transaction=True)
    pipe.xack(
        redis_ai_stream_key(queue_key),
        REDIS_AI_CONSUMER_GROUP,
        str(message_id),
    )
    pipe.xdel(redis_ai_stream_key(queue_key), str(message_id))
    result = pipe.execute() or [0, 0]
    return int(result[0] or 0)


def redis_ai_ack_and_clear_dedup(
    client,
    queue_key: str,
    *,
    message_id: str,
    post_uid: str,
) -> tuple[int, int]:
    resolved_queue_key = str(queue_key or "").strip()
    resolved_message_id = str(message_id or "").strip()
    resolved_post_uid = str(post_uid or "").strip()
    if (
        not client
        or not resolved_queue_key
        or not resolved_message_id
        or not resolved_post_uid
    ):
        raise RuntimeError("missing_redis_queue")
    pipe = client.pipeline(transaction=True)
    pipe.xack(
        redis_ai_stream_key(resolved_queue_key),
        REDIS_AI_CONSUMER_GROUP,
        resolved_message_id,
    )
    pipe.xdel(redis_ai_stream_key(resolved_queue_key), resolved_message_id)
    pipe.delete(_redis_dedup_key(resolved_queue_key, resolved_post_uid))
    result = pipe.execute() or [0, 0, 0]
    acked = int(result[0] or 0) if len(result) > 0 else 0
    deleted = int(result[2] or 0) if len(result) > 2 else 0
    return int(acked), int(deleted)


def redis_ai_push_retry(
    client,
    queue_key: str,
    *,
    payload: dict[str, Any],
    next_retry_at: int,
) -> None:
    if not client or not str(queue_key or "").strip():
        raise RuntimeError("missing_redis_queue")
    payload_text = json.dumps(payload, ensure_ascii=False)
    client.zadd(redis_ai_retry_key(queue_key), {payload_text: int(next_retry_at)})


def redis_ai_ack_and_push_retry(
    client,
    queue_key: str,
    *,
    message_id: str,
    payload: dict[str, Any],
    next_retry_at: int,
) -> tuple[int, int]:
    resolved_queue_key = str(queue_key or "").strip()
    resolved_message_id = str(message_id or "").strip()
    if not client or not resolved_queue_key or not resolved_message_id:
        raise RuntimeError("missing_redis_queue")
    payload_text = json.dumps(payload, ensure_ascii=False)
    pipe = client.pipeline(transaction=True)
    pipe.zadd(
        redis_ai_retry_key(resolved_queue_key), {payload_text: int(next_retry_at)}
    )
    pipe.xack(
        redis_ai_stream_key(resolved_queue_key),
        REDIS_AI_CONSUMER_GROUP,
        resolved_message_id,
    )
    pipe.xdel(redis_ai_stream_key(resolved_queue_key), resolved_message_id)
    result = pipe.execute() or [0, 0, 0]
    retry_added = int(result[0] or 0) if len(result) > 0 else 0
    acked = int(result[1] or 0) if len(result) > 1 else 0
    return int(retry_added), int(acked)


def redis_ai_move_due_retries_to_stream(
    client,
    queue_key: str,
    *,
    now_epoch: int,
    max_items: int,
    verbose: bool,
) -> int:
    if not client or not str(queue_key or "").strip() or max_items <= 0:
        return 0
    retry_key = redis_ai_retry_key(queue_key)
    due_payloads = list(
        client.zrangebyscore(
            retry_key,
            min="-inf",
            max=int(now_epoch),
            start=0,
            num=max(1, int(max_items)),
        )
        or []
    )
    if not due_payloads:
        return 0

    redis_ensure_ai_consumer_group(client, str(queue_key))
    moved = 0
    for payload_text in due_payloads:
        resolved_payload_text = str(payload_text or "").strip()
        if not resolved_payload_text:
            continue
        try:
            payload = json.loads(resolved_payload_text)
        except Exception:
            client.zrem(retry_key, payload_text)
            continue
        if not isinstance(payload, dict):
            client.zrem(retry_key, payload_text)
            continue
        _xadd_payload(
            client,
            queue_key=str(queue_key),
            payload_text=resolved_payload_text,
            post_uid=str(payload.get("post_uid") or "").strip(),
            queue_maxlen=resolve_redis_ai_queue_maxlen(),
        )
        client.zrem(retry_key, payload_text)
        moved += 1
    if moved and verbose:
        print(f"[redis] ai_retry_to_stream moved={moved}", flush=True)
    return moved


def _pending_count_from_summary(summary: object) -> int:
    if isinstance(summary, dict):
        return max(0, int(summary.get("pending") or 0))
    return 0


def _unread_count_from_groups(groups: object) -> int:
    if not isinstance(groups, list):
        return 0
    for group in groups:
        if not isinstance(group, dict):
            continue
        if str(group.get("name") or "") != REDIS_AI_CONSUMER_GROUP:
            continue
        try:
            lag = int(group.get("lag") or 0)
        except Exception:
            lag = 0
        return max(0, lag)
    return 0


def redis_ai_consumer_snapshot(
    client,
    queue_key: str,
    *,
    consumer_name: str,
) -> dict[str, int]:
    if (
        not client
        or not str(queue_key or "").strip()
        or not str(consumer_name or "").strip()
    ):
        return {
            "consumer_pending_count": 0,
            "consumer_idle_ms": 0,
        }
    redis_ensure_ai_consumer_group(client, queue_key)
    try:
        consumers = client.xinfo_consumers(
            redis_ai_stream_key(queue_key),
            REDIS_AI_CONSUMER_GROUP,
        )
    except Exception:
        return {
            "consumer_pending_count": 0,
            "consumer_idle_ms": 0,
        }
    if not isinstance(consumers, list):
        return {
            "consumer_pending_count": 0,
            "consumer_idle_ms": 0,
        }
    resolved_consumer_name = str(consumer_name or "").strip()
    for consumer in consumers:
        if not isinstance(consumer, dict):
            continue
        if str(consumer.get("name") or "").strip() != resolved_consumer_name:
            continue
        try:
            pending = int(consumer.get("pending") or 0)
        except Exception:
            pending = 0
        try:
            idle_ms = int(consumer.get("idle") or 0)
        except Exception:
            idle_ms = 0
        return {
            "consumer_pending_count": max(0, pending),
            "consumer_idle_ms": max(0, idle_ms),
        }
    return {
        "consumer_pending_count": 0,
        "consumer_idle_ms": 0,
    }


def redis_ai_pressure_snapshot(client, queue_key: str) -> dict[str, int]:
    if not client or not str(queue_key or "").strip():
        return {
            "pending_count": 0,
            "unread_count": 0,
            "retry_count": 0,
            "total_backlog": 0,
        }
    redis_ensure_ai_consumer_group(client, queue_key)
    pending_count = _pending_count_from_summary(
        client.xpending(redis_ai_stream_key(queue_key), REDIS_AI_CONSUMER_GROUP)
    )
    unread_count = _unread_count_from_groups(
        client.xinfo_groups(redis_ai_stream_key(queue_key))
    )
    retry_count = max(0, int(client.zcard(redis_ai_retry_key(queue_key)) or 0))
    total_backlog = int(pending_count + unread_count + retry_count)
    return {
        "pending_count": int(pending_count),
        "unread_count": int(unread_count),
        "retry_count": int(retry_count),
        "total_backlog": int(total_backlog),
    }


def redis_ai_due_count(client, queue_key: str, *, now_epoch: int) -> int:
    snapshot = redis_ai_pressure_snapshot(client, queue_key)
    due_retry_count = 0
    if client and str(queue_key or "").strip():
        due_retry_count = max(
            0,
            int(
                client.zcount(
                    redis_ai_retry_key(queue_key),
                    "-inf",
                    int(now_epoch),
                )
                or 0
            ),
        )
    return int(snapshot["pending_count"] + snapshot["unread_count"] + due_retry_count)


__all__ = [
    "REDIS_AI_CONSUMER_GROUP",
    "REDIS_PUSH_STATUS_DUPLICATE",
    "REDIS_PUSH_STATUS_ERROR",
    "REDIS_PUSH_STATUS_PUSHED",
    "build_redis_ai_consumer_name",
    "redis_ai_ack",
    "redis_ai_ack_and_clear_dedup",
    "redis_ai_ack_and_push_retry",
    "redis_ai_consumer_snapshot",
    "redis_ai_reset_consumer_group",
    "redis_ai_claim_stuck_messages",
    "redis_ai_due_count",
    "redis_ai_move_due_retries_to_stream",
    "redis_ai_pressure_snapshot",
    "redis_ai_push_retry",
    "redis_ai_read_group_messages",
    "redis_ai_retry_key",
    "redis_ai_stream_key",
    "redis_ensure_ai_consumer_group",
    "redis_force_push_ai_message",
    "redis_try_push_ai_message_status",
    "resolve_redis_ai_queue_maxlen",
    "resolve_redis_dedup_ttl_seconds",
]
