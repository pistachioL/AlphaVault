from __future__ import annotations

import json
import os
from pathlib import Path
import time
from typing import Any, Dict, Optional

from alphavault.constants import (
    DEFAULT_REDIS_AI_QUEUE_MAXLEN,
    DEFAULT_REDIS_DEDUP_TTL_SECONDS,
    ENV_REDIS_AI_QUEUE_MAXLEN,
    ENV_REDIS_DEDUP_TTL_SECONDS,
    ENV_REDIS_QUEUE_KEY,
    ENV_REDIS_URL,
)
from alphavault.worker.spool import sha1_short, spool_delete


DEFAULT_REDIS_QUEUE_KEY = "alphavault:rss_spool"
REDIS_PUSH_STATUS_PUSHED = "pushed"
REDIS_PUSH_STATUS_DUPLICATE = "duplicate"
REDIS_PUSH_STATUS_ERROR = "error"
REDIS_AI_REQUEUE_SCAN_BATCH_SIZE = 200
_REDIS_AI_RELEASE_LEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"""


def try_get_redis():
    redis_url = os.getenv(ENV_REDIS_URL, "").strip()
    if not redis_url:
        return None, ""
    try:
        import redis  # type: ignore
    except Exception as e:
        print(f"[redis] disabled import_error {type(e).__name__}: {e}", flush=True)
        return None, ""
    try:
        client = redis.Redis.from_url(redis_url, decode_responses=True)
        client.ping()
    except Exception as e:
        print(f"[redis] disabled connect_error {type(e).__name__}: {e}", flush=True)
        return None, ""
    key = os.getenv(ENV_REDIS_QUEUE_KEY, "").strip() or DEFAULT_REDIS_QUEUE_KEY
    return client, key


def _redis_dedup_key(queue_key: str, post_uid: str) -> str:
    return f"{queue_key}:dedup:{sha1_short(post_uid)}"


def redis_ai_ready_key(queue_key: str) -> str:
    return f"{queue_key}:ai:ready"


def redis_ai_processing_key(queue_key: str) -> str:
    return f"{queue_key}:ai:processing"


def redis_ai_delayed_key(queue_key: str) -> str:
    return f"{queue_key}:ai:delayed"


def redis_ai_lease_key(queue_key: str, post_uid: str) -> str:
    return f"{queue_key}:lease:ai:{post_uid}"


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


def _redis_try_push_dedup_status(
    client,
    *,
    dedup_queue_key: str,
    push_queue_key: str,
    post_uid: str,
    payload: Dict[str, Any],
    ttl_seconds: int,
    queue_maxlen: int | None,
    verbose: bool,
) -> str:
    if not client or not dedup_queue_key or not push_queue_key or not post_uid:
        return REDIS_PUSH_STATUS_ERROR

    dedup_key = _redis_dedup_key(dedup_queue_key, post_uid)
    try:
        ok = client.set(dedup_key, "1", nx=True, ex=max(1, int(ttl_seconds)))
    except Exception as e:
        if verbose:
            print(f"[redis] dedup_set_error {type(e).__name__}: {e}", flush=True)
        return REDIS_PUSH_STATUS_ERROR
    if not ok:
        return REDIS_PUSH_STATUS_DUPLICATE

    try:
        _redis_push(client, push_queue_key, payload, maxlen=queue_maxlen)
        return REDIS_PUSH_STATUS_PUSHED
    except Exception as e:
        try:
            client.delete(dedup_key)
        except Exception:
            pass
        if verbose:
            print(f"[redis] push_error {type(e).__name__}: {e}", flush=True)
        return REDIS_PUSH_STATUS_ERROR


def redis_try_push_dedup_status(
    client,
    queue_key: str,
    *,
    post_uid: str,
    payload: Dict[str, Any],
    ttl_seconds: int,
    verbose: bool,
) -> str:
    return _redis_try_push_dedup_status(
        client,
        dedup_queue_key=queue_key,
        push_queue_key=queue_key,
        post_uid=post_uid,
        payload=payload,
        ttl_seconds=ttl_seconds,
        queue_maxlen=resolve_redis_ai_queue_maxlen(),
        verbose=verbose,
    )


def redis_try_push_dedup(
    client,
    queue_key: str,
    *,
    post_uid: str,
    payload: Dict[str, Any],
    ttl_seconds: int,
    verbose: bool,
) -> bool:
    return (
        redis_try_push_dedup_status(
            client,
            queue_key,
            post_uid=post_uid,
            payload=payload,
            ttl_seconds=ttl_seconds,
            verbose=verbose,
        )
        == REDIS_PUSH_STATUS_PUSHED
    )


def redis_try_push_ai_dedup_status(
    client,
    queue_key: str,
    *,
    post_uid: str,
    payload: Dict[str, Any],
    ttl_seconds: int,
    verbose: bool,
) -> str:
    return _redis_try_push_dedup_status(
        client,
        dedup_queue_key=queue_key,
        push_queue_key=redis_ai_ready_key(queue_key),
        post_uid=post_uid,
        payload=payload,
        ttl_seconds=ttl_seconds,
        queue_maxlen=resolve_redis_ai_queue_maxlen(),
        verbose=verbose,
    )


def _redis_push(
    client,
    queue_key: str,
    payload: Dict[str, Any],
    *,
    maxlen: int | None = None,
) -> None:
    msg = json.dumps(payload, ensure_ascii=False)
    if maxlen is not None and int(maxlen) > 0:
        capped = max(1, int(maxlen))
        pipe = client.pipeline()
        pipe.lpush(queue_key, msg)
        pipe.ltrim(queue_key, 0, capped - 1)
        pipe.execute()
        return
    client.lpush(queue_key, msg)


def _extract_post_uid_from_ai_message(msg: object) -> str:
    text = str(msg or "").strip()
    if not text:
        return ""
    try:
        payload = json.loads(text)
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("post_uid") or "").strip()


def _queue_contains_post_uid(
    client,
    queue_key: str,
    *,
    post_uid: str,
    sorted_set: bool,
) -> bool:
    resolved_queue_key = str(queue_key or "").strip()
    resolved_post_uid = str(post_uid or "").strip()
    if not client or not resolved_queue_key or not resolved_post_uid:
        return False
    scan_batch_size = max(1, int(REDIS_AI_REQUEUE_SCAN_BATCH_SIZE))
    if sorted_set:
        total = int(client.zcard(resolved_queue_key) or 0)
        if total <= 0:
            return False
        for start in range(0, total, scan_batch_size):
            msgs = list(
                client.zrange(resolved_queue_key, start, start + scan_batch_size - 1)
            )
            if any(
                _extract_post_uid_from_ai_message(msg) == resolved_post_uid
                for msg in msgs
            ):
                return True
        return False

    total = int(client.llen(resolved_queue_key) or 0)
    if total <= 0:
        return False
    for start in range(0, total, scan_batch_size):
        end = start + scan_batch_size - 1
        msgs = list(client.lrange(resolved_queue_key, start, end) or [])
        if any(
            _extract_post_uid_from_ai_message(msg) == resolved_post_uid for msg in msgs
        ):
            return True
    return False


def redis_ai_try_claim_lease(
    client,
    queue_key: str,
    *,
    post_uid: str,
    lease_seconds: int,
) -> str:
    resolved_queue_key = str(queue_key or "").strip()
    resolved_post_uid = str(post_uid or "").strip()
    if not client or not resolved_queue_key or not resolved_post_uid:
        return ""
    lease_key = redis_ai_lease_key(resolved_queue_key, resolved_post_uid)
    lease_token = f"{os.getpid()}:{time.time_ns()}:{sha1_short(resolved_post_uid)}"
    try:
        ok = client.set(
            lease_key,
            lease_token,
            nx=True,
            ex=max(1, int(lease_seconds)),
        )
    except Exception:
        raise
    return lease_token if ok else ""


def redis_ai_release_lease(
    client,
    queue_key: str,
    *,
    post_uid: str,
    lease_token: str,
    verbose: bool,
) -> bool:
    resolved_queue_key = str(queue_key or "").strip()
    resolved_post_uid = str(post_uid or "").strip()
    resolved_token = str(lease_token or "").strip()
    if (
        not client
        or not resolved_queue_key
        or not resolved_post_uid
        or not resolved_token
    ):
        return False
    lease_key = redis_ai_lease_key(resolved_queue_key, resolved_post_uid)
    try:
        deleted = int(
            client.eval(
                _REDIS_AI_RELEASE_LEASE_SCRIPT,
                1,
                lease_key,
                resolved_token,
            )
            or 0
        )
    except Exception as e:
        if verbose:
            print(
                f"[redis] ai_lease_release_error {type(e).__name__}: {e}",
                flush=True,
            )
        return False
    return deleted > 0


def redis_ai_requeue_processing_without_lease(
    client, queue_key: str, *, max_items: int, verbose: bool
) -> int:
    if max_items <= 0:
        return 0
    src = redis_ai_processing_key(queue_key)
    dst = redis_ai_ready_key(queue_key)
    moved = 0
    total = int(client.llen(src) or 0)
    scan_batch_size = max(1, int(REDIS_AI_REQUEUE_SCAN_BATCH_SIZE))
    next_end = total - 1
    while next_end >= 0:
        next_start = max(0, next_end - scan_batch_size + 1)
        msgs = list(client.lrange(src, next_start, next_end) or [])
        for msg in reversed(msgs):
            if moved >= int(max_items):
                break
            resolved_msg = str(msg or "")
            if not resolved_msg:
                continue
            post_uid = _extract_post_uid_from_ai_message(resolved_msg)
            if post_uid and client.exists(redis_ai_lease_key(queue_key, post_uid)):
                continue
            removed = client.lrem(src, 1, resolved_msg)
            if int(removed or 0) <= 0:
                continue
            if not post_uid:
                continue
            client.lpush(dst, resolved_msg)
            moved += 1
        if moved >= int(max_items):
            break
        next_end = next_start - 1
    if moved and verbose:
        print(f"[redis] ai_requeue moved={moved}", flush=True)
    return moved


def redis_ai_pop_to_processing(client, queue_key: str) -> Optional[str]:
    src = redis_ai_ready_key(queue_key)
    dst = redis_ai_processing_key(queue_key)
    msg = client.rpoplpush(src, dst)
    if not msg:
        return None
    return str(msg)


def redis_ai_ack_processing(client, queue_key: str, msg: str) -> None:
    key = redis_ai_processing_key(queue_key)
    client.lrem(key, 1, msg)


def redis_ai_push_delayed(
    client,
    queue_key: str,
    *,
    payload: Dict[str, Any],
    next_retry_at: int,
) -> None:
    key = redis_ai_delayed_key(queue_key)
    msg = json.dumps(payload, ensure_ascii=False)
    client.zadd(key, {msg: int(next_retry_at)})


def redis_ai_move_due_delayed_to_ready(
    client,
    queue_key: str,
    *,
    now_epoch: int,
    max_items: int,
    verbose: bool,
) -> int:
    if max_items <= 0:
        return 0
    delayed_key = redis_ai_delayed_key(queue_key)
    ready_key = redis_ai_ready_key(queue_key)
    msgs = client.zrangebyscore(
        delayed_key,
        min="-inf",
        max=int(now_epoch),
        start=0,
        num=max_items,
    )
    if not msgs:
        return 0
    pipe = client.pipeline()
    for msg in msgs:
        pipe.zrem(delayed_key, msg)
        pipe.lpush(ready_key, msg)
    pipe.ltrim(ready_key, 0, max(1, resolve_redis_ai_queue_maxlen()) - 1)
    pipe.execute()
    moved = len(msgs)
    if moved and verbose:
        print(f"[redis] ai_due_to_ready moved={moved}", flush=True)
    return moved


def redis_ai_due_count(client, queue_key: str, *, now_epoch: int) -> int:
    ready_count = int(client.llen(redis_ai_ready_key(queue_key)) or 0)
    delayed_due = int(
        client.zcount(redis_ai_delayed_key(queue_key), "-inf", int(now_epoch)) or 0
    )
    return int(ready_count + delayed_due)


def redis_ai_pending_count(client, queue_key: str) -> int:
    ready_count = int(client.llen(redis_ai_ready_key(queue_key)) or 0)
    processing_count = int(client.llen(redis_ai_processing_key(queue_key)) or 0)
    delayed_count = int(client.zcard(redis_ai_delayed_key(queue_key)) or 0)
    return int(ready_count + processing_count + delayed_count)


def redis_ai_clear_stale_dedup(
    client,
    queue_key: str,
    *,
    post_uid: str,
    verbose: bool,
) -> bool:
    resolved_queue_key = str(queue_key or "").strip()
    resolved_post_uid = str(post_uid or "").strip()
    if not client or not resolved_queue_key or not resolved_post_uid:
        return False
    dedup_key = _redis_dedup_key(resolved_queue_key, resolved_post_uid)
    try:
        if not client.exists(dedup_key):
            return False
        if client.exists(redis_ai_lease_key(resolved_queue_key, resolved_post_uid)):
            return False
        if _queue_contains_post_uid(
            client,
            redis_ai_ready_key(resolved_queue_key),
            post_uid=resolved_post_uid,
            sorted_set=False,
        ):
            return False
        if _queue_contains_post_uid(
            client,
            redis_ai_processing_key(resolved_queue_key),
            post_uid=resolved_post_uid,
            sorted_set=False,
        ):
            return False
        if _queue_contains_post_uid(
            client,
            redis_ai_delayed_key(resolved_queue_key),
            post_uid=resolved_post_uid,
            sorted_set=True,
        ):
            return False
        deleted = int(client.delete(dedup_key) or 0)
    except Exception as e:
        if verbose:
            print(
                f"[redis] ai_clear_stale_dedup_error post_uid={resolved_post_uid} "
                f"{type(e).__name__}: {e}",
                flush=True,
            )
        return False
    if deleted and verbose:
        print(f"[redis] ai_clear_stale_dedup post_uid={resolved_post_uid}", flush=True)
    return deleted > 0


def redis_ai_ack_and_cleanup(
    client,
    queue_key: str,
    *,
    msg: str,
    post_uid: str,
    spool_dir: Path,
    lease_token: str = "",
    verbose: bool,
) -> bool:
    try:
        redis_ai_ack_processing(client, queue_key, msg)
    except Exception as e:
        if verbose:
            print(f"[redis] ai_ack_error {type(e).__name__}: {e}", flush=True)
        return False
    resolved_post_uid = str(post_uid or "").strip()
    if resolved_post_uid:
        try:
            spool_delete(spool_dir, resolved_post_uid)
        except Exception:
            pass
    del lease_token
    return True


__all__ = [
    "try_get_redis",
    "resolve_redis_dedup_ttl_seconds",
    "resolve_redis_ai_queue_maxlen",
    "redis_ai_ready_key",
    "redis_ai_processing_key",
    "redis_ai_delayed_key",
    "redis_ai_lease_key",
    "redis_try_push_dedup_status",
    "redis_try_push_dedup",
    "redis_try_push_ai_dedup_status",
    "redis_ai_try_claim_lease",
    "redis_ai_release_lease",
    "redis_ai_requeue_processing_without_lease",
    "redis_ai_pop_to_processing",
    "redis_ai_ack_processing",
    "redis_ai_push_delayed",
    "redis_ai_move_due_delayed_to_ready",
    "redis_ai_due_count",
    "redis_ai_pending_count",
    "redis_ai_clear_stale_dedup",
    "redis_ai_ack_and_cleanup",
]
