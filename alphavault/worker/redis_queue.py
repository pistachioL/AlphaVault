from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from alphavault.constants import (
    DEFAULT_REDIS_AI_QUEUE_MAXLEN,
    DEFAULT_REDIS_DEDUP_TTL_SECONDS,
    ENV_REDIS_AI_QUEUE_MAXLEN,
    ENV_REDIS_DEDUP_TTL_SECONDS,
    ENV_REDIS_QUEUE_KEY,
    ENV_REDIS_URL,
)
from alphavault.worker.spool import sha1_short


DEFAULT_REDIS_QUEUE_KEY = "alphavault:rss_spool"
REDIS_PUSH_STATUS_PUSHED = "pushed"
REDIS_PUSH_STATUS_DUPLICATE = "duplicate"
REDIS_PUSH_STATUS_ERROR = "error"


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


def redis_ai_requeue_processing(
    client, queue_key: str, *, max_items: int, verbose: bool
) -> int:
    if max_items <= 0:
        return 0
    src = redis_ai_processing_key(queue_key)
    dst = redis_ai_ready_key(queue_key)
    moved = 0
    for _ in range(max_items):
        msg = client.rpoplpush(src, dst)
        if not msg:
            break
        moved += 1
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


def redis_ai_ack_and_cleanup(
    client,
    queue_key: str,
    *,
    msg: str,
    post_uid: str,
    spool_dir: Path,
    verbose: bool,
) -> bool:
    try:
        redis_ai_ack_processing(client, queue_key, msg)
    except Exception as e:
        if verbose:
            print(f"[redis] ai_ack_error {type(e).__name__}: {e}", flush=True)
        return False
    del post_uid, spool_dir
    return True
