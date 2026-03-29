from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from alphavault.constants import (
    DEFAULT_REDIS_AI_QUEUE_MAXLEN,
    DEFAULT_REDIS_ASSERTION_QUEUE_MAXLEN,
    DEFAULT_REDIS_AUTHOR_CACHE_MAX_POSTS,
    DEFAULT_REDIS_AUTHOR_CACHE_TTL_SECONDS,
    DEFAULT_REDIS_DEDUP_TTL_SECONDS,
    ENV_REDIS_AI_QUEUE_MAXLEN,
    ENV_REDIS_ASSERTION_QUEUE_MAXLEN,
    ENV_REDIS_AUTHOR_CACHE_MAX_POSTS,
    ENV_REDIS_AUTHOR_CACHE_TTL_SECONDS,
    ENV_REDIS_DEDUP_TTL_SECONDS,
    ENV_REDIS_QUEUE_KEY,
    ENV_REDIS_URL,
)
from alphavault.worker.spool import sha1_short, spool_delete


DEFAULT_REDIS_QUEUE_KEY = "alphavault:rss_spool"
REDIS_PUSH_STATUS_PUSHED = "pushed"
REDIS_PUSH_STATUS_DUPLICATE = "duplicate"
REDIS_PUSH_STATUS_ERROR = "error"
REDIS_AUTHOR_RECENT_EMPTY_MARKER_POST_UID = "__alphavault_author_recent_empty__"

_REDIS_AUTHOR_RECENT_CACHE_STATE_KEY = "_cache_state"
_REDIS_AUTHOR_RECENT_CACHE_STATE_EMPTY = "empty"


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


def _author_hash(author: str) -> str:
    return sha1_short(str(author or "").strip().lower())


def redis_ai_ready_key(queue_key: str) -> str:
    return f"{queue_key}:ai:ready"


def redis_ai_processing_key(queue_key: str) -> str:
    return f"{queue_key}:ai:processing"


def redis_ai_delayed_key(queue_key: str) -> str:
    return f"{queue_key}:ai:delayed"


def redis_assertion_ready_key(queue_key: str) -> str:
    return f"{queue_key}:assertion_evt:ready"


def redis_assertion_cursor_key(queue_key: str) -> str:
    return f"{queue_key}:assertion_evt:cursor"


def redis_author_recent_key(queue_key: str, author: str) -> str:
    return f"{queue_key}:author:recent:{_author_hash(author)}"


def _redis_author_recent_empty_marker(author: str) -> dict[str, str]:
    return {
        "post_uid": REDIS_AUTHOR_RECENT_EMPTY_MARKER_POST_UID,
        "author": str(author or "").strip(),
        _REDIS_AUTHOR_RECENT_CACHE_STATE_KEY: _REDIS_AUTHOR_RECENT_CACHE_STATE_EMPTY,
    }


def _is_redis_author_recent_empty_marker(item: dict[str, Any]) -> bool:
    post_uid = str(item.get("post_uid") or "").strip()
    if post_uid != REDIS_AUTHOR_RECENT_EMPTY_MARKER_POST_UID:
        return False
    return (
        str(item.get(_REDIS_AUTHOR_RECENT_CACHE_STATE_KEY) or "").strip().lower()
        == _REDIS_AUTHOR_RECENT_CACHE_STATE_EMPTY
    )


def _env_int_or_default(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return max(minimum, int(default))
    try:
        value = int(raw)
    except Exception:
        return max(minimum, int(default))
    return max(minimum, int(value))


def resolve_redis_dedup_ttl_seconds() -> int:
    return _env_int_or_default(
        ENV_REDIS_DEDUP_TTL_SECONDS,
        int(DEFAULT_REDIS_DEDUP_TTL_SECONDS),
        minimum=1,
    )


def resolve_redis_ai_queue_maxlen() -> int:
    return _env_int_or_default(
        ENV_REDIS_AI_QUEUE_MAXLEN,
        int(DEFAULT_REDIS_AI_QUEUE_MAXLEN),
        minimum=100,
    )


def resolve_redis_assertion_queue_maxlen() -> int:
    return _env_int_or_default(
        ENV_REDIS_ASSERTION_QUEUE_MAXLEN,
        int(DEFAULT_REDIS_ASSERTION_QUEUE_MAXLEN),
        minimum=100,
    )


def resolve_redis_author_cache_ttl_seconds() -> int:
    return _env_int_or_default(
        ENV_REDIS_AUTHOR_CACHE_TTL_SECONDS,
        int(DEFAULT_REDIS_AUTHOR_CACHE_TTL_SECONDS),
        minimum=60,
    )


def resolve_redis_author_cache_max_posts() -> int:
    return _env_int_or_default(
        ENV_REDIS_AUTHOR_CACHE_MAX_POSTS,
        int(DEFAULT_REDIS_AUTHOR_CACHE_MAX_POSTS),
        minimum=20,
    )


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


def redis_author_recent_push(
    client,
    queue_key: str,
    *,
    payload: Dict[str, Any],
    ttl_seconds: int | None = None,
    max_items: int | None = None,
) -> bool:
    author = str(payload.get("author") or "").strip()
    if not client or not queue_key or not author:
        return False
    ttl = (
        int(ttl_seconds)
        if ttl_seconds is not None
        else int(resolve_redis_author_cache_ttl_seconds())
    )
    maxlen = (
        int(max_items)
        if max_items is not None
        else int(resolve_redis_author_cache_max_posts())
    )
    key = redis_author_recent_key(queue_key, author)
    try:
        _redis_push(client, key, payload, maxlen=maxlen)
        client.expire(key, max(60, int(ttl)))
        return True
    except Exception:
        return False


def redis_author_recent_push_many(
    client,
    queue_key: str,
    *,
    author: str,
    rows: list[dict[str, Any]],
    ttl_seconds: int | None = None,
    max_items: int | None = None,
) -> int:
    resolved_author = str(author or "").strip()
    if not client or not queue_key or not resolved_author:
        return 0
    if not rows:
        return 0
    ttl = (
        int(ttl_seconds)
        if ttl_seconds is not None
        else int(resolve_redis_author_cache_ttl_seconds())
    )
    maxlen = (
        int(max_items)
        if max_items is not None
        else int(resolve_redis_author_cache_max_posts())
    )
    key = redis_author_recent_key(queue_key, resolved_author)
    normalized = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append(json.dumps(row, ensure_ascii=False))
    if not normalized:
        return 0
    try:
        pipe = client.pipeline()
        for msg in reversed(normalized):
            pipe.lpush(key, msg)
        pipe.ltrim(key, 0, max(1, int(maxlen)) - 1)
        pipe.expire(key, max(60, int(ttl)))
        pipe.execute()
        return int(len(normalized))
    except Exception:
        return 0


def redis_author_recent_mark_empty(
    client,
    queue_key: str,
    *,
    author: str,
    ttl_seconds: int | None = None,
) -> bool:
    resolved_author = str(author or "").strip()
    if not client or not queue_key or not resolved_author:
        return False
    ttl = (
        int(ttl_seconds)
        if ttl_seconds is not None
        else int(resolve_redis_author_cache_ttl_seconds())
    )
    key = redis_author_recent_key(queue_key, resolved_author)
    try:
        pipe = client.pipeline()
        pipe.lpush(
            key,
            json.dumps(
                _redis_author_recent_empty_marker(resolved_author),
                ensure_ascii=False,
            ),
        )
        pipe.ltrim(key, 0, 0)
        pipe.expire(key, max(60, int(ttl)))
        pipe.execute()
        return True
    except Exception:
        return False


def redis_author_recent_load_state(
    client,
    queue_key: str,
    *,
    author: str,
    limit: int,
) -> tuple[list[dict[str, Any]], bool]:
    resolved_author = str(author or "").strip()
    if not client or not queue_key or not resolved_author:
        return [], False
    n = max(0, int(limit))
    if n <= 0:
        return [], False
    key = redis_author_recent_key(queue_key, resolved_author)
    try:
        raw = client.lrange(key, 0, n - 1)
    except Exception:
        return [], False
    out: list[dict[str, Any]] = []
    has_empty_marker = False
    for item in raw or []:
        try:
            parsed = json.loads(str(item))
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        if _is_redis_author_recent_empty_marker(parsed):
            has_empty_marker = True
            continue
        out.append(parsed)
    return out, (bool(has_empty_marker) and not bool(out))


def redis_assertion_push_event(
    client,
    queue_key: str,
    *,
    payload: Dict[str, Any],
) -> bool:
    if not client or not queue_key:
        return False
    try:
        _redis_push(
            client,
            redis_assertion_ready_key(queue_key),
            payload,
            maxlen=resolve_redis_assertion_queue_maxlen(),
        )
        return True
    except Exception:
        return False


def redis_assertion_event_count(client, queue_key: str) -> int:
    if not client or not queue_key:
        return 0
    try:
        return int(client.llen(redis_assertion_ready_key(queue_key)) or 0)
    except Exception:
        return 0


def redis_assertion_clear_events(client, queue_key: str) -> int:
    if not client or not queue_key:
        return 0
    key = redis_assertion_ready_key(queue_key)
    try:
        count = int(client.llen(key) or 0)
    except Exception:
        count = 0
    try:
        client.delete(key)
    except Exception:
        return 0
    return count


def redis_assertion_get_cursor(client, queue_key: str) -> int:
    if not client or not queue_key:
        return 0
    try:
        value = client.get(redis_assertion_cursor_key(queue_key))
    except Exception:
        return 0
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return max(0, int(text))
    except Exception:
        return 0


def redis_assertion_set_cursor(client, queue_key: str, *, cursor: int) -> None:
    if not client or not queue_key:
        return
    try:
        client.set(redis_assertion_cursor_key(queue_key), int(max(0, int(cursor))))
    except Exception:
        return


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
    try:
        spool_delete(spool_dir, post_uid)
    except Exception as e:
        if verbose:
            print(f"[redis] ai_spool_delete_error {type(e).__name__}: {e}", flush=True)
        return False
    return True
