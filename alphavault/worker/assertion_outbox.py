from __future__ import annotations

import json

from alphavault.db.turso_queue import load_assertion_outbox_events
from alphavault.db.turso_db import TursoEngine
from alphavault.worker.redis_queue import (
    redis_assertion_event_count,
    redis_assertion_get_cursor,
    redis_assertion_push_event,
    redis_assertion_set_cursor,
    resolve_redis_assertion_queue_maxlen,
)
from alphavault.worker.turso_runtime import (
    maybe_dispose_turso_engine_on_transient_error,
)


ASSERTION_OUTBOX_PUMP_BATCH_SIZE = 200
ASSERTION_OUTBOX_PUMP_SKIP_FILL_RATIO = 0.8

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def pump_assertion_outbox_to_redis(
    *,
    engine: TursoEngine,
    redis_client,
    redis_queue_key: str,
    verbose: bool,
) -> tuple[int, bool]:
    if not redis_client or not str(redis_queue_key or "").strip():
        return 0, False
    cursor = 0
    try:
        cursor = redis_assertion_get_cursor(redis_client, redis_queue_key)
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        if verbose:
            print(
                f"[outbox] redis_cursor_get_error {type(err).__name__}: {err}",
                flush=True,
            )
        cursor = 0
    try:
        queue_maxlen = max(1, int(resolve_redis_assertion_queue_maxlen()))
        queue_skip_threshold = max(
            1, int(float(queue_maxlen) * float(ASSERTION_OUTBOX_PUMP_SKIP_FILL_RATIO))
        )
        current_queue_len = max(
            0, int(redis_assertion_event_count(redis_client, redis_queue_key))
        )
        if current_queue_len >= queue_skip_threshold:
            if verbose:
                print(
                    f"[outbox] skip_turso_read queue_len={current_queue_len} "
                    f"threshold={queue_skip_threshold}",
                    flush=True,
                )
            return 0, False
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        if verbose:
            print(
                f"[outbox] redis_queue_count_error {type(err).__name__}: {err}",
                flush=True,
            )
    try:
        events = load_assertion_outbox_events(
            engine,
            after_id=max(0, int(cursor)),
            limit=int(ASSERTION_OUTBOX_PUMP_BATCH_SIZE),
        )
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        maybe_dispose_turso_engine_on_transient_error(
            engine=engine, err=err, verbose=bool(verbose)
        )
        if verbose:
            print(
                f"[outbox] turso_load_error {type(err).__name__}: {err}",
                flush=True,
            )
        return 0, True
    if not events:
        return 0, False
    pushed = 0
    last_id = max(0, int(cursor))
    for event in events:
        payload: dict[str, object] = {}
        text = str(event.event_json or "").strip()
        if text:
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                payload = {
                    str(key): value
                    for key, value in parsed.items()
                    if str(key or "").strip()
                }
        if not payload:
            payload = {
                "event_type": "ai_done",
                "post_uid": str(event.post_uid or "").strip(),
                "author": str(event.author or "").strip(),
            }
        ok = redis_assertion_push_event(
            redis_client,
            redis_queue_key,
            payload=payload,
        )
        if not ok:
            if verbose:
                print(
                    f"[outbox] redis_push_error post_uid={event.post_uid}",
                    flush=True,
                )
            return pushed, True
        pushed += 1
        last_id = max(last_id, int(event.id))
    redis_assertion_set_cursor(redis_client, redis_queue_key, cursor=int(last_id))
    if verbose and pushed > 0:
        print(
            f"[outbox] pumped events={pushed} cursor={last_id}",
            flush=True,
        )
    return pushed, False


__all__ = [
    "ASSERTION_OUTBOX_PUMP_BATCH_SIZE",
    "ASSERTION_OUTBOX_PUMP_SKIP_FILL_RATIO",
    "pump_assertion_outbox_to_redis",
]
