from __future__ import annotations

import json
import time
from typing import Any

from alphavault.db.postgres_db import PostgresConnection, PostgresEngine
from alphavault.rss.utils import now_str
from alphavault.worker.job_state import (
    save_worker_job_cursor,
    worker_progress_state_key,
)
from alphavault.worker.redis_queue import (
    redis_ai_due_count,
)
from alphavault.worker.turso_runtime import (
    maybe_dispose_turso_engine_on_transient_error,
)


_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def save_worker_progress_state(
    *,
    source: Any,
    stage: str,
    payload: dict[str, object],
    verbose: bool,
) -> None:
    cfg = getattr(source, "config", None)
    source_name = str(getattr(cfg, "name", "") or "").strip()
    state_key = worker_progress_state_key(source_name=source_name, stage=stage)
    if not state_key:
        return
    data = {str(key): value for key, value in payload.items() if str(key or "").strip()}
    data["source"] = source_name
    data["stage"] = str(stage or "").strip()
    data["updated_at"] = now_str()

    cache_key = str(stage or "").strip()
    cached = getattr(source, "progress_state_cache", {}).get(cache_key)
    comparable = {k: v for k, v in data.items() if k != "updated_at"}
    if cached is not None and cached == comparable:
        return
    getattr(source, "progress_state_cache", {})[cache_key] = comparable

    engine_or_conn = getattr(source, "engine", None)
    if not isinstance(engine_or_conn, (PostgresEngine, PostgresConnection)):
        return
    try:
        save_worker_job_cursor(
            engine_or_conn,
            state_key=state_key,
            cursor=json.dumps(data, ensure_ascii=False),
        )
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        engine = engine_or_conn
        if isinstance(engine, PostgresEngine):
            maybe_dispose_turso_engine_on_transient_error(
                engine=engine, err=err, verbose=bool(verbose)
            )
        if verbose:
            print(
                f"[progress:{source_name}] write_error {type(err).__name__}: {err}",
                flush=True,
            )


def has_due_ai_posts(
    *,
    engine: PostgresEngine | None,
    platform: str,
    verbose: bool,
    redis_client=None,
    redis_queue_key: str = "",
) -> bool:
    del engine, platform
    if not redis_client or not str(redis_queue_key or "").strip():
        return False
    try:
        return bool(
            redis_ai_due_count(
                redis_client,
                str(redis_queue_key),
                now_epoch=int(time.time()),
            )
        )
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        if verbose:
            print(f"[ai] redis_due_check_error {type(err).__name__}: {err}", flush=True)
        return False


__all__ = [
    "has_due_ai_posts",
    "save_worker_progress_state",
]
