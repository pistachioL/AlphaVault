from __future__ import annotations

from contextlib import contextmanager
import os
from threading import Lock
from typing import Iterator

from alphavault.timeutil import now_cst_str
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    turso_connect_autocommit,
)

# Track schema ensure status at process scope, so all worker job-state operations
# in the same process pay the DDL cost at most once.
_schema_ensure_lock = Lock()
_schema_ensured_process = False
_TRUTHY_VALUES = {"1", "true", "yes", "on"}
ENV_WORKER_JOB_STATE_ASSUME_SCHEMA_READY = "WORKER_JOB_STATE_ASSUME_SCHEMA_READY"


WORKER_STATE_TABLE = "worker_cursor"
WORKER_LOCKS_TABLE = "worker_locks"
WORKER_PROGRESS_STATE_PREFIX = "worker.progress"
WORKER_PROGRESS_STAGE_CYCLE = "cycle"
WORKER_PROGRESS_STAGE_AI = "ai"
WORKER_PROGRESS_STAGE_ALIAS = "alias"
WORKER_PROGRESS_STAGE_BACKFILL = "backfill"
WORKER_PROGRESS_STAGE_RELATION = "relation"
WORKER_PROGRESS_STAGE_STOCK_HOT = "stock_hot"
WORKER_PROGRESS_STAGES = (
    WORKER_PROGRESS_STAGE_CYCLE,
    WORKER_PROGRESS_STAGE_AI,
    WORKER_PROGRESS_STAGE_ALIAS,
    WORKER_PROGRESS_STAGE_BACKFILL,
    WORKER_PROGRESS_STAGE_RELATION,
    WORKER_PROGRESS_STAGE_STOCK_HOT,
)

SQL_CREATE_WORKER_STATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {WORKER_STATE_TABLE} (
    state_key TEXT PRIMARY KEY,
    cursor TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
)
"""

SQL_UPSERT_WORKER_STATE = f"""
INSERT INTO {WORKER_STATE_TABLE}(state_key, cursor, updated_at)
VALUES (:state_key, :cursor, :now)
ON CONFLICT(state_key) DO UPDATE SET
    cursor = excluded.cursor,
    updated_at = excluded.updated_at
"""

SQL_SELECT_WORKER_CURSOR = f"""
SELECT cursor
FROM {WORKER_STATE_TABLE}
WHERE state_key = :state_key
"""

SQL_CREATE_WORKER_LOCKS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {WORKER_LOCKS_TABLE} (
    lock_key TEXT PRIMARY KEY,
    locked_until INTEGER NOT NULL,
    updated_at TEXT NOT NULL
)
"""

SQL_TRY_INSERT_LOCK = f"""
INSERT INTO {WORKER_LOCKS_TABLE}(lock_key, locked_until, updated_at)
VALUES (:lock_key, :locked_until, :now)
ON CONFLICT(lock_key) DO NOTHING
"""

SQL_TRY_REFRESH_LOCK_IF_EXPIRED = f"""
UPDATE {WORKER_LOCKS_TABLE}
SET locked_until = :locked_until, updated_at = :now
WHERE lock_key = :lock_key
  AND locked_until <= :now_epoch
"""

SQL_RELEASE_LOCK = f"""
UPDATE {WORKER_LOCKS_TABLE}
SET locked_until = 0, updated_at = :now
WHERE lock_key = :lock_key
"""


def _now_str() -> str:
    return now_cst_str()


def worker_progress_state_key(*, source_name: str, stage: str) -> str:
    source = str(source_name or "").strip().lower()
    stage_value = str(stage or "").strip().lower()
    if not source or not stage_value:
        return ""
    return f"{WORKER_PROGRESS_STATE_PREFIX}.{source}.{stage_value}"


@contextmanager
def _use_conn(
    engine_or_conn: TursoEngine | TursoConnection,
) -> Iterator[TursoConnection]:
    if isinstance(engine_or_conn, TursoConnection):
        yield engine_or_conn
        return
    with turso_connect_autocommit(engine_or_conn) as conn:
        yield conn


def ensure_worker_job_state_schema(
    engine_or_conn: TursoEngine | TursoConnection,
) -> None:
    with _use_conn(engine_or_conn) as conn:
        conn.execute(SQL_CREATE_WORKER_STATE_TABLE)
        conn.execute(SQL_CREATE_WORKER_LOCKS_TABLE)


def _ensure_schema_once(engine_or_conn: TursoEngine | TursoConnection) -> None:
    """Run schema DDL only once per process, not on every call."""
    assume_ready = (
        str(os.getenv(ENV_WORKER_JOB_STATE_ASSUME_SCHEMA_READY, "") or "")
        .strip()
        .lower()
        in _TRUTHY_VALUES
    )
    if assume_ready:
        return

    global _schema_ensured_process
    if _schema_ensured_process:
        return
    with _schema_ensure_lock:
        if _schema_ensured_process:
            return
        ensure_worker_job_state_schema(engine_or_conn)
        _schema_ensured_process = True


def load_worker_job_cursor(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    state_key: str,
) -> str:
    key = str(state_key or "").strip()
    if not key:
        return ""
    _ensure_schema_once(engine_or_conn)
    with _use_conn(engine_or_conn) as conn:
        return str(
            conn.execute(SQL_SELECT_WORKER_CURSOR, {"state_key": key}).scalar() or ""
        ).strip()


def save_worker_job_cursor(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    state_key: str,
    cursor: str,
) -> None:
    key = str(state_key or "").strip()
    if not key:
        return
    _ensure_schema_once(engine_or_conn)
    with _use_conn(engine_or_conn) as conn:
        conn.execute(
            SQL_UPSERT_WORKER_STATE,
            {"state_key": key, "cursor": str(cursor or "").strip(), "now": _now_str()},
        )


def try_acquire_worker_job_lock(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    lock_key: str,
    now_epoch: int,
    lease_seconds: int,
) -> bool:
    key = str(lock_key or "").strip()
    if not key:
        return False
    _ensure_schema_once(engine_or_conn)
    locked_until = int(now_epoch) + max(1, int(lease_seconds))
    with _use_conn(engine_or_conn) as conn:
        inserted = conn.execute(
            SQL_TRY_INSERT_LOCK,
            {"lock_key": key, "locked_until": locked_until, "now": _now_str()},
        )
        if int(inserted.rowcount or 0) > 0:
            return True
        updated = conn.execute(
            SQL_TRY_REFRESH_LOCK_IF_EXPIRED,
            {
                "lock_key": key,
                "locked_until": locked_until,
                "now_epoch": int(now_epoch),
                "now": _now_str(),
            },
        )
        return int(updated.rowcount or 0) > 0


def release_worker_job_lock(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    lock_key: str,
) -> None:
    key = str(lock_key or "").strip()
    if not key:
        return
    _ensure_schema_once(engine_or_conn)
    with _use_conn(engine_or_conn) as conn:
        conn.execute(
            SQL_RELEASE_LOCK,
            {"lock_key": key, "now": _now_str()},
        )


__all__ = [
    "ensure_worker_job_state_schema",
    "load_worker_job_cursor",
    "release_worker_job_lock",
    "save_worker_job_cursor",
    "try_acquire_worker_job_lock",
    "worker_progress_state_key",
    "WORKER_PROGRESS_STAGE_AI",
    "WORKER_PROGRESS_STAGE_ALIAS",
    "WORKER_PROGRESS_STAGE_BACKFILL",
    "WORKER_PROGRESS_STAGE_CYCLE",
    "WORKER_PROGRESS_STAGE_RELATION",
    "WORKER_PROGRESS_STAGE_STOCK_HOT",
    "WORKER_PROGRESS_STAGES",
    "WORKER_LOCKS_TABLE",
    "WORKER_PROGRESS_STATE_PREFIX",
    "WORKER_STATE_TABLE",
]
