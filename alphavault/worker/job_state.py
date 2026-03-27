from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Iterator

from alphavault.constants import DATETIME_FMT
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    turso_connect_autocommit,
)


WORKER_STATE_TABLE = "research_worker_state"
WORKER_LOCKS_TABLE = "research_worker_locks"

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
    return datetime.now().strftime(DATETIME_FMT)


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


def load_worker_job_cursor(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    state_key: str,
) -> str:
    key = str(state_key or "").strip()
    if not key:
        return ""
    ensure_worker_job_state_schema(engine_or_conn)
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
    ensure_worker_job_state_schema(engine_or_conn)
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
    ensure_worker_job_state_schema(engine_or_conn)
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
    ensure_worker_job_state_schema(engine_or_conn)
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
    "WORKER_LOCKS_TABLE",
    "WORKER_STATE_TABLE",
]
