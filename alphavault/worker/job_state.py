from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    qualify_postgres_table,
    require_postgres_schema_name,
    postgres_connect_autocommit,
)
from alphavault.timeutil import now_cst_str

WORKER_STATE_TABLE = "worker_cursor"
WORKER_LOCKS_TABLE = "worker_locks"
WORKER_PROGRESS_STATE_PREFIX = "worker.progress"
WORKER_PROGRESS_STAGE_CYCLE = "cycle"
WORKER_PROGRESS_STAGE_AI = "ai"
WORKER_PROGRESS_STAGE_ALIAS = "alias"
WORKER_PROGRESS_STAGE_RELATION = "relation"
WORKER_PROGRESS_STAGE_STOCK_HOT = "stock_hot"
WORKER_PROGRESS_STAGES = (
    WORKER_PROGRESS_STAGE_CYCLE,
    WORKER_PROGRESS_STAGE_AI,
    WORKER_PROGRESS_STAGE_ALIAS,
    WORKER_PROGRESS_STAGE_RELATION,
    WORKER_PROGRESS_STAGE_STOCK_HOT,
)


def _worker_state_table(engine_or_conn: object) -> str:
    return qualify_postgres_table(
        require_postgres_schema_name(engine_or_conn),
        WORKER_STATE_TABLE,
    )


def _worker_locks_table(engine_or_conn: object) -> str:
    return qualify_postgres_table(
        require_postgres_schema_name(engine_or_conn),
        WORKER_LOCKS_TABLE,
    )


def _sql_upsert_worker_state(table_name: str) -> str:
    return f"""
INSERT INTO {table_name}(state_key, cursor, updated_at)
VALUES (:state_key, :cursor, :now)
ON CONFLICT(state_key) DO UPDATE SET
    cursor = excluded.cursor,
    updated_at = excluded.updated_at
"""


def _sql_select_worker_cursor(table_name: str) -> str:
    return f"""
SELECT cursor
FROM {table_name}
WHERE state_key = :state_key
"""


def _sql_try_insert_lock(table_name: str) -> str:
    return f"""
INSERT INTO {table_name}(lock_key, locked_until, updated_at)
VALUES (:lock_key, :locked_until, :now)
ON CONFLICT(lock_key) DO NOTHING
"""


def _sql_try_refresh_lock_if_expired(table_name: str) -> str:
    return f"""
UPDATE {table_name}
SET locked_until = :locked_until, updated_at = :now
WHERE lock_key = :lock_key
  AND locked_until <= :now_epoch
"""


def _sql_release_lock(table_name: str) -> str:
    return f"""
UPDATE {table_name}
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
    engine_or_conn: PostgresEngine | PostgresConnection,
) -> Iterator[PostgresConnection]:
    if isinstance(engine_or_conn, PostgresConnection):
        yield engine_or_conn
        return
    with postgres_connect_autocommit(engine_or_conn) as conn:
        yield conn


def load_worker_job_cursor(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    state_key: str,
) -> str:
    key = str(state_key or "").strip()
    if not key:
        return ""
    with _use_conn(engine_or_conn) as conn:
        return str(
            conn.execute(
                _sql_select_worker_cursor(_worker_state_table(conn)),
                {"state_key": key},
            ).scalar()
            or ""
        ).strip()


def save_worker_job_cursor(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    state_key: str,
    cursor: str,
) -> None:
    key = str(state_key or "").strip()
    if not key:
        return
    with _use_conn(engine_or_conn) as conn:
        conn.execute(
            _sql_upsert_worker_state(_worker_state_table(conn)),
            {"state_key": key, "cursor": str(cursor or "").strip(), "now": _now_str()},
        )


def try_acquire_worker_job_lock(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    lock_key: str,
    now_epoch: int,
    lease_seconds: int,
) -> bool:
    key = str(lock_key or "").strip()
    if not key:
        return False
    locked_until = int(now_epoch) + max(1, int(lease_seconds))
    with _use_conn(engine_or_conn) as conn:
        inserted = conn.execute(
            _sql_try_insert_lock(_worker_locks_table(conn)),
            {"lock_key": key, "locked_until": locked_until, "now": _now_str()},
        )
        if int(inserted.rowcount or 0) > 0:
            return True
        updated = conn.execute(
            _sql_try_refresh_lock_if_expired(_worker_locks_table(conn)),
            {
                "lock_key": key,
                "locked_until": locked_until,
                "now_epoch": int(now_epoch),
                "now": _now_str(),
            },
        )
        return int(updated.rowcount or 0) > 0


def release_worker_job_lock(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    lock_key: str,
) -> None:
    key = str(lock_key or "").strip()
    if not key:
        return
    with _use_conn(engine_or_conn) as conn:
        conn.execute(
            _sql_release_lock(_worker_locks_table(conn)),
            {"lock_key": key, "now": _now_str()},
        )


__all__ = [
    "load_worker_job_cursor",
    "release_worker_job_lock",
    "save_worker_job_cursor",
    "try_acquire_worker_job_lock",
    "worker_progress_state_key",
    "WORKER_PROGRESS_STAGE_AI",
    "WORKER_PROGRESS_STAGE_ALIAS",
    "WORKER_PROGRESS_STAGE_CYCLE",
    "WORKER_PROGRESS_STAGE_RELATION",
    "WORKER_PROGRESS_STAGE_STOCK_HOT",
    "WORKER_PROGRESS_STAGES",
    "WORKER_LOCKS_TABLE",
    "WORKER_PROGRESS_STATE_PREFIX",
    "WORKER_STATE_TABLE",
]
