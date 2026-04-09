from __future__ import annotations

from alphavault.constants import SCHEMA_WEIBO, SCHEMA_XUEQIU
from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.postgres_db import PostgresConnection
from alphavault.worker.job_state import (
    load_worker_job_cursor,
    release_worker_job_lock,
    save_worker_job_cursor,
    try_acquire_worker_job_lock,
)


def test_apply_cloud_schema_uses_worker_job_state_target_tables(pg_conn) -> None:
    apply_cloud_schema(pg_conn, target="source", schema_name=SCHEMA_WEIBO)
    rows = pg_conn.execute(
        """
SELECT tablename
FROM pg_tables
WHERE schemaname = %(schema_name)s
""",
        {"schema_name": SCHEMA_WEIBO},
    ).fetchall()
    table_names = {str(row[0]) for row in rows}

    assert {"worker_cursor", "worker_locks"}.issubset(table_names)


def test_worker_job_state_reads_and_writes_xueqiu_schema(pg_conn) -> None:
    apply_cloud_schema(pg_conn, target="source", schema_name=SCHEMA_XUEQIU)
    conn = PostgresConnection(pg_conn, schema_name=SCHEMA_XUEQIU)

    save_worker_job_cursor(
        conn,
        state_key="worker.progress.xueqiu.ai",
        cursor="post:2",
    )
    assert (
        load_worker_job_cursor(
            conn,
            state_key="worker.progress.xueqiu.ai",
        )
        == "post:2"
    )
    assert (
        conn.execute(
            """
SELECT cursor
FROM xueqiu.worker_cursor
WHERE state_key = :state_key
""",
            {"state_key": "worker.progress.xueqiu.ai"},
        ).scalar()
        == "post:2"
    )

    assert (
        try_acquire_worker_job_lock(
            conn,
            lock_key="stock_hot_cache.lock",
            now_epoch=100,
            lease_seconds=60,
        )
        is True
    )
    release_worker_job_lock(conn, lock_key="stock_hot_cache.lock")
    assert (
        conn.execute(
            """
SELECT locked_until
FROM xueqiu.worker_locks
WHERE lock_key = :lock_key
""",
            {"lock_key": "stock_hot_cache.lock"},
        ).scalar()
        == 0
    )
