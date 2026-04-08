from __future__ import annotations

from alphavault.constants import SCHEMA_WEIBO
from alphavault.db.cloud_schema import apply_cloud_schema


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
