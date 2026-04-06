from __future__ import annotations

import libsql

from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.turso_db import TursoConnection


def test_apply_cloud_schema_uses_worker_job_state_target_tables() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        apply_cloud_schema(conn)
        table_names = {
            str(row["name"])
            for row in conn.execute(
                """
SELECT name
FROM sqlite_schema
WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
"""
            )
            .mappings()
            .all()
        }
    finally:
        conn.close()

    assert {"worker_cursor", "worker_locks"}.issubset(table_names)
