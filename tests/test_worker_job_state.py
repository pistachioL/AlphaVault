from __future__ import annotations

import libsql

from alphavault.db.turso_db import TursoConnection
from alphavault.db.turso_db import TursoEngine
from alphavault.worker import job_state


def test_ensure_worker_job_state_schema_uses_target_tables() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        job_state.ensure_worker_job_state_schema(conn)
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

    assert table_names == {"worker_cursor", "worker_locks"}


def test_ensure_schema_once_runs_once_per_process(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(job_state, "_schema_ensured_process", False)
    monkeypatch.setattr(
        job_state,
        "ensure_worker_job_state_schema",
        lambda _engine_or_conn: calls.append("ddl"),
    )
    monkeypatch.delenv(
        job_state.ENV_WORKER_JOB_STATE_ASSUME_SCHEMA_READY, raising=False
    )

    engine_a = TursoEngine(remote_url="libsql://unit-a.test", auth_token="token")
    engine_b = TursoEngine(remote_url="libsql://unit-b.test", auth_token="token")

    job_state._ensure_schema_once(engine_a)
    job_state._ensure_schema_once(engine_b)

    assert calls == ["ddl"]


def test_ensure_schema_once_skips_when_assume_ready_enabled(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(job_state, "_schema_ensured_process", False)
    monkeypatch.setattr(
        job_state,
        "ensure_worker_job_state_schema",
        lambda _engine_or_conn: calls.append("ddl"),
    )
    monkeypatch.setenv(job_state.ENV_WORKER_JOB_STATE_ASSUME_SCHEMA_READY, "true")

    engine = TursoEngine(remote_url="libsql://unit.test", auth_token="token")
    job_state._ensure_schema_once(engine)

    assert calls == []
