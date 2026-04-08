from __future__ import annotations


def test_postgres_fixture_can_connect(pg_conn) -> None:
    assert pg_conn.execute("SELECT 1").fetchone()[0] == 1
