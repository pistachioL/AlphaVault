from __future__ import annotations

from uuid import uuid4

import pytest

from alphavault.db.postgres_db import (
    ensure_postgres_engine,
    postgres_connect_autocommit,
    run_postgres_transaction,
)


def test_postgres_fixture_can_connect(pg_conn) -> None:
    assert pg_conn.execute("SELECT 1").fetchone()[0] == 1


def test_postgres_execute_supports_named_params_and_mappings(
    postgres_dsn: str,
) -> None:
    engine = ensure_postgres_engine(postgres_dsn)
    try:
        with postgres_connect_autocommit(engine) as conn:
            conn.execute(
                "CREATE TEMP TABLE posts(post_uid text primary key, author text not null, score integer not null)"
            )
            conn.execute(
                "INSERT INTO posts(post_uid, author, score) VALUES (:post_uid, :author, :score)",
                {"post_uid": "p1", "author": "alice", "score": 7},
            )
            row = (
                conn.execute(
                    "SELECT post_uid, author, score FROM posts WHERE post_uid = :post_uid",
                    {"post_uid": "p1"},
                )
                .mappings()
                .fetchone()
            )

        assert row == {"post_uid": "p1", "author": "alice", "score": 7}
    finally:
        engine.dispose()


def test_run_postgres_transaction_rolls_back_on_error(postgres_dsn: str) -> None:
    engine = ensure_postgres_engine(postgres_dsn)
    table_name = f"test_tx_{uuid4().hex}"
    try:
        with postgres_connect_autocommit(engine) as conn:
            conn.execute(
                f"CREATE TABLE {table_name}(id integer primary key, v text not null)"
            )

        def _boom(tx_conn) -> None:
            tx_conn.execute(
                f"INSERT INTO {table_name}(id, v) VALUES (:id, :v)",
                {"id": 1, "v": "a"},
            )
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            run_postgres_transaction(engine, _boom)

        with postgres_connect_autocommit(engine) as conn:
            assert conn.execute(f"SELECT COUNT(*) FROM {table_name}").scalar() == 0
    finally:
        with postgres_connect_autocommit(engine) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        engine.dispose()
