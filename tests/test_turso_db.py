from __future__ import annotations

import libsql
import pytest

from alphavault.db.sql.turso_queue import RECOVER_STUCK_AI_TASKS
from alphavault.db.turso_db import (
    TursoConnection,
    is_turso_stream_not_found_error,
    turso_savepoint,
)


def test_is_turso_stream_not_found_error_true() -> None:
    err = ValueError(
        'Hrana: `api error: `status=404 Not Found, body={"error":"stream not found: abc"} ``'
    )
    assert is_turso_stream_not_found_error(err)


def test_is_turso_stream_not_found_error_true_wrapped() -> None:
    inner = ValueError("stream not found: xyz")
    try:
        raise RuntimeError("outer") from inner
    except RuntimeError as outer:
        assert is_turso_stream_not_found_error(outer)


def test_is_turso_stream_not_found_error_false() -> None:
    err = RuntimeError("connection reset by peer")
    assert not is_turso_stream_not_found_error(err)


def test_turso_connection_named_params_and_mappings() -> None:
    conn = TursoConnection(libsql.connect(":memory:", autocommit=True))
    try:
        conn.execute(
            "CREATE TABLE posts(post_uid TEXT PRIMARY KEY, author TEXT NOT NULL, score INTEGER NOT NULL)"
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
        conn.close()


def test_turso_connection_executemany_with_mapping_list() -> None:
    conn = TursoConnection(libsql.connect(":memory:", autocommit=True))
    try:
        conn.execute(
            "CREATE TABLE assertions(post_uid TEXT NOT NULL, idx INTEGER NOT NULL)"
        )
        conn.execute(
            "INSERT INTO assertions(post_uid, idx) VALUES (:post_uid, :idx)",
            [
                {"post_uid": "p1", "idx": 1},
                {"post_uid": "p1", "idx": 2},
                {"post_uid": "p2", "idx": 1},
            ],
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM assertions WHERE post_uid = :post_uid",
                {"post_uid": "p1"},
            ).scalar()
            == 2
        )
    finally:
        conn.close()


def test_turso_savepoint_commit_and_rollback() -> None:
    conn = TursoConnection(libsql.connect(":memory:", autocommit=True))
    try:
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")

        with turso_savepoint(conn):
            conn.execute(
                "INSERT INTO t(id, v) VALUES (:id, :v)",
                {"id": 1, "v": "a"},
            )

        assert conn.execute("SELECT COUNT(*) FROM t").scalar() == 1

        with pytest.raises(RuntimeError):
            with turso_savepoint(conn):
                conn.execute(
                    "INSERT INTO t(id, v) VALUES (:id, :v)",
                    {"id": 2, "v": "b"},
                )
                raise RuntimeError("boom")

        assert conn.execute("SELECT COUNT(*) FROM t").scalar() == 1
    finally:
        conn.close()


def test_named_params_support_escaped_colon_literal() -> None:
    conn = TursoConnection(libsql.connect(":memory:", autocommit=True))
    try:
        conn.execute(
            "CREATE TABLE posts(post_uid TEXT PRIMARY KEY, ai_status TEXT, ai_running_at INTEGER, ai_last_error TEXT, ai_next_retry_at INTEGER)"
        )
        conn.execute(
            "INSERT INTO posts(post_uid, ai_status, ai_running_at) VALUES (:post_uid, :ai_status, :ai_running_at)",
            {"post_uid": "p1", "ai_status": "running", "ai_running_at": 10},
        )

        res = conn.execute(
            RECOVER_STUCK_AI_TASKS,
            {"threshold": 10, "next_retry_at": 999},
        )
        assert int(res.rowcount or 0) == 1

        row = (
            conn.execute(
                "SELECT ai_status, ai_last_error, ai_next_retry_at FROM posts WHERE post_uid = :post_uid",
                {"post_uid": "p1"},
            )
            .mappings()
            .fetchone()
        )
        assert row == {
            "ai_status": "error",
            "ai_last_error": "ai:recovered_after_restart",
            "ai_next_retry_at": 999,
        }
    finally:
        conn.close()
