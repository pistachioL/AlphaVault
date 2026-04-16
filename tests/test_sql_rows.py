from __future__ import annotations

import sqlite3

from alphavault.db.sql_rows import read_sql_rows


def test_read_sql_rows_returns_columns_and_rows() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        conn.execute(
            "INSERT INTO t(id, v) VALUES (:id, :v)",
            {"id": 1, "v": "a"},
        )
        rows = read_sql_rows(conn, "SELECT id, v FROM t ORDER BY id")
        assert rows == [{"id": 1, "v": "a"}]
    finally:
        conn.close()


def test_read_sql_rows_empty_keeps_columns_shape_as_empty_rows() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        rows = read_sql_rows(conn, "SELECT id, v FROM t WHERE id = :id", {"id": 999})
        assert rows == []
    finally:
        conn.close()
