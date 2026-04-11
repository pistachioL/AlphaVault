from __future__ import annotations

import sqlite3

from alphavault.db.sql_df import read_sql_df


def test_read_sql_df_returns_columns_and_rows() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        conn.execute(
            "INSERT INTO t(id, v) VALUES (:id, :v)",
            {"id": 1, "v": "a"},
        )
        df = read_sql_df(conn, "SELECT id, v FROM t ORDER BY id")
        assert df.to_dict(orient="records") == [{"id": 1, "v": "a"}]
    finally:
        conn.close()


def test_read_sql_df_empty_keeps_columns() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        df = read_sql_df(conn, "SELECT id, v FROM t WHERE id = :id", {"id": 999})
        assert list(df.columns) == ["id", "v"]
        assert df.empty
    finally:
        conn.close()
