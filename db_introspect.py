from __future__ import annotations

from typing import Any

from sqlalchemy import text


def table_columns(conn_or_engine: Any, table: str) -> set[str]:
    """
    Return a set of column names for a table.

    Accepts either:
    - SQLAlchemy Engine (has .connect())
    - SQLAlchemy Connection (has .execute())
    """
    if hasattr(conn_or_engine, "connect"):
        with conn_or_engine.connect() as conn:
            return _table_columns_from_conn(conn, table)
    return _table_columns_from_conn(conn_or_engine, table)


def _table_columns_from_conn(conn: Any, table: str) -> set[str]:
    rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    out: set[str] = set()
    for row in rows:
        # row is a tuple: (cid, name, type, notnull, dflt_value, pk)
        if row and len(row) >= 2:
            out.add(str(row[1]))
    return out

