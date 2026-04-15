from __future__ import annotations

from typing import Any


def _columns_from_description(description: Any) -> list[str]:
    # DB-API cursor.description: sequence of sequences, first item is column name.
    if not description:
        return []
    cols: list[str] = []
    for col in description:
        if not col:
            continue
        name = getattr(col, "name", None)
        if name is None:
            try:
                name = col[0] if len(col) > 0 else None
            except Exception:
                name = None
        if name is None:
            continue
        cols.append(str(name))
    return cols


def read_sql_rows(conn: Any, sql: str, params: Any = None) -> list[dict[str, Any]]:
    res = conn.execute(sql, params) if params is not None else conn.execute(sql)
    cols = _columns_from_description(getattr(res, "description", None))
    rows = res.fetchall()
    if not cols:
        return []
    return [dict(zip(cols, row, strict=False)) for row in rows]


__all__ = ["read_sql_rows"]
