from __future__ import annotations

from typing import Any

import pandas as pd


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


def turso_read_sql_df(conn: Any, sql: str, params: Any = None) -> pd.DataFrame:
    """
    Read a SELECT query from Turso (libsql) and return a pandas DataFrame.

    We intentionally avoid pandas SQL helpers here because our Turso connection
    is not a SQLAlchemy connectable and pandas will emit a UserWarning.
    """
    res = conn.execute(sql, params) if params is not None else conn.execute(sql)
    cols = _columns_from_description(getattr(res, "description", None))
    rows = res.fetchall()
    if cols:
        return pd.DataFrame(rows, columns=cols)
    return pd.DataFrame(rows)
