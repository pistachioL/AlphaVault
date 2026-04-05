from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sqlite3
from typing import Iterator

from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    turso_connect_autocommit,
)


_CLOUD_SCHEMA_PATH = Path(__file__).resolve().parent / "sql" / "cloud_schema.sql"


def load_cloud_schema_sql() -> str:
    return _CLOUD_SCHEMA_PATH.read_text(encoding="utf-8")


def _normalize_statement(statement: str) -> str:
    lines = [
        line
        for line in str(statement or "").splitlines()
        if not line.lstrip().startswith("--")
    ]
    return "\n".join(lines).strip()


def iter_cloud_schema_statements(sql_text: str) -> Iterator[str]:
    buffer = ""
    for line in str(sql_text or "").splitlines():
        buffer = f"{buffer}\n{line}" if buffer else line
        if not sqlite3.complete_statement(buffer):
            continue
        statement = _normalize_statement(buffer)
        buffer = ""
        if statement:
            yield statement
    tail = _normalize_statement(buffer)
    if tail:
        raise ValueError("incomplete_cloud_schema_sql")


@contextmanager
def _use_conn(
    engine_or_conn: TursoEngine | TursoConnection,
) -> Iterator[TursoConnection]:
    if isinstance(engine_or_conn, TursoConnection):
        yield engine_or_conn
        return
    with turso_connect_autocommit(engine_or_conn) as conn:
        yield conn


def apply_cloud_schema(engine_or_conn: TursoEngine | TursoConnection) -> None:
    with _use_conn(engine_or_conn) as conn:
        for statement in iter_cloud_schema_statements(load_cloud_schema_sql()):
            conn.execute(statement)


__all__ = [
    "apply_cloud_schema",
    "iter_cloud_schema_statements",
    "load_cloud_schema_sql",
]
