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


_SQL_DIR = Path(__file__).resolve().parent / "sql"
_SCHEMA_TARGET_SOURCE = "source"
_SCHEMA_TARGET_STANDARD = "standard"
_SCHEMA_TARGET_ALL = "all"
_SCHEMA_PATHS = {
    _SCHEMA_TARGET_SOURCE: _SQL_DIR / "source_schema.sql",
    _SCHEMA_TARGET_STANDARD: _SQL_DIR / "standard_schema.sql",
}


def _normalize_schema_target(target: str) -> str:
    normalized = str(target or "").strip().lower() or _SCHEMA_TARGET_ALL
    if normalized == _SCHEMA_TARGET_ALL:
        return normalized
    if normalized in _SCHEMA_PATHS:
        return normalized
    raise ValueError(f"unknown_cloud_schema_target:{normalized}")


def load_cloud_schema_sql(*, target: str = _SCHEMA_TARGET_ALL) -> str:
    normalized = _normalize_schema_target(target)
    if normalized == _SCHEMA_TARGET_ALL:
        parts = [
            load_cloud_schema_sql(target=_SCHEMA_TARGET_SOURCE).strip(),
            load_cloud_schema_sql(target=_SCHEMA_TARGET_STANDARD).strip(),
        ]
        return "\n\n".join(part for part in parts if part).strip() + "\n"
    return _SCHEMA_PATHS[normalized].read_text(encoding="utf-8")


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


def apply_cloud_schema(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    target: str = _SCHEMA_TARGET_ALL,
) -> None:
    with _use_conn(engine_or_conn) as conn:
        for statement in iter_cloud_schema_statements(
            load_cloud_schema_sql(target=target)
        ):
            conn.execute(statement)


__all__ = [
    "apply_cloud_schema",
    "iter_cloud_schema_statements",
    "load_cloud_schema_sql",
]
