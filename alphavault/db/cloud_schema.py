from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterator

from alphavault.constants import SCHEMA_STANDARD, SCHEMA_WEIBO, SCHEMA_XUEQIU
from alphavault.db.postgres_db import (
    PostgresEngine,
    postgres_connect_autocommit,
)


_SQL_DIR = Path(__file__).resolve().parent / "sql"
_SCHEMA_TARGET_SOURCE = "source"
_SCHEMA_TARGET_STANDARD = "standard"
_SCHEMA_TARGET_ALL = "all"
_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SCHEMA_TEMPLATE_NAME = "{{schema_name}}"
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


def _normalize_schema_name(schema_name: str | None) -> str:
    resolved = str(schema_name or "").strip()
    if not resolved:
        raise ValueError("missing_cloud_schema_name")
    if _SCHEMA_NAME_RE.fullmatch(resolved) is None:
        raise ValueError(f"invalid_cloud_schema_name:{resolved}")
    return resolved


def render_cloud_schema_sql(sql_text: str, *, schema_name: str) -> str:
    return str(sql_text or "").replace(
        _SCHEMA_TEMPLATE_NAME,
        _normalize_schema_name(schema_name),
    )


def _resolve_schema_jobs(
    *, target: str, schema_name: str | None
) -> tuple[tuple[str, str], ...]:
    normalized = _normalize_schema_target(target)
    if normalized == _SCHEMA_TARGET_ALL:
        return (
            (_SCHEMA_TARGET_SOURCE, SCHEMA_WEIBO),
            (_SCHEMA_TARGET_SOURCE, SCHEMA_XUEQIU),
            (_SCHEMA_TARGET_STANDARD, SCHEMA_STANDARD),
        )
    return ((normalized, _normalize_schema_name(schema_name)),)


@contextmanager
def _use_conn(
    engine_or_conn: PostgresEngine | Any,
) -> Iterator[Any]:
    if isinstance(engine_or_conn, PostgresEngine):
        with postgres_connect_autocommit(engine_or_conn) as conn:
            yield conn
        return
    yield engine_or_conn


def apply_cloud_schema(
    engine_or_conn: PostgresEngine | Any,
    *,
    target: str = _SCHEMA_TARGET_ALL,
    schema_name: str | None = None,
) -> None:
    with _use_conn(engine_or_conn) as conn:
        for sql_target, resolved_schema_name in _resolve_schema_jobs(
            target=target,
            schema_name=schema_name,
        ):
            rendered_sql = render_cloud_schema_sql(
                load_cloud_schema_sql(target=sql_target),
                schema_name=resolved_schema_name,
            )
            for statement in iter_cloud_schema_statements(rendered_sql):
                conn.execute(statement)


__all__ = [
    "apply_cloud_schema",
    "iter_cloud_schema_statements",
    "load_cloud_schema_sql",
    "render_cloud_schema_sql",
]
