from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import re
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
_DOLLAR_QUOTE_RE = re.compile(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$")
_VECTOR_EXTENSION_STATEMENT_RE = re.compile(
    r"^CREATE EXTENSION IF NOT EXISTS vector\s*$",
    re.IGNORECASE,
)
_VECTOR_DEPENDENT_STATEMENT_RE = re.compile(
    r"\bsemantic_docs\b|\bhalfvec\b|\bhnsw\b",
    re.IGNORECASE,
)
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


def _match_dollar_quote(sql_text: str, start: int) -> str:
    matched = _DOLLAR_QUOTE_RE.match(sql_text, start)
    if matched is None:
        return ""
    return matched.group(0)


def iter_cloud_schema_statements(sql_text: str) -> Iterator[str]:
    sql = str(sql_text or "")
    buffer: list[str] = []
    idx = 0
    size = len(sql)
    single_quote = False
    double_quote = False
    line_comment = False
    block_comment = False
    dollar_quote = ""

    while idx < size:
        if line_comment:
            char = sql[idx]
            buffer.append(char)
            idx += 1
            if char == "\n":
                line_comment = False
            continue

        if block_comment:
            if sql.startswith("*/", idx):
                buffer.append("*/")
                idx += 2
                block_comment = False
                continue
            buffer.append(sql[idx])
            idx += 1
            continue

        if dollar_quote:
            if sql.startswith(dollar_quote, idx):
                buffer.append(dollar_quote)
                idx += len(dollar_quote)
                dollar_quote = ""
                continue
            buffer.append(sql[idx])
            idx += 1
            continue

        if single_quote:
            if sql.startswith("''", idx):
                buffer.append("''")
                idx += 2
                continue
            char = sql[idx]
            buffer.append(char)
            idx += 1
            if char == "'":
                single_quote = False
            continue

        if double_quote:
            if sql.startswith('""', idx):
                buffer.append('""')
                idx += 2
                continue
            char = sql[idx]
            buffer.append(char)
            idx += 1
            if char == '"':
                double_quote = False
            continue

        if sql.startswith("--", idx):
            buffer.append("--")
            idx += 2
            line_comment = True
            continue

        if sql.startswith("/*", idx):
            buffer.append("/*")
            idx += 2
            block_comment = True
            continue

        dollar_quote = _match_dollar_quote(sql, idx)
        if dollar_quote:
            buffer.append(dollar_quote)
            idx += len(dollar_quote)
            continue

        char = sql[idx]
        if char == "'":
            single_quote = True
            buffer.append(char)
            idx += 1
            continue
        if char == '"':
            double_quote = True
            buffer.append(char)
            idx += 1
            continue
        if char == ";":
            statement = _normalize_statement("".join(buffer))
            buffer = []
            idx += 1
            if statement:
                yield statement
            continue

        buffer.append(char)
        idx += 1

    if single_quote or double_quote or line_comment or block_comment or dollar_quote:
        raise ValueError("incomplete_cloud_schema_sql")
    tail = _normalize_statement("".join(buffer))
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


def _should_skip_vector_extension_statement(
    *,
    statement: str,
    error: Exception,
) -> bool:
    if _VECTOR_EXTENSION_STATEMENT_RE.match(str(statement or "").strip()) is None:
        return False
    return 'extension "vector" is not available' in str(error or "").lower()


def _is_vector_dependent_statement(statement: str) -> bool:
    return _VECTOR_DEPENDENT_STATEMENT_RE.search(str(statement or "")) is not None


def apply_cloud_schema(
    engine_or_conn: PostgresEngine | Any,
    *,
    target: str = _SCHEMA_TARGET_ALL,
    schema_name: str | None = None,
) -> None:
    vector_extension_available = True
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
                if not vector_extension_available and _is_vector_dependent_statement(
                    statement
                ):
                    continue
                with conn.transaction():
                    try:
                        conn.execute(statement)
                    except Exception as exc:
                        if _should_skip_vector_extension_statement(
                            statement=statement,
                            error=exc,
                        ):
                            vector_extension_available = False
                            continue
                        raise


__all__ = [
    "apply_cloud_schema",
    "iter_cloud_schema_statements",
    "load_cloud_schema_sql",
    "render_cloud_schema_sql",
]
