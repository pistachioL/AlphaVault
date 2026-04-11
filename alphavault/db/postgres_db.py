from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Callable, Mapping, Sequence, TypeVar

import psycopg
from psycopg_pool import ConnectionPool
import sqlparams

from alphavault.constants import ENV_POSTGRES_POOL_MAX_SIZE

_DEFAULT_POSTGRES_POOL_MAX_SIZE = 4
_NAMED_TO_PYFORMAT = sqlparams.SQLParams("named", "pyformat", escape_char=True)
_QMARK_TO_FORMAT = sqlparams.SQLParams("qmark", "format", escape_char=True)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
_T = TypeVar("_T")
_POSTGRES_ENGINE_CACHE: dict[tuple[str, str, int], "PostgresEngine"] = {}
_POSTGRES_ENGINE_CACHE_LOCK = Lock()


def is_fatal_base_exception(err: BaseException) -> bool:
    return isinstance(err, _FATAL_BASE_EXCEPTIONS)


def _to_sql_text(statement: Any) -> str:
    if isinstance(statement, str):
        return statement
    text_attr = getattr(statement, "text", None)
    if isinstance(text_attr, str):
        return text_attr
    return str(statement)


def _to_sequence(values: Sequence[Any]) -> tuple[Any, ...]:
    return tuple(values)


def _bind_single(
    query: str, params: Any
) -> tuple[str, Mapping[str, Any] | tuple[Any, ...] | None]:
    if params is None:
        return query, None
    if isinstance(params, Mapping):
        converted_query, converted_params = _NAMED_TO_PYFORMAT.format(query, params)
        return str(converted_query), converted_params
    if isinstance(params, (list, tuple)):
        if "?" in query:
            converted_query, converted_params = _QMARK_TO_FORMAT.format(query, params)
            return str(converted_query), _to_sequence(converted_params)
        return query, _to_sequence(params)
    raise TypeError(f"unsupported_sql_params_type: {type(params).__name__}")


def _bind_many(
    query: str, items: Sequence[Any]
) -> tuple[str, list[Mapping[str, Any] | tuple[Any, ...]]]:
    if not items:
        return query, []
    first = items[0]
    if isinstance(first, Mapping):
        converted_query, converted_many = _NAMED_TO_PYFORMAT.formatmany(query, items)
        return str(converted_query), list(converted_many)
    if isinstance(first, (list, tuple)):
        if "?" in query:
            converted_query, converted_many = _QMARK_TO_FORMAT.formatmany(query, items)
            return str(converted_query), [_to_sequence(item) for item in converted_many]
        return query, [_to_sequence(item) for item in items]
    raise TypeError(f"unsupported_sql_many_item_type: {type(first).__name__}")


def _normalize_batch_params(params: Any) -> list[Any]:
    if params is None:
        return []
    if isinstance(params, list):
        return params
    if isinstance(params, tuple):
        return list(params)
    raise TypeError(f"unsupported_sql_batch_params_type: {type(params).__name__}")


def _normalize_identifier(name: str, *, label: str) -> str:
    resolved = str(name or "").strip()
    if not resolved:
        raise RuntimeError(f"missing_postgres_{label}")
    if _IDENTIFIER_RE.fullmatch(resolved) is None:
        raise RuntimeError(f"invalid_postgres_{label}:{resolved}")
    return resolved


def require_postgres_schema_name(engine_or_conn: object) -> str:
    return _normalize_identifier(
        str(getattr(engine_or_conn, "schema_name", "") or ""),
        label="schema_name",
    )


def qualify_postgres_table(schema_name: str, table_name: str) -> str:
    return ".".join(
        (
            _normalize_identifier(schema_name, label="schema_name"),
            _normalize_identifier(table_name, label="table_name"),
        )
    )


class PostgresMappingsResult:
    def __init__(self, cursor: psycopg.Cursor[Any]):
        self._cursor = cursor
        description = getattr(cursor, "description", None) or ()
        self._keys = tuple(
            str(col.name) if getattr(col, "name", None) is not None else str(col[0])
            for col in description
            if col
        )

    def _to_mapping(self, row: Any) -> dict[str, Any] | None:
        if row is None:
            return None
        if not self._keys:
            return {}
        return {
            key: row[idx] if idx < len(row) else None
            for idx, key in enumerate(self._keys)
        }

    def fetchone(self) -> dict[str, Any] | None:
        return self._to_mapping(self._cursor.fetchone())

    def fetchall(self) -> list[dict[str, Any]]:
        rows = self._cursor.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            mapped = self._to_mapping(row)
            if mapped is not None:
                out.append(mapped)
        return out

    def all(self) -> list[dict[str, Any]]:
        return self.fetchall()

    def first(self) -> dict[str, Any] | None:
        return self.fetchone()


class PostgresCursorResult:
    def __init__(self, cursor: psycopg.Cursor[Any]):
        self._cursor = cursor

    @property
    def rowcount(self) -> int:
        raw = getattr(self._cursor, "rowcount", 0)
        try:
            return int(raw or 0)
        except Exception:
            return 0

    @property
    def description(self) -> Any:
        return getattr(self._cursor, "description", None)

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchall(self) -> list[Any]:
        return self._cursor.fetchall()

    def scalar(self) -> Any:
        row = self.fetchone()
        if not row:
            return None
        return row[0]

    def mappings(self) -> PostgresMappingsResult:
        return PostgresMappingsResult(self._cursor)


class PostgresConnection:
    def __init__(
        self,
        raw_conn: psycopg.Connection[Any],
        *,
        _pool: ConnectionPool[psycopg.Connection[Any]] | None = None,
        schema_name: str = "",
    ) -> None:
        self._raw = raw_conn
        self._pool = _pool
        self._closed = False
        self.schema_name = str(schema_name or "").strip()

    def __enter__(self) -> PostgresConnection:
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.close(broken=bool(getattr(self._raw, "broken", False)))

    def __getattr__(self, item: str) -> Any:
        return getattr(self._raw, item)

    def cursor(self) -> psycopg.Cursor[Any]:
        return self._raw.cursor()

    def close(self, *, broken: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        if self._pool is None:
            self._raw.close()
            return
        if broken:
            self._raw.close()
        self._pool.putconn(self._raw)

    def execute(self, statement: Any, params: Any = None) -> PostgresCursorResult:
        query = _to_sql_text(statement)
        is_batch = bool(
            isinstance(params, (list, tuple))
            and params
            and isinstance(params[0], (Mapping, list, tuple))
        )
        if is_batch:
            batch_params = _normalize_batch_params(params)
            prepared_query, prepared_many = _bind_many(query, batch_params)
            cursor = self._raw.cursor()
            cursor.executemany(prepared_query, prepared_many)
            return PostgresCursorResult(cursor)

        prepared_query, prepared_params = _bind_single(query, params)
        return PostgresCursorResult(self._raw.execute(prepared_query, prepared_params))

    def transaction(self):
        return self._raw.transaction()


@dataclass
class PostgresEngine:
    dsn: str
    max_connections: int = _DEFAULT_POSTGRES_POOL_MAX_SIZE
    schema_name: str = ""
    _pool: ConnectionPool[psycopg.Connection[Any]] = field(init=False, repr=False)
    _cache_key: tuple[str, str, int] | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _disposed: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        max_conns = int(self.max_connections or 0)
        if max_conns <= 0:
            max_conns = _DEFAULT_POSTGRES_POOL_MAX_SIZE
        self.max_connections = max(1, max_conns)
        self._pool = ConnectionPool(
            conninfo=self.dsn,
            kwargs={"autocommit": True},
            min_size=0,
            max_size=self.max_connections,
            max_idle=1.0,
            open=True,
        )

    def connect(self, *, autocommit: bool = True) -> PostgresConnection:
        if not autocommit:
            raise ValueError("postgres_connection_requires_autocommit")
        if self._disposed:
            raise RuntimeError("postgres_engine_disposed")
        return PostgresConnection(
            self._pool.getconn(),
            _pool=self._pool,
            schema_name=self.schema_name,
        )

    def dispose(self) -> None:
        if self._disposed:
            return
        self._disposed = True
        try:
            self._pool.close()
        finally:
            _drop_cached_postgres_engine(self)


def _drop_cached_postgres_engine(engine: PostgresEngine) -> None:
    cache_key = engine._cache_key
    if cache_key is None:
        return
    with _POSTGRES_ENGINE_CACHE_LOCK:
        cached = _POSTGRES_ENGINE_CACHE.get(cache_key)
        if cached is engine:
            _POSTGRES_ENGINE_CACHE.pop(cache_key, None)
    engine._cache_key = None


def postgres_connect_autocommit(engine: PostgresEngine) -> PostgresConnection:
    return engine.connect(autocommit=True)


def run_postgres_transaction(
    engine_or_conn: PostgresEngine | PostgresConnection,
    fn: Callable[[PostgresConnection], _T],
) -> _T:
    if isinstance(engine_or_conn, PostgresConnection):
        with engine_or_conn.transaction():
            return fn(engine_or_conn)

    with postgres_connect_autocommit(engine_or_conn) as conn:
        with conn.transaction():
            return fn(conn)


def _max_postgres_pool_size_from_env() -> int:
    raw = os.getenv(ENV_POSTGRES_POOL_MAX_SIZE, "").strip()
    if not raw:
        return _DEFAULT_POSTGRES_POOL_MAX_SIZE
    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_POSTGRES_POOL_MAX_SIZE
    return max(1, int(value))


def ensure_postgres_engine(dsn: str, *, schema_name: str = "") -> PostgresEngine:
    resolved_dsn = str(dsn or "").strip()
    if not resolved_dsn:
        raise RuntimeError("Missing Postgres dsn")
    resolved_schema_name = str(schema_name or "").strip()
    max_connections = _max_postgres_pool_size_from_env()
    cache_key = (resolved_dsn, resolved_schema_name, max_connections)
    with _POSTGRES_ENGINE_CACHE_LOCK:
        cached = _POSTGRES_ENGINE_CACHE.get(cache_key)
        if cached is not None and not cached._disposed:
            return cached
        if cached is not None:
            _POSTGRES_ENGINE_CACHE.pop(cache_key, None)
        engine = PostgresEngine(
            dsn=resolved_dsn,
            max_connections=max_connections,
            schema_name=resolved_schema_name,
        )
        engine._cache_key = cache_key
        _POSTGRES_ENGINE_CACHE[cache_key] = engine
        return engine


__all__ = [
    "is_fatal_base_exception",
    "PostgresConnection",
    "PostgresCursorResult",
    "PostgresEngine",
    "PostgresMappingsResult",
    "ensure_postgres_engine",
    "qualify_postgres_table",
    "postgres_connect_autocommit",
    "require_postgres_schema_name",
    "run_postgres_transaction",
]
