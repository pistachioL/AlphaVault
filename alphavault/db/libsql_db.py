from __future__ import annotations

import queue
import re
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping, Sequence

import libsql
import sqlparams

# NOTE: This module keeps Turso engine creation + connection helpers.

SQL_BEGIN = "BEGIN"
SQL_COMMIT = "COMMIT"
SQL_ROLLBACK = "ROLLBACK"
TURSO_SAVEPOINT_NAME = "alphavault_sp"
_SAVEPOINT_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
_DEFAULT_TURSO_MAX_CONNECTIONS = 4
_POOL_WAIT_TIMEOUT_SECONDS = 0.5
_NAMED_TO_QMARK = sqlparams.SQLParams("named", "qmark", escape_char=True)
_DEFAULT_EXECUTE_RETRIES = 2
_DEFAULT_TRANSACTION_RETRIES = 1
_RETRY_BASE_SLEEP_SECONDS = 0.2


def is_fatal_base_exception(err: BaseException) -> bool:
    return isinstance(err, _FATAL_BASE_EXCEPTIONS)


def is_turso_stream_not_found_error(err: BaseException) -> bool:
    """
    Detect the transient Hrana error: "stream not found" (HTTP 404).
    """
    needle = "stream not found"
    current: BaseException | None = err
    for _ in range(8):
        if current is None:
            break
        try:
            msg = str(current)
        except Exception:
            msg = ""
        if needle in msg.lower():
            return True
        orig = getattr(current, "orig", None)
        if isinstance(orig, BaseException) and orig is not current:
            current = orig
            continue
        current = (
            current.__cause__
            if isinstance(current.__cause__, BaseException)
            else current.__context__
        )
        if not isinstance(current, BaseException):
            current = None
    return False


def is_turso_libsql_panic_error(err: BaseException) -> bool:
    """
    Detect libsql/pyo3 panic surfaced as a Python BaseException.
    """
    current: BaseException | None = err
    for _ in range(10):
        if current is None:
            break
        t = type(current)
        if getattr(t, "__name__", "") == "PanicException":
            mod = str(getattr(t, "__module__", "") or "")
            if "pyo3" in mod or "pyo3_runtime" in mod:
                return True
        try:
            msg = str(current)
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
            msg = ""
        msg_lower = msg.lower()
        if "option::unwrap" in msg_lower:
            return True
        if "panicexception" in msg_lower and "pyo3" in msg_lower:
            return True
        orig = getattr(current, "orig", None)
        if isinstance(orig, BaseException) and orig is not current:
            current = orig
            continue
        current = (
            current.__cause__
            if isinstance(current.__cause__, BaseException)
            else current.__context__
        )
        if not isinstance(current, BaseException):
            current = None
    return False


def is_turso_retryable_error(err: BaseException) -> bool:
    if is_turso_stream_not_found_error(err):
        return True
    try:
        msg = str(err)
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        msg = ""
    msg_lower = msg.lower()
    if "timeout" in msg_lower or "timed out" in msg_lower:
        return True
    if "connection reset" in msg_lower or "broken pipe" in msg_lower:
        return True
    if "connection refused" in msg_lower:
        return True
    return False


def maybe_dispose_turso_engine_on_transient_error(
    engine: "LibsqlEngine | None", err: BaseException
) -> bool:
    if engine is None:
        return False
    if not (is_turso_stream_not_found_error(err) or is_turso_libsql_panic_error(err)):
        return False
    try:
        engine.dispose()
    except BaseException as dispose_err:
        if is_fatal_base_exception(dispose_err):
            raise
    return True


def _is_read_only_sql(query: str) -> bool:
    s = str(query or "").lstrip().lower()
    return s.startswith("select") or s.startswith("pragma") or s.startswith("explain")


def _to_sql_text(statement: Any) -> str:
    if isinstance(statement, str):
        return statement
    text_attr = getattr(statement, "text", None)
    if isinstance(text_attr, str):
        return text_attr
    return str(statement)


def _to_sequence(values: Sequence[Any]) -> tuple[Any, ...]:
    return tuple(values)


def _bind_single(query: str, params: Any) -> tuple[str, tuple[Any, ...]]:
    if params is None:
        return query, ()
    if isinstance(params, Mapping):
        converted_query, converted_params = _NAMED_TO_QMARK.format(query, params)
        return str(converted_query), tuple(converted_params)
    if isinstance(params, (list, tuple)):
        return query, _to_sequence(params)
    raise TypeError(f"unsupported_sql_params_type: {type(params).__name__}")


def _bind_many(query: str, items: Sequence[Any]) -> tuple[str, list[tuple[Any, ...]]]:
    if not items:
        return query, []
    first = items[0]
    if isinstance(first, Mapping):
        converted_query, converted_many = _NAMED_TO_QMARK.formatmany(query, items)
        bound = [tuple(row) for row in converted_many]
        return str(converted_query), bound
    if isinstance(first, (list, tuple)):
        bound = [_to_sequence(item) for item in items]
        return query, bound
    raise TypeError(f"unsupported_sql_many_item_type: {type(first).__name__}")


def _normalize_batch_params(params: Any) -> list[Any]:
    if params is None:
        return []
    if isinstance(params, list):
        return params
    if isinstance(params, tuple):
        return list(params)
    raise TypeError(f"unsupported_sql_batch_params_type: {type(params).__name__}")


class TursoMappingsResult:
    def __init__(self, cursor: Any):
        self._cursor = cursor
        description = getattr(cursor, "description", None) or ()
        self._keys = tuple(
            str(col[0]) for col in description if col and col[0] is not None
        )

    def __iter__(self):
        while True:
            row = self._cursor.fetchone()
            if row is None:
                break
            mapped = self._to_mapping(row)
            if mapped is not None:
                yield mapped

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


class TursoCursorResult:
    def __init__(self, cursor: Any):
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

    def mappings(self) -> TursoMappingsResult:
        return TursoMappingsResult(self._cursor)


class LibsqlConnection:
    def __init__(
        self,
        raw_conn: Any,
        *,
        _engine: LibsqlEngine | None = None,
        _generation: int = 0,
    ):
        self._raw = raw_conn
        self._engine = _engine
        self._generation = int(_generation)
        self._closed = False
        self._in_transaction = False

    def __enter__(self) -> "LibsqlConnection":
        return self

    def __exit__(self, _exc_type, exc, _tb) -> None:
        broken = False
        if isinstance(exc, BaseException):
            broken = bool(
                is_turso_retryable_error(exc) or is_turso_libsql_panic_error(exc)
            )
        self.close(broken=broken)

    def __getattr__(self, item: str) -> Any:
        if item == "sync":
            raise AttributeError(item)
        return getattr(self._raw, item)

    def cursor(self) -> Any:
        return self._raw.cursor()

    def close(self, *, broken: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        if self._engine is not None:
            self._engine._release_raw(
                self._raw,
                generation=self._generation,
                broken=bool(broken),
            )
            return
        self._raw.close()

    def execute(self, statement: Any, params: Any = None) -> TursoCursorResult:
        query = _to_sql_text(statement)
        is_batch = bool(
            isinstance(params, (list, tuple))
            and params
            and isinstance(params[0], (Mapping, list, tuple))
        )
        if is_batch:
            batch_params = _normalize_batch_params(params)
            prepared_query, prepared_many = _bind_many(query, batch_params)
        else:
            prepared_query, prepared_params = _bind_single(query, params)

        retries = _DEFAULT_EXECUTE_RETRIES
        if self._in_transaction or not _is_read_only_sql(prepared_query):
            retries = 0

        for attempt in range(max(0, int(retries)) + 1):
            try:
                if is_batch:
                    cursor = self._raw.executemany(prepared_query, prepared_many)
                else:
                    cursor = self._raw.execute(prepared_query, prepared_params)
                return TursoCursorResult(cursor)
            except BaseException as e:
                if is_fatal_base_exception(e):
                    raise
                maybe_dispose_turso_engine_on_transient_error(self._engine, e)
                if (
                    attempt >= int(retries)
                    or not is_turso_retryable_error(e)
                    or self._engine is None
                    or self._closed
                ):
                    raise

                old_raw = self._raw
                old_gen = int(self._generation)
                self._raw = None
                self._generation = 0
                try:
                    self._engine._release_raw(
                        old_raw,
                        generation=old_gen,
                        broken=True,
                    )
                    raw_conn, gen = self._engine._acquire_raw()
                    self._raw = raw_conn
                    self._generation = int(gen)
                except BaseException:
                    self._closed = True
                    raise

                time.sleep(_RETRY_BASE_SLEEP_SECONDS * (2**attempt))
                continue
        raise RuntimeError("turso_execute_unreachable")


@dataclass
class LibsqlEngine:
    remote_url: str
    auth_token: str
    max_connections: int = _DEFAULT_TURSO_MAX_CONNECTIONS

    _pool: queue.LifoQueue[tuple[Any, int]] = field(init=False, repr=False)
    _pool_lock: threading.Lock = field(init=False, repr=False)
    _created: int = field(init=False, default=0, repr=False)
    _generation: int = field(init=False, default=0, repr=False)

    def __post_init__(self) -> None:
        max_conns = int(self.max_connections or 0)
        if max_conns <= 0:
            max_conns = _DEFAULT_TURSO_MAX_CONNECTIONS
        self.max_connections = max(1, max_conns)
        self._pool = queue.LifoQueue(maxsize=self.max_connections)
        self._pool_lock = threading.Lock()

    def _open_raw_connection(self) -> Any:
        return libsql.connect(
            self.remote_url,
            auth_token=self.auth_token,
            # Autocommit: use DB-API compatible way.
            # Some libsql versions may not accept `autocommit=...`.
            isolation_level=None,
            _check_same_thread=False,
        )

    def _close_raw_and_decrement(self, raw_conn: Any) -> None:
        try:
            raw_conn.close()
        except Exception:
            pass
        finally:
            with self._pool_lock:
                self._created = max(0, int(self._created) - 1)

    def _acquire_raw(self) -> tuple[Any, int]:
        while True:
            try:
                raw_conn, gen = self._pool.get_nowait()
            except queue.Empty:
                raw_conn = None
                gen = -1

            if raw_conn is not None:
                if int(gen) == int(self._generation):
                    return raw_conn, int(gen)
                self._close_raw_and_decrement(raw_conn)
                continue

            with self._pool_lock:
                gen = int(self._generation)
                if int(self._created) < int(self.max_connections):
                    self._created += 1
                    create_new = True
                else:
                    create_new = False

            if create_new:
                try:
                    raw_conn = self._open_raw_connection()
                except BaseException:
                    with self._pool_lock:
                        self._created = max(0, int(self._created) - 1)
                    raise
                if int(gen) != int(self._generation):
                    self._close_raw_and_decrement(raw_conn)
                    continue
                return raw_conn, int(gen)

            try:
                raw_conn, gen = self._pool.get(timeout=_POOL_WAIT_TIMEOUT_SECONDS)
            except queue.Empty:
                continue
            if int(gen) != int(self._generation):
                self._close_raw_and_decrement(raw_conn)
                continue
            return raw_conn, int(gen)

    def _release_raw(self, raw_conn: Any, *, generation: int, broken: bool) -> None:
        if broken or int(generation) != int(self._generation):
            self._close_raw_and_decrement(raw_conn)
            return
        try:
            self._pool.put_nowait((raw_conn, int(generation)))
        except queue.Full:
            self._close_raw_and_decrement(raw_conn)

    def connect(self, *, autocommit: bool = True) -> LibsqlConnection:
        if not bool(autocommit):
            raise ValueError("turso_connection_requires_autocommit")
        raw_conn, gen = self._acquire_raw()
        return LibsqlConnection(raw_conn, _engine=self, _generation=int(gen))

    def dispose(self) -> None:
        with self._pool_lock:
            self._generation += 1
        while True:
            try:
                raw_conn, _gen = self._pool.get_nowait()
            except queue.Empty:
                break
            self._close_raw_and_decrement(raw_conn)


@contextmanager
def turso_savepoint(
    conn: LibsqlConnection, name: str = TURSO_SAVEPOINT_NAME
) -> Iterator[LibsqlConnection]:
    """
    Keep the old helper API, but use SQL BEGIN/COMMIT/ROLLBACK.
    """
    savepoint = str(name or "").strip()
    if not savepoint or _SAVEPOINT_NAME_RE.fullmatch(savepoint) is None:
        raise ValueError("invalid_savepoint_name")

    conn.execute(SQL_BEGIN)
    conn._in_transaction = True
    try:
        yield conn
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        try:
            conn.execute(SQL_ROLLBACK)
        except BaseException as rollback_e:
            if isinstance(rollback_e, _FATAL_BASE_EXCEPTIONS):
                raise
            # Preserve the original application error.
            pass
        raise
    else:
        conn.execute(SQL_COMMIT)
    finally:
        conn._in_transaction = False
