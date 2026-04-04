from __future__ import annotations

from contextlib import contextmanager
import json
import threading
from typing import Iterator

from alphavault.db.sql.homework_trade_feed import (
    create_homework_trade_feed_index,
    create_homework_trade_feed_table,
    select_homework_trade_feed,
    upsert_homework_trade_feed,
)
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
    turso_connect_autocommit,
)
from alphavault.timeutil import now_cst_str

HOMEWORK_TRADE_FEED_TABLE = "homework_trade_feed"
HOMEWORK_DEFAULT_VIEW_KEY = "default"

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
_SCHEMA_READY_LOCK = threading.RLock()
_SCHEMA_READY_KEYS: set[str] = set()


def _now_str() -> str:
    return now_cst_str()


@contextmanager
def _use_conn(
    engine_or_conn: TursoEngine | TursoConnection,
) -> Iterator[TursoConnection]:
    if isinstance(engine_or_conn, TursoConnection):
        yield engine_or_conn
        return
    with turso_connect_autocommit(engine_or_conn) as conn:
        yield conn


def _resolve_engine(
    engine_or_conn: TursoEngine | TursoConnection,
) -> TursoEngine | None:
    return (
        engine_or_conn._engine
        if isinstance(engine_or_conn, TursoConnection)
        else engine_or_conn
    )


def _schema_cache_key(engine_or_conn: TursoEngine | TursoConnection) -> str:
    engine = _resolve_engine(engine_or_conn)
    if engine is None:
        return ""
    return (
        f"{str(engine.remote_url or '').strip()}|{str(engine.auth_token or '').strip()}"
    )


def _clear_schema_ready(engine_or_conn: TursoEngine | TursoConnection) -> None:
    cache_key = _schema_cache_key(engine_or_conn)
    if not cache_key:
        return
    with _SCHEMA_READY_LOCK:
        _SCHEMA_READY_KEYS.discard(cache_key)


def _handle_turso_error(
    engine_or_conn: TursoEngine | TursoConnection, err: BaseException
) -> None:
    if isinstance(err, _FATAL_BASE_EXCEPTIONS):
        raise err
    engine = _resolve_engine(engine_or_conn)
    if engine is not None and (
        is_turso_stream_not_found_error(err) or is_turso_libsql_panic_error(err)
    ):
        _clear_schema_ready(engine_or_conn)
        engine.dispose()
    raise err


def _run_schema_ddl(engine_or_conn: TursoEngine | TursoConnection) -> None:
    with _use_conn(engine_or_conn) as conn:
        conn.execute(create_homework_trade_feed_table(HOMEWORK_TRADE_FEED_TABLE))
        conn.execute(create_homework_trade_feed_index(HOMEWORK_TRADE_FEED_TABLE))


def ensure_homework_trade_feed_schema(
    engine_or_conn: TursoEngine | TursoConnection,
) -> None:
    cache_key = _schema_cache_key(engine_or_conn)
    if cache_key:
        with _SCHEMA_READY_LOCK:
            if cache_key in _SCHEMA_READY_KEYS:
                return
            try:
                _run_schema_ddl(engine_or_conn)
            except BaseException as err:
                _handle_turso_error(engine_or_conn, err)
            _SCHEMA_READY_KEYS.add(cache_key)
        return
    try:
        _run_schema_ddl(engine_or_conn)
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _clean_rows(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for row in value:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                str(key): _clean_text(raw)
                for key, raw in row.items()
                if str(key or "").strip()
            }
        )
    return out


def _json_load_dict(value: object) -> dict[str, object]:
    text = _clean_text(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_load_rows(value: object) -> list[dict[str, str]]:
    text = _clean_text(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return _clean_rows(parsed)


def _coerce_positive_int(value: object, *, default: int) -> int:
    text = _clean_text(value)
    if not text:
        return max(1, int(default))
    try:
        parsed = int(text)
    except (TypeError, ValueError):
        return max(1, int(default))
    return max(1, parsed)


def _coerce_non_negative_int(value: object, *, default: int) -> int:
    text = _clean_text(value)
    if not text:
        return max(0, int(default))
    try:
        parsed = int(text)
    except (TypeError, ValueError):
        return max(0, int(default))
    return max(0, parsed)


def save_homework_trade_feed(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    view_key: str,
    caption: str,
    used_window_days: int,
    rows: list[dict[str, object]] | list[dict[str, str]],
) -> None:
    key = _clean_text(view_key)
    if not key:
        return
    cleaned_rows = _clean_rows(rows)
    params = {
        "view_key": key,
        "header_json": json.dumps(
            {
                "caption": _clean_text(caption),
                "used_window_days": _coerce_positive_int(
                    used_window_days,
                    default=1,
                ),
            },
            ensure_ascii=False,
        ),
        "items_json": json.dumps(cleaned_rows, ensure_ascii=False),
        "counters_json": json.dumps(
            {"row_count": len(cleaned_rows)},
            ensure_ascii=False,
        ),
        "updated_at": _now_str(),
    }
    try:
        ensure_homework_trade_feed_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            conn.execute(upsert_homework_trade_feed(HOMEWORK_TRADE_FEED_TABLE), params)
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def load_homework_trade_feed(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    view_key: str,
) -> dict[str, object]:
    key = _clean_text(view_key)
    if not key:
        return {}
    try:
        ensure_homework_trade_feed_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            row = (
                conn.execute(
                    select_homework_trade_feed(HOMEWORK_TRADE_FEED_TABLE),
                    {"view_key": key},
                )
                .mappings()
                .fetchone()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    if not row:
        return {}
    header = _json_load_dict(row.get("header_json"))
    counters = _json_load_dict(row.get("counters_json"))
    rows = _json_load_rows(row.get("items_json"))
    return {
        "view_key": _clean_text(row.get("view_key")),
        "caption": _clean_text(header.get("caption")),
        "used_window_days": _coerce_positive_int(
            header.get("used_window_days"),
            default=1,
        ),
        "rows": rows,
        "row_count": max(
            0,
            _coerce_non_negative_int(
                counters.get("row_count"),
                default=len(rows),
            ),
        ),
        "updated_at": _clean_text(row.get("updated_at")),
    }


__all__ = [
    "HOMEWORK_DEFAULT_VIEW_KEY",
    "HOMEWORK_TRADE_FEED_TABLE",
    "ensure_homework_trade_feed_schema",
    "load_homework_trade_feed",
    "save_homework_trade_feed",
]
