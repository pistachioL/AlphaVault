from __future__ import annotations

from contextlib import contextmanager
import json
import threading
from typing import Iterator

from alphavault.domains.common.assertion_entities import extract_stock_entity_keys
from alphavault.timeutil import now_cst_str
from alphavault.db.sql.research_stock_cache import (
    create_research_stock_dirty_keys_index,
    create_research_stock_dirty_keys_table,
    create_research_stock_extras_index,
    create_research_stock_extras_table,
    create_research_stock_hot_index,
    create_research_stock_hot_table,
    select_research_stock_dirty_keys,
    select_research_stock_extras,
    select_research_stock_hot,
    upsert_research_stock_dirty_key,
    upsert_research_stock_extras,
    upsert_research_stock_hot,
)
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
    turso_connect_autocommit,
    turso_savepoint,
)

RESEARCH_STOCK_HOT_TABLE = "research_stock_signals_hot"
RESEARCH_STOCK_EXTRAS_TABLE = "research_stock_extras_snapshot"
RESEARCH_STOCK_DIRTY_TABLE = "research_stock_dirty_keys"

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
        conn.execute(create_research_stock_hot_table(RESEARCH_STOCK_HOT_TABLE))
        conn.execute(create_research_stock_hot_index(RESEARCH_STOCK_HOT_TABLE))
        conn.execute(create_research_stock_extras_table(RESEARCH_STOCK_EXTRAS_TABLE))
        conn.execute(create_research_stock_extras_index(RESEARCH_STOCK_EXTRAS_TABLE))
        conn.execute(create_research_stock_dirty_keys_table(RESEARCH_STOCK_DIRTY_TABLE))
        conn.execute(create_research_stock_dirty_keys_index(RESEARCH_STOCK_DIRTY_TABLE))


def ensure_research_stock_cache_schema(
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


def _clean_json_rows(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for row in value:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                str(key): str(raw or "").strip()
                for key, raw in row.items()
                if str(key).strip()
            }
        )
    return out


def _json_list(value: object) -> list[dict[str, str]]:
    if isinstance(value, list):
        return _clean_json_rows(value)
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return _clean_json_rows(parsed)


def _json_dumps(value: list[dict[str, str]]) -> str:
    return json.dumps(_clean_json_rows(value), ensure_ascii=False)


def _coerce_non_negative_int(value: object, *, default: int) -> int:
    text = str(value or "").strip()
    if not text:
        return max(int(default), 0)
    try:
        parsed = int(text)
    except (TypeError, ValueError):
        return max(int(default), 0)
    return max(parsed, 0)


def save_stock_hot_view(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    payload: dict[str, object],
) -> None:
    key = str(stock_key or "").strip()
    if not key:
        return
    signals = _clean_json_rows(payload.get("signals"))
    related_sectors = _clean_json_rows(payload.get("related_sectors"))
    params = {
        "stock_key": key,
        "entity_key": str(payload.get("entity_key") or key).strip(),
        "header_title": str(payload.get("header_title") or "").strip(),
        "signal_total": _coerce_non_negative_int(
            payload.get("signal_total"),
            default=len(signals),
        ),
        "signals_json": _json_dumps(signals),
        "related_sectors_json": _json_dumps(related_sectors),
        "updated_at": _now_str(),
    }
    try:
        ensure_research_stock_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            conn.execute(upsert_research_stock_hot(RESEARCH_STOCK_HOT_TABLE), params)
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def load_stock_hot_view(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
) -> dict[str, object]:
    key = str(stock_key or "").strip()
    if not key:
        return {}
    try:
        ensure_research_stock_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            row = (
                conn.execute(
                    select_research_stock_hot(RESEARCH_STOCK_HOT_TABLE),
                    {"stock_key": key},
                )
                .mappings()
                .fetchone()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    if not row:
        return {}
    return {
        "stock_key": str(row.get("stock_key") or "").strip(),
        "entity_key": str(row.get("entity_key") or "").strip(),
        "header_title": str(row.get("header_title") or "").strip(),
        "signal_total": _coerce_non_negative_int(row.get("signal_total"), default=0),
        "signals": _json_list(row.get("signals_json")),
        "related_sectors": _json_list(row.get("related_sectors_json")),
        "updated_at": str(row.get("updated_at") or "").strip(),
    }


def save_stock_extras_snapshot(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    pending_candidates: list[dict[str, object]] | list[dict[str, str]],
    backfill_posts: list[dict[str, object]] | list[dict[str, str]],
) -> None:
    key = str(stock_key or "").strip()
    if not key:
        return
    pending_rows = _clean_json_rows(pending_candidates)
    backfill_rows = _clean_json_rows(backfill_posts)
    params = {
        "stock_key": key,
        "pending_candidates_json": _json_dumps(pending_rows),
        "backfill_posts_json": _json_dumps(backfill_rows),
        "updated_at": _now_str(),
    }
    try:
        ensure_research_stock_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            conn.execute(
                upsert_research_stock_extras(RESEARCH_STOCK_EXTRAS_TABLE),
                params,
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def load_stock_extras_snapshot(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
) -> dict[str, object]:
    key = str(stock_key or "").strip()
    if not key:
        return {}
    try:
        ensure_research_stock_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            row = (
                conn.execute(
                    select_research_stock_extras(RESEARCH_STOCK_EXTRAS_TABLE),
                    {"stock_key": key},
                )
                .mappings()
                .fetchone()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    if not row:
        return {}
    return {
        "stock_key": str(row.get("stock_key") or "").strip(),
        "pending_candidates": _json_list(row.get("pending_candidates_json")),
        "backfill_posts": _json_list(row.get("backfill_posts_json")),
        "updated_at": str(row.get("updated_at") or "").strip(),
    }


def mark_stock_dirty(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    reason: str,
) -> None:
    key = str(stock_key or "").strip()
    if not key:
        return
    try:
        ensure_research_stock_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            conn.execute(
                upsert_research_stock_dirty_key(RESEARCH_STOCK_DIRTY_TABLE),
                {
                    "stock_key": key,
                    "reason": str(reason or "").strip(),
                    "updated_at": _now_str(),
                },
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def list_stock_dirty_keys(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    limit: int,
) -> list[str]:
    n = max(0, int(limit))
    if n <= 0:
        return []
    try:
        ensure_research_stock_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            rows = (
                conn.execute(
                    select_research_stock_dirty_keys(RESEARCH_STOCK_DIRTY_TABLE),
                    {"limit": n},
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    return [
        str(row.get("stock_key") or "").strip()
        for row in rows
        if str(row.get("stock_key") or "").strip()
    ]


def list_stock_dirty_entries(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    limit: int,
) -> list[dict[str, str]]:
    n = max(0, int(limit))
    if n <= 0:
        return []
    try:
        ensure_research_stock_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            rows = (
                conn.execute(
                    select_research_stock_dirty_keys(RESEARCH_STOCK_DIRTY_TABLE),
                    {"limit": n},
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    out: list[dict[str, str]] = []
    for row in rows:
        stock_key = str(row.get("stock_key") or "").strip()
        if not stock_key:
            continue
        out.append(
            {
                "stock_key": stock_key,
                "reason": str(row.get("reason") or "").strip(),
                "updated_at": str(row.get("updated_at") or "").strip(),
            }
        )
    return out


def remove_stock_dirty_keys(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_keys: list[str],
) -> int:
    keys = [str(item or "").strip() for item in stock_keys if str(item or "").strip()]
    if not keys:
        return 0
    placeholders = ", ".join(["?"] * len(keys))
    sql = (
        f"DELETE FROM {RESEARCH_STOCK_DIRTY_TABLE} WHERE stock_key IN ({placeholders})"
    )
    try:
        ensure_research_stock_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            with turso_savepoint(conn):
                res = conn.execute(sql, keys)
                return int(res.rowcount or 0)
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    return 0


def pop_stock_dirty_keys(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    limit: int,
) -> list[str]:
    keys = list_stock_dirty_keys(engine_or_conn, limit=limit)
    if not keys:
        return []
    remove_stock_dirty_keys(engine_or_conn, stock_keys=keys)
    return keys


def mark_stock_dirty_from_assertions(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    assertions: list[dict[str, object]],
    reason: str,
) -> int:
    keys = extract_stock_entity_keys(assertions)
    for key in keys:
        mark_stock_dirty(engine_or_conn, stock_key=key, reason=reason)
    return len(keys)


__all__ = [
    "RESEARCH_STOCK_DIRTY_TABLE",
    "RESEARCH_STOCK_EXTRAS_TABLE",
    "RESEARCH_STOCK_HOT_TABLE",
    "ensure_research_stock_cache_schema",
    "list_stock_dirty_entries",
    "list_stock_dirty_keys",
    "load_stock_extras_snapshot",
    "load_stock_hot_view",
    "mark_stock_dirty",
    "mark_stock_dirty_from_assertions",
    "pop_stock_dirty_keys",
    "remove_stock_dirty_keys",
    "save_stock_extras_snapshot",
    "save_stock_hot_view",
]
