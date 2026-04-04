from __future__ import annotations

from contextlib import contextmanager
import json
import threading
from typing import Iterator

from alphavault.content_hash import build_content_hash
from alphavault.db.introspect import table_columns
from alphavault.domains.common.assertion_entities import extract_stock_entity_keys
from alphavault.timeutil import now_cst_str
from alphavault.db.sql.research_stock_cache import (
    create_entity_page_snapshot_index,
    create_entity_page_snapshot_table,
    create_research_stock_dirty_keys_index,
    create_research_stock_dirty_keys_table,
    select_entity_page_snapshot,
    select_research_stock_dirty_keys,
    upsert_entity_page_snapshot_extras,
    upsert_entity_page_snapshot_hot,
    upsert_research_stock_dirty_key,
)
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
    turso_connect_autocommit,
    turso_savepoint,
)

ENTITY_PAGE_SNAPSHOT_TABLE = "entity_page_snapshot"
PROJECTION_DIRTY_TABLE = "projection_dirty"
PROJECTION_JOB_TYPE_ENTITY_PAGE = "entity_page"

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
        conn.execute(create_entity_page_snapshot_table(ENTITY_PAGE_SNAPSHOT_TABLE))
        _ensure_entity_page_snapshot_schema(conn)
        conn.execute(create_entity_page_snapshot_index(ENTITY_PAGE_SNAPSHOT_TABLE))
        conn.execute(create_research_stock_dirty_keys_table(PROJECTION_DIRTY_TABLE))
        conn.execute(create_research_stock_dirty_keys_index(PROJECTION_DIRTY_TABLE))


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


def _ensure_entity_page_snapshot_schema(conn: TursoConnection) -> None:
    cols = table_columns(conn, ENTITY_PAGE_SNAPSHOT_TABLE)
    if "content_hash" not in cols:
        conn.execute(
            f"ALTER TABLE {ENTITY_PAGE_SNAPSHOT_TABLE} "
            "ADD COLUMN content_hash TEXT NOT NULL DEFAULT ''"
        )


def _select_entity_page_snapshot_row(
    conn: TursoConnection,
    *,
    entity_key: str,
) -> dict[str, object]:
    row = (
        conn.execute(
            select_entity_page_snapshot(ENTITY_PAGE_SNAPSHOT_TABLE),
            {"entity_key": entity_key},
        )
        .mappings()
        .fetchone()
    )
    return dict(row) if row else {}


def _entity_page_snapshot_content_hash(
    *,
    header_title: str,
    signal_total: int,
    signals: list[dict[str, str]],
    related_sectors: list[dict[str, str]],
    backfill_posts: list[dict[str, str]],
) -> str:
    return build_content_hash(
        {
            "header_title": str(header_title or "").strip(),
            "signal_total": max(0, int(signal_total)),
            "signals": _clean_json_rows(signals),
            "related_sectors": _clean_json_rows(related_sectors),
            "backfill_posts": _clean_json_rows(backfill_posts),
        }
    )


def _entity_page_snapshot_row_hash(row: dict[str, object]) -> str:
    if not row:
        return ""
    stored_hash = str(row.get("content_hash") or "").strip()
    if stored_hash:
        return stored_hash
    return _entity_page_snapshot_content_hash(
        header_title=str(row.get("header_title") or "").strip(),
        signal_total=_coerce_non_negative_int(row.get("signal_total"), default=0),
        signals=_json_list(row.get("signals_json")),
        related_sectors=_json_list(row.get("related_sectors_json")),
        backfill_posts=_json_list(row.get("backfill_posts_json")),
    )


def save_entity_page_signal_snapshot(
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
    entity_key = str(payload.get("entity_key") or key).strip() or key
    header_title = str(payload.get("header_title") or "").strip()
    signal_total = _coerce_non_negative_int(
        payload.get("signal_total"),
        default=len(signals),
    )
    try:
        ensure_research_stock_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            existing_row = _select_entity_page_snapshot_row(conn, entity_key=entity_key)
            content_hash = _entity_page_snapshot_content_hash(
                header_title=header_title,
                signal_total=signal_total,
                signals=signals,
                related_sectors=related_sectors,
                backfill_posts=_json_list(existing_row.get("backfill_posts_json")),
            )
            if (
                existing_row
                and _entity_page_snapshot_row_hash(existing_row) == content_hash
            ):
                return
            conn.execute(
                upsert_entity_page_snapshot_hot(ENTITY_PAGE_SNAPSHOT_TABLE),
                {
                    "entity_key": entity_key,
                    "header_title": header_title,
                    "signal_total": signal_total,
                    "signals_json": _json_dumps(signals),
                    "related_sectors_json": _json_dumps(related_sectors),
                    "content_hash": content_hash,
                    "updated_at": _now_str(),
                },
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def load_entity_page_signal_snapshot(
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
            row = _select_entity_page_snapshot_row(conn, entity_key=key)
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    if not row:
        return {}
    return {
        "entity_key": str(row.get("entity_key") or "").strip(),
        "header_title": str(row.get("header_title") or "").strip(),
        "signal_total": _coerce_non_negative_int(row.get("signal_total"), default=0),
        "signals": _json_list(row.get("signals_json")),
        "related_sectors": _json_list(row.get("related_sectors_json")),
        "updated_at": str(row.get("updated_at") or "").strip(),
    }


def save_entity_page_backfill_snapshot(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    backfill_posts: list[dict[str, object]] | list[dict[str, str]],
) -> None:
    key = str(stock_key or "").strip()
    if not key:
        return
    backfill_rows = _clean_json_rows(backfill_posts)
    try:
        ensure_research_stock_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            existing_row = _select_entity_page_snapshot_row(conn, entity_key=key)
            content_hash = _entity_page_snapshot_content_hash(
                header_title=str(existing_row.get("header_title") or "").strip(),
                signal_total=_coerce_non_negative_int(
                    existing_row.get("signal_total"),
                    default=0,
                ),
                signals=_json_list(existing_row.get("signals_json")),
                related_sectors=_json_list(existing_row.get("related_sectors_json")),
                backfill_posts=backfill_rows,
            )
            if (
                existing_row
                and _entity_page_snapshot_row_hash(existing_row) == content_hash
            ):
                return
            conn.execute(
                upsert_entity_page_snapshot_extras(ENTITY_PAGE_SNAPSHOT_TABLE),
                {
                    "entity_key": key,
                    "backfill_posts_json": _json_dumps(backfill_rows),
                    "content_hash": content_hash,
                    "updated_at": _now_str(),
                },
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def load_entity_page_backfill_snapshot(
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
            row = _select_entity_page_snapshot_row(conn, entity_key=key)
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    if not row:
        return {}
    return {
        "stock_key": str(row.get("entity_key") or "").strip(),
        "backfill_posts": _json_list(row.get("backfill_posts_json")),
        "updated_at": str(row.get("updated_at") or "").strip(),
    }


def mark_entity_page_dirty(
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
                upsert_research_stock_dirty_key(PROJECTION_DIRTY_TABLE),
                {
                    "job_type": PROJECTION_JOB_TYPE_ENTITY_PAGE,
                    "target_key": key,
                    "reason": str(reason or "").strip(),
                    "updated_at": _now_str(),
                },
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def list_entity_page_dirty_keys(
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
                    select_research_stock_dirty_keys(PROJECTION_DIRTY_TABLE),
                    {
                        "job_type": PROJECTION_JOB_TYPE_ENTITY_PAGE,
                        "limit": n,
                    },
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    return [
        str(row.get("target_key") or "").strip()
        for row in rows
        if str(row.get("target_key") or "").strip()
    ]


def list_entity_page_dirty_entries(
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
                    select_research_stock_dirty_keys(PROJECTION_DIRTY_TABLE),
                    {
                        "job_type": PROJECTION_JOB_TYPE_ENTITY_PAGE,
                        "limit": n,
                    },
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    out: list[dict[str, str]] = []
    for row in rows:
        stock_key = str(row.get("target_key") or "").strip()
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


def remove_entity_page_dirty_keys(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_keys: list[str],
) -> int:
    keys = [str(item or "").strip() for item in stock_keys if str(item or "").strip()]
    if not keys:
        return 0
    placeholders = ", ".join(["?"] * len(keys))
    sql = f"""
DELETE FROM {PROJECTION_DIRTY_TABLE}
WHERE job_type = ?
  AND target_key IN ({placeholders})
"""
    try:
        ensure_research_stock_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            with turso_savepoint(conn):
                res = conn.execute(
                    sql,
                    [PROJECTION_JOB_TYPE_ENTITY_PAGE, *keys],
                )
                return int(res.rowcount or 0)
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    return 0


def pop_entity_page_dirty_keys(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    limit: int,
) -> list[str]:
    keys = list_entity_page_dirty_keys(engine_or_conn, limit=limit)
    if not keys:
        return []
    remove_entity_page_dirty_keys(engine_or_conn, stock_keys=keys)
    return keys


def mark_entity_page_dirty_from_assertions(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    assertions: list[dict[str, object]],
    reason: str,
) -> int:
    keys = extract_stock_entity_keys(assertions)
    for key in keys:
        mark_entity_page_dirty(engine_or_conn, stock_key=key, reason=reason)
    return len(keys)


__all__ = [
    "ENTITY_PAGE_SNAPSHOT_TABLE",
    "PROJECTION_DIRTY_TABLE",
    "PROJECTION_JOB_TYPE_ENTITY_PAGE",
    "ensure_research_stock_cache_schema",
    "list_entity_page_dirty_entries",
    "list_entity_page_dirty_keys",
    "load_entity_page_backfill_snapshot",
    "load_entity_page_signal_snapshot",
    "mark_entity_page_dirty",
    "mark_entity_page_dirty_from_assertions",
    "pop_entity_page_dirty_keys",
    "remove_entity_page_dirty_keys",
    "save_entity_page_backfill_snapshot",
    "save_entity_page_signal_snapshot",
]
