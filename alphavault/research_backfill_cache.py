from __future__ import annotations

from contextlib import contextmanager
import json
import threading
from typing import Iterator

from alphavault.db.introspect import table_columns
from alphavault.db.sql.research_backfill_cache import (
    create_research_stock_backfill_dirty_keys_index,
    create_research_stock_backfill_dirty_keys_table,
    create_research_stock_backfill_meta_index,
    create_research_stock_backfill_meta_table,
    create_research_stock_backfill_posts_index,
    create_research_stock_backfill_posts_table,
    delete_stock_backfill_posts,
    insert_stock_backfill_posts,
    select_stock_backfill_dirty_keys,
    select_stock_backfill_meta,
    select_stock_backfill_posts,
    upsert_stock_backfill_dirty_key,
    upsert_stock_backfill_meta,
)
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
    turso_connect_autocommit,
    turso_savepoint,
)
from alphavault.timeutil import now_cst_str


RESEARCH_STOCK_BACKFILL_POSTS_TABLE = "research_stock_backfill_posts"
RESEARCH_STOCK_BACKFILL_META_TABLE = "research_stock_backfill_meta"
RESEARCH_STOCK_BACKFILL_DIRTY_TABLE = "research_stock_backfill_dirty_keys"
_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
_SCHEMA_READY_LOCK = threading.RLock()
_SCHEMA_READY_KEYS: set[str] = set()


def _now_str() -> str:
    return now_cst_str()


def _ensure_backfill_posts_schema(conn: TursoConnection) -> None:
    cols = table_columns(conn, RESEARCH_STOCK_BACKFILL_POSTS_TABLE)
    if "tree_text" not in cols:
        conn.execute(
            f"ALTER TABLE {RESEARCH_STOCK_BACKFILL_POSTS_TABLE} "
            "ADD COLUMN tree_text TEXT NOT NULL DEFAULT ''"
        )


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
        conn.execute(
            create_research_stock_backfill_posts_table(
                RESEARCH_STOCK_BACKFILL_POSTS_TABLE
            )
        )
        _ensure_backfill_posts_schema(conn)
        conn.execute(
            create_research_stock_backfill_posts_index(
                RESEARCH_STOCK_BACKFILL_POSTS_TABLE
            )
        )
        conn.execute(
            create_research_stock_backfill_meta_table(
                RESEARCH_STOCK_BACKFILL_META_TABLE
            )
        )
        conn.execute(
            create_research_stock_backfill_meta_index(
                RESEARCH_STOCK_BACKFILL_META_TABLE
            )
        )
        conn.execute(
            create_research_stock_backfill_dirty_keys_table(
                RESEARCH_STOCK_BACKFILL_DIRTY_TABLE
            )
        )
        conn.execute(
            create_research_stock_backfill_dirty_keys_index(
                RESEARCH_STOCK_BACKFILL_DIRTY_TABLE
            )
        )


def ensure_research_backfill_cache_schema(
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

    with _use_conn(engine_or_conn) as conn:
        try:
            _run_schema_ddl(conn)
        except BaseException as err:
            _handle_turso_error(engine_or_conn, err)


def replace_stock_backfill_posts(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    posts: list[dict[str, str]],
) -> int:
    key = str(stock_key or "").strip()
    if not key:
        return 0
    now = _now_str()
    try:
        ensure_research_backfill_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            with turso_savepoint(conn):
                conn.execute(
                    delete_stock_backfill_posts(RESEARCH_STOCK_BACKFILL_POSTS_TABLE),
                    {"stock_key": key},
                )
                if not posts:
                    return 0
                payloads = [
                    {
                        "stock_key": key,
                        "post_uid": str(row.get("post_uid") or "").strip(),
                        "author": str(row.get("author") or "").strip(),
                        "created_at": str(row.get("created_at") or "").strip(),
                        "url": str(row.get("url") or "").strip(),
                        "matched_terms": str(row.get("matched_terms") or "").strip(),
                        "preview": str(row.get("preview") or "").strip(),
                        "tree_text": str(row.get("tree_text") or "").strip(),
                        "updated_at": now,
                    }
                    for row in posts
                    if str(row.get("post_uid") or "").strip()
                ]
                if not payloads:
                    return 0
                conn.execute(
                    insert_stock_backfill_posts(RESEARCH_STOCK_BACKFILL_POSTS_TABLE),
                    payloads,
                )
                return int(len(payloads))
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def load_stock_backfill_meta(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
) -> dict[str, object]:
    key = str(stock_key or "").strip()
    if not key:
        return {}
    try:
        ensure_research_backfill_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            row = (
                conn.execute(
                    select_stock_backfill_meta(RESEARCH_STOCK_BACKFILL_META_TABLE),
                    {"stock_key": key},
                )
                .mappings()
                .fetchone()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    if not row:
        return {}
    row_count = max(0, int(row.get("row_count") or 0))
    return {
        "stock_key": str(row.get("stock_key") or "").strip(),
        "signature": str(row.get("signature") or "").strip(),
        "row_count": int(row_count),
        "updated_at": str(row.get("updated_at") or "").strip(),
    }


def save_stock_backfill_meta(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    signature: str,
    row_count: int,
) -> None:
    key = str(stock_key or "").strip()
    if not key:
        return
    try:
        ensure_research_backfill_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            conn.execute(
                upsert_stock_backfill_meta(RESEARCH_STOCK_BACKFILL_META_TABLE),
                {
                    "stock_key": key,
                    "signature": str(signature or "").strip(),
                    "row_count": max(0, int(row_count)),
                    "updated_at": _now_str(),
                },
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def mark_stock_backfill_dirty(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    reason: str,
) -> None:
    key = str(stock_key or "").strip()
    if not key:
        return
    try:
        ensure_research_backfill_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            conn.execute(
                upsert_stock_backfill_dirty_key(RESEARCH_STOCK_BACKFILL_DIRTY_TABLE),
                {
                    "stock_key": key,
                    "reason": str(reason or "").strip(),
                    "updated_at": _now_str(),
                },
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def list_stock_backfill_dirty_keys(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    limit: int,
) -> list[str]:
    n = max(0, int(limit))
    if n <= 0:
        return []
    try:
        ensure_research_backfill_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            rows = (
                conn.execute(
                    select_stock_backfill_dirty_keys(
                        RESEARCH_STOCK_BACKFILL_DIRTY_TABLE
                    ),
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


def remove_stock_backfill_dirty_keys(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_keys: list[str],
) -> int:
    keys = [str(item or "").strip() for item in stock_keys if str(item or "").strip()]
    if not keys:
        return 0
    placeholders = ", ".join(["?"] * len(keys))
    sql = (
        f"DELETE FROM {RESEARCH_STOCK_BACKFILL_DIRTY_TABLE} "
        f"WHERE stock_key IN ({placeholders})"
    )
    try:
        ensure_research_backfill_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            with turso_savepoint(conn):
                res = conn.execute(sql, keys)
                return int(res.rowcount or 0)
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    return 0


def _parse_json_str_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item or "").strip() for item in value if str(item or "").strip()]
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item or "").strip() for item in parsed if str(item or "").strip()]


def mark_stock_backfill_dirty_from_assertions(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    assertions: list[dict[str, object]],
    reason: str,
) -> int:
    keys: set[str] = set()
    for row in assertions:
        if not isinstance(row, dict):
            continue
        topic_key = str(row.get("topic_key") or "").strip()
        if topic_key.startswith("stock:"):
            keys.add(topic_key)
        for raw_code in _parse_json_str_list(row.get("stock_codes_json")):
            keys.add(f"stock:{raw_code}")
    for key in sorted(keys):
        mark_stock_backfill_dirty(engine_or_conn, stock_key=key, reason=reason)
    return int(len(keys))


def list_stock_backfill_posts(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    limit: int = 12,
) -> list[dict[str, object]]:
    key = str(stock_key or "").strip()
    if not key:
        return []
    try:
        ensure_research_backfill_cache_schema(engine_or_conn)
        with _use_conn(engine_or_conn) as conn:
            return (
                conn.execute(
                    select_stock_backfill_posts(RESEARCH_STOCK_BACKFILL_POSTS_TABLE),
                    {"stock_key": key, "limit": max(0, int(limit))},
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    raise AssertionError("unreachable")


__all__ = [
    "RESEARCH_STOCK_BACKFILL_DIRTY_TABLE",
    "RESEARCH_STOCK_BACKFILL_META_TABLE",
    "RESEARCH_STOCK_BACKFILL_POSTS_TABLE",
    "ensure_research_backfill_cache_schema",
    "list_stock_backfill_dirty_keys",
    "list_stock_backfill_posts",
    "load_stock_backfill_meta",
    "mark_stock_backfill_dirty",
    "mark_stock_backfill_dirty_from_assertions",
    "remove_stock_backfill_dirty_keys",
    "replace_stock_backfill_posts",
    "save_stock_backfill_meta",
]
