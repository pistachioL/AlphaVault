from __future__ import annotations

from contextlib import contextmanager
import threading
from typing import Iterator

from alphavault.timeutil import now_cst_str
from alphavault.db.sql.research_backfill_cache import (
    create_research_stock_backfill_posts_index,
    create_research_stock_backfill_posts_table,
    delete_stock_backfill_posts,
    insert_stock_backfill_posts,
    select_stock_backfill_posts,
)
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
    turso_connect_autocommit,
    turso_savepoint,
)


RESEARCH_STOCK_BACKFILL_POSTS_TABLE = "research_stock_backfill_posts"
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
        conn.execute(
            create_research_stock_backfill_posts_table(
                RESEARCH_STOCK_BACKFILL_POSTS_TABLE
            )
        )
        conn.execute(
            create_research_stock_backfill_posts_index(
                RESEARCH_STOCK_BACKFILL_POSTS_TABLE
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
    "RESEARCH_STOCK_BACKFILL_POSTS_TABLE",
    "ensure_research_backfill_cache_schema",
    "list_stock_backfill_posts",
    "replace_stock_backfill_posts",
]
