from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Iterator

from alphavault.constants import DATETIME_FMT
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
    turso_connect_autocommit,
    turso_savepoint,
)


RESEARCH_STOCK_BACKFILL_POSTS_TABLE = "research_stock_backfill_posts"


def _now_str() -> str:
    return datetime.now().strftime(DATETIME_FMT)


@contextmanager
def _use_conn(
    engine_or_conn: TursoEngine | TursoConnection,
) -> Iterator[TursoConnection]:
    if isinstance(engine_or_conn, TursoConnection):
        yield engine_or_conn
        return
    with turso_connect_autocommit(engine_or_conn) as conn:
        yield conn


def ensure_research_backfill_cache_schema(
    engine_or_conn: TursoEngine | TursoConnection,
) -> None:
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


def list_stock_backfill_posts(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    limit: int = 12,
) -> list[dict[str, object]]:
    key = str(stock_key or "").strip()
    if not key:
        return []
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


__all__ = [
    "RESEARCH_STOCK_BACKFILL_POSTS_TABLE",
    "ensure_research_backfill_cache_schema",
    "list_stock_backfill_posts",
    "replace_stock_backfill_posts",
]
