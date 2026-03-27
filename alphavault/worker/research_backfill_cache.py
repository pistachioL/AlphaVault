from __future__ import annotations

import re
import time
from typing import Any

from alphavault.db.introspect import table_columns
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    turso_connect_autocommit,
)
from alphavault.research_backfill_cache import (
    list_stock_backfill_posts,
    replace_stock_backfill_posts,
)
from alphavault.research_stock_cache import mark_stock_dirty
from alphavault.worker.job_state import (
    load_worker_job_cursor,
    release_worker_job_lock,
    save_worker_job_cursor,
    try_acquire_worker_job_lock,
)


BACKFILL_CACHE_CURSOR_KEY = "stock_backfill_cache.stock_cursor"
BACKFILL_CACHE_LOCK_KEY = "stock_backfill_cache.lock"
BACKFILL_CACHE_LOCK_LEASE_SECONDS = 600

BACKFILL_CACHE_MAX_STOCKS_PER_RUN = 2
BACKFILL_CACHE_MAX_ROWS_PER_STOCK = 12
BACKFILL_CACHE_POST_BATCH_SIZE = 200

_WS_RE = re.compile(r"\s+")
_STOCK_KEY_PREFIX = "stock:"


def _select_stock_keys_batch(
    conn: TursoConnection,
    *,
    after_stock_key: str,
    limit: int,
) -> list[str]:
    sql = """
SELECT DISTINCT topic_key
FROM assertions
WHERE action LIKE 'trade.%'
  AND topic_key LIKE 'stock:%'
  AND topic_key > :after_stock_key
ORDER BY topic_key ASC
LIMIT :limit
"""
    rows = conn.execute(
        sql,
        {
            "after_stock_key": str(after_stock_key or "").strip(),
            "limit": max(0, int(limit)),
        },
    ).fetchall()
    return [
        str(row[0] or "").strip() for row in rows if row and str(row[0] or "").strip()
    ]


def _has_more_stock_keys(conn: TursoConnection, *, after_stock_key: str) -> bool:
    sql = """
SELECT 1
FROM assertions
WHERE action LIKE 'trade.%'
  AND topic_key LIKE 'stock:%'
  AND topic_key > :after_stock_key
LIMIT 1
"""
    row = conn.execute(
        sql, {"after_stock_key": str(after_stock_key or "").strip()}
    ).fetchone()
    return bool(row)


def _stock_terms(stock_key: str) -> list[str]:
    raw_key = str(stock_key or "").strip()
    value = (
        raw_key[len(_STOCK_KEY_PREFIX) :]
        if raw_key.startswith(_STOCK_KEY_PREFIX)
        else raw_key
    )
    candidates: list[str] = []
    if value:
        candidates.append(value)
        if "." in value:
            short = value.split(".", 1)[0].strip()
            if short:
                candidates.append(short)

    out: list[str] = []
    seen: set[str] = set()
    for term in sorted(candidates, key=lambda item: (-len(item), item)):
        text = str(term or "").strip()
        if not text or text in seen:
            continue
        if len(text) < 2 and not any(char.isdigit() for char in text):
            continue
        seen.add(text)
        out.append(text)
    return out


def _select_post_batch(
    conn: TursoConnection,
    *,
    stock_key: str,
    cursor_created_at: str,
    cursor_post_uid: str,
    limit: int,
) -> list[dict[str, Any]]:
    post_cols = table_columns(conn, "posts")
    display_expr = "display_md" if "display_md" in post_cols else "'' AS display_md"
    cursor_clause = ""
    params: dict[str, Any] = {
        "stock_key": str(stock_key or "").strip(),
        "limit": max(1, int(limit)),
    }
    if str(cursor_created_at or "").strip() and str(cursor_post_uid or "").strip():
        cursor_clause = """
  AND (created_at < :cursor_created_at OR (created_at = :cursor_created_at AND post_uid < :cursor_post_uid))
"""
        params["cursor_created_at"] = str(cursor_created_at or "").strip()
        params["cursor_post_uid"] = str(cursor_post_uid or "").strip()

    sql = f"""
SELECT post_uid, author, created_at, url, raw_text, {display_expr}
FROM posts
WHERE processed_at IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM assertions a
    WHERE a.post_uid = posts.post_uid
      AND a.topic_key = :stock_key
      AND a.action LIKE 'trade.%'
  )
{cursor_clause}
ORDER BY created_at DESC, post_uid DESC
LIMIT :limit
"""
    rows = conn.execute(sql, params).mappings().all()
    return [dict(row) for row in rows if row]


def _preview_text(text: str) -> str:
    preview = _WS_RE.sub(" ", str(text or "").strip()).strip()
    if len(preview) > 180:
        return f"{preview[:177]}..."
    return preview


def _build_backfill_candidates_for_stock(
    conn: TursoConnection,
    *,
    stock_key: str,
    max_rows: int,
    post_batch_size: int,
) -> list[dict[str, str]]:
    terms = _stock_terms(stock_key)
    if not terms:
        return []
    terms_lower = [term.lower() for term in terms]

    out: list[dict[str, str]] = []
    cursor_created_at = ""
    cursor_post_uid = ""
    max_items = max(1, int(max_rows))
    batch_size = max(1, int(post_batch_size))
    while len(out) < max_items:
        rows = _select_post_batch(
            conn,
            stock_key=stock_key,
            cursor_created_at=cursor_created_at,
            cursor_post_uid=cursor_post_uid,
            limit=batch_size,
        )
        if not rows:
            break
        last = rows[-1]
        cursor_created_at = (
            str(last.get("created_at") or "").strip() or cursor_created_at
        )
        cursor_post_uid = str(last.get("post_uid") or "").strip() or cursor_post_uid

        for row in rows:
            if len(out) >= max_items:
                break
            post_uid = str(row.get("post_uid") or "").strip()
            if not post_uid:
                continue
            raw_text = str(row.get("raw_text") or "").strip()
            display_md = str(row.get("display_md") or "").strip()
            haystack = (raw_text or display_md).strip()
            if not haystack:
                continue
            haystack_lower = haystack.lower()
            matched_terms = [
                term
                for term, term_lower in zip(terms, terms_lower, strict=False)
                if term_lower and term_lower in haystack_lower
            ]
            if not matched_terms:
                continue
            out.append(
                {
                    "post_uid": post_uid,
                    "author": str(row.get("author") or "").strip(),
                    "created_at": str(row.get("created_at") or "").strip(),
                    "url": str(row.get("url") or "").strip(),
                    "matched_terms": ", ".join(matched_terms[:3]),
                    "preview": _preview_text(haystack),
                }
            )
    return out


def _backfill_rows_signature(rows: list[dict[str, Any]]) -> list[tuple[str, ...]]:
    normalized = [
        (
            str(row.get("post_uid") or "").strip(),
            str(row.get("author") or "").strip(),
            str(row.get("created_at") or "").strip(),
            str(row.get("url") or "").strip(),
            str(row.get("matched_terms") or "").strip(),
            str(row.get("preview") or "").strip(),
        )
        for row in rows
        if str(row.get("post_uid") or "").strip()
    ]
    return sorted(normalized, key=lambda item: (item[2], item[0]), reverse=True)


def sync_stock_backfill_cache(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    max_stocks_per_run: int = BACKFILL_CACHE_MAX_STOCKS_PER_RUN,
    max_rows_per_stock: int = BACKFILL_CACHE_MAX_ROWS_PER_STOCK,
    post_batch_size: int = BACKFILL_CACHE_POST_BATCH_SIZE,
    lock_lease_seconds: int = BACKFILL_CACHE_LOCK_LEASE_SECONDS,
) -> dict[str, int | bool]:
    now_epoch = int(time.time())
    if not try_acquire_worker_job_lock(
        engine_or_conn,
        lock_key=BACKFILL_CACHE_LOCK_KEY,
        now_epoch=now_epoch,
        lease_seconds=int(lock_lease_seconds),
    ):
        return {"processed": 0, "written": 0, "has_more": False, "locked": True}
    try:
        cursor = load_worker_job_cursor(
            engine_or_conn, state_key=BACKFILL_CACHE_CURSOR_KEY
        )
        with (
            turso_connect_autocommit(engine_or_conn)
            if isinstance(engine_or_conn, TursoEngine)
            else engine_or_conn
        ) as conn:
            stock_keys = _select_stock_keys_batch(
                conn,
                after_stock_key=cursor,
                limit=max(1, int(max_stocks_per_run)),
            )
            if not stock_keys:
                if cursor:
                    save_worker_job_cursor(
                        engine_or_conn,
                        state_key=BACKFILL_CACHE_CURSOR_KEY,
                        cursor="",
                    )
                return {"processed": 0, "written": 0, "has_more": False}

            total_written = 0
            for stock_key in stock_keys:
                before_rows = list_stock_backfill_posts(
                    conn,
                    stock_key=stock_key,
                    limit=max(1, int(max_rows_per_stock)),
                )
                rows = _build_backfill_candidates_for_stock(
                    conn,
                    stock_key=stock_key,
                    max_rows=int(max_rows_per_stock),
                    post_batch_size=int(post_batch_size),
                )
                total_written += replace_stock_backfill_posts(
                    conn,
                    stock_key=stock_key,
                    posts=rows,
                )
                if _backfill_rows_signature(before_rows) != _backfill_rows_signature(
                    rows
                ):
                    mark_stock_dirty(
                        conn,
                        stock_key=stock_key,
                        reason="backfill_cache",
                    )

            new_cursor = stock_keys[-1]
            save_worker_job_cursor(
                engine_or_conn,
                state_key=BACKFILL_CACHE_CURSOR_KEY,
                cursor=new_cursor,
            )
            has_more = _has_more_stock_keys(conn, after_stock_key=new_cursor)
            return {
                "processed": int(len(stock_keys)),
                "written": int(total_written),
                "has_more": bool(has_more),
            }
    finally:
        release_worker_job_lock(engine_or_conn, lock_key=BACKFILL_CACHE_LOCK_KEY)


__all__ = ["sync_stock_backfill_cache"]
