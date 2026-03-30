from __future__ import annotations

import re
import time
from typing import Any, Callable

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
BACKFILL_CACHE_MAX_SCAN_BATCHES_PER_STOCK = 20

_WS_RE = re.compile(r"\s+")
_STOCK_KEY_PREFIX = "stock:"


def _log_backfill_sync(*, verbose: bool, message: str) -> None:
    if not verbose:
        return
    print(f"[backfill_cache] {message}", flush=True)


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


def _escape_like_term(term: str) -> str:
    return str(term or "").replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _build_text_match_clause(terms_lower: list[str]) -> tuple[str, dict[str, str]]:
    clauses: list[str] = []
    params: dict[str, str] = {}
    for idx, term in enumerate(terms_lower):
        text = str(term or "").strip()
        if not text:
            continue
        key = f"term_{idx}"
        params[key] = f"%{_escape_like_term(text)}%"
        clauses.append(
            f"(LOWER(COALESCE(raw_text, '')) LIKE :{key} ESCAPE '\\' "
            f"OR LOWER(COALESCE(display_md, '')) LIKE :{key} ESCAPE '\\')"
        )
    if not clauses:
        return "", {}
    return f"\n  AND ({' OR '.join(clauses)})", params


def _select_post_batch(
    conn: TursoConnection,
    *,
    cursor_created_at: str,
    cursor_post_uid: str,
    limit: int,
    terms_lower: list[str],
) -> list[dict[str, Any]]:
    cursor_clause = ""
    text_clause, text_params = _build_text_match_clause(terms_lower)
    params: dict[str, Any] = {
        "limit": max(1, int(limit)),
        **text_params,
    }
    if str(cursor_created_at or "").strip() and str(cursor_post_uid or "").strip():
        cursor_clause = """
  AND (created_at < :cursor_created_at OR (created_at = :cursor_created_at AND post_uid < :cursor_post_uid))
"""
        params["cursor_created_at"] = str(cursor_created_at or "").strip()
        params["cursor_post_uid"] = str(cursor_post_uid or "").strip()

    sql = f"""
SELECT post_uid, author, created_at, url, raw_text, display_md
FROM posts
WHERE processed_at IS NOT NULL
{text_clause}
{cursor_clause}
ORDER BY created_at DESC, post_uid DESC
LIMIT :limit
"""
    rows = conn.execute(sql, params).mappings().all()
    return [dict(row) for row in rows if row]


def _select_asserted_post_uids(
    conn: TursoConnection,
    *,
    stock_key: str,
) -> set[str]:
    key = str(stock_key or "").strip()
    if not key:
        return set()
    rows = conn.execute(
        """
SELECT DISTINCT post_uid
FROM assertions
WHERE topic_key = :stock_key
  AND action LIKE 'trade.%'
""",
        {"stock_key": key},
    ).fetchall()
    out: set[str] = set()
    for row in rows:
        if not row:
            continue
        post_uid = str(row[0] or "").strip()
        if post_uid:
            out.add(post_uid)
    return out


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
    max_scan_batches: int,
) -> tuple[list[dict[str, str]], bool]:
    terms = _stock_terms(stock_key)
    if not terms:
        return [], False
    terms_lower = [term.lower() for term in terms]
    asserted_post_uids = _select_asserted_post_uids(conn, stock_key=stock_key)

    out: list[dict[str, str]] = []
    cursor_created_at = ""
    cursor_post_uid = ""
    max_items = max(1, int(max_rows))
    batch_size = max(1, int(post_batch_size))
    scan_batches = 0
    max_batches = max(1, int(max_scan_batches))
    scan_exhausted = False
    while len(out) < max_items and scan_batches < max_batches:
        rows = _select_post_batch(
            conn,
            cursor_created_at=cursor_created_at,
            cursor_post_uid=cursor_post_uid,
            limit=batch_size,
            terms_lower=terms_lower,
        )
        scan_batches += 1
        if not rows:
            scan_exhausted = True
            break
        last = rows[-1]
        next_cursor_created_at = (
            str(last.get("created_at") or "").strip() or cursor_created_at
        )
        next_cursor_post_uid = (
            str(last.get("post_uid") or "").strip() or cursor_post_uid
        )
        # Prevent cursor stall (same page fetched repeatedly).
        if (
            next_cursor_created_at == cursor_created_at
            and next_cursor_post_uid == cursor_post_uid
        ):
            scan_exhausted = True
            break
        cursor_created_at = next_cursor_created_at
        cursor_post_uid = next_cursor_post_uid

        for row in rows:
            if len(out) >= max_items:
                break
            post_uid = str(row.get("post_uid") or "").strip()
            if not post_uid:
                continue
            if post_uid in asserted_post_uids:
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
    scan_truncated = bool(
        len(out) < max_items and not scan_exhausted and scan_batches >= max_batches
    )
    return out, scan_truncated


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
    should_continue: Callable[[], bool] | None = None,
    verbose: bool = False,
) -> dict[str, int | bool]:
    _log_backfill_sync(
        verbose=verbose,
        message=(
            "start "
            f"max_stocks_per_run={max(1, int(max_stocks_per_run))} "
            f"max_rows_per_stock={max(1, int(max_rows_per_stock))} "
            f"post_batch_size={max(1, int(post_batch_size))}"
        ),
    )
    now_epoch = int(time.time())
    if not try_acquire_worker_job_lock(
        engine_or_conn,
        lock_key=BACKFILL_CACHE_LOCK_KEY,
        now_epoch=now_epoch,
        lease_seconds=int(lock_lease_seconds),
    ):
        _log_backfill_sync(verbose=verbose, message="lock_busy skip=1")
        return {"processed": 0, "written": 0, "has_more": False, "locked": True}
    try:
        cursor = load_worker_job_cursor(
            engine_or_conn, state_key=BACKFILL_CACHE_CURSOR_KEY
        )
        _log_backfill_sync(
            verbose=verbose,
            message=f"cursor_loaded value={cursor or '(empty)'}",
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
                    _log_backfill_sync(
                        verbose=verbose,
                        message="stock_keys_empty cursor_reset=1",
                    )
                else:
                    _log_backfill_sync(
                        verbose=verbose,
                        message="stock_keys_empty cursor_reset=0",
                    )
                return {"processed": 0, "written": 0, "has_more": False}

            can_continue = should_continue or (lambda: True)
            total_written = 0
            processed_count = 0
            for stock_key in stock_keys:
                before_rows = list_stock_backfill_posts(
                    conn,
                    stock_key=stock_key,
                    limit=max(1, int(max_rows_per_stock)),
                )
                rows, scan_truncated = _build_backfill_candidates_for_stock(
                    conn,
                    stock_key=stock_key,
                    max_rows=int(max_rows_per_stock),
                    post_batch_size=int(post_batch_size),
                    max_scan_batches=int(BACKFILL_CACHE_MAX_SCAN_BATCHES_PER_STOCK),
                )
                keep_existing = bool(scan_truncated and before_rows)
                if keep_existing:
                    replaced_rows = 0
                    signature_after = _backfill_rows_signature(before_rows)
                    _log_backfill_sync(
                        verbose=verbose,
                        message=(
                            f"scan_truncated_keep_existing stock_key={stock_key} "
                            f"before={int(len(before_rows))} "
                            f"candidates={int(len(rows))}"
                        ),
                    )
                else:
                    replaced_rows = replace_stock_backfill_posts(
                        conn,
                        stock_key=stock_key,
                        posts=rows,
                    )
                    total_written += int(replaced_rows)
                    signature_after = _backfill_rows_signature(rows)
                if _backfill_rows_signature(before_rows) != signature_after:
                    mark_stock_dirty(conn, stock_key=stock_key, reason="backfill_cache")
                _log_backfill_sync(
                    verbose=verbose,
                    message=(
                        f"stock_done stock_key={stock_key} "
                        f"before={int(len(before_rows))} "
                        f"candidates={int(len(rows))} "
                        f"truncated={1 if scan_truncated else 0} "
                        f"written={int(replaced_rows)}"
                    ),
                )
                processed_count += 1
                try:
                    continue_now = bool(can_continue())
                except Exception:
                    continue_now = False
                if not continue_now:
                    save_worker_job_cursor(
                        engine_or_conn,
                        state_key=BACKFILL_CACHE_CURSOR_KEY,
                        cursor=stock_key,
                    )
                    remaining_in_batch = bool(processed_count < len(stock_keys))
                    has_more = remaining_in_batch or _has_more_stock_keys(
                        conn, after_stock_key=stock_key
                    )
                    _log_backfill_sync(
                        verbose=verbose,
                        message=(
                            f"yield_to_rss processed={int(processed_count)} "
                            f"written={int(total_written)} "
                            f"has_more={1 if has_more else 0} "
                            f"cursor_out={stock_key}"
                        ),
                    )
                    return {
                        "processed": int(processed_count),
                        "written": int(total_written),
                        "has_more": bool(has_more),
                    }

            new_cursor = stock_keys[-1]
            save_worker_job_cursor(
                engine_or_conn,
                state_key=BACKFILL_CACHE_CURSOR_KEY,
                cursor=new_cursor,
            )
            has_more = _has_more_stock_keys(conn, after_stock_key=new_cursor)
            _log_backfill_sync(
                verbose=verbose,
                message=(
                    f"done processed={int(processed_count)} "
                    f"written={int(total_written)} "
                    f"has_more={1 if has_more else 0} "
                    f"cursor_out={new_cursor}"
                ),
            )
            return {
                "processed": int(processed_count),
                "written": int(total_written),
                "has_more": bool(has_more),
            }
    finally:
        release_worker_job_lock(engine_or_conn, lock_key=BACKFILL_CACHE_LOCK_KEY)


__all__ = ["sync_stock_backfill_cache"]
