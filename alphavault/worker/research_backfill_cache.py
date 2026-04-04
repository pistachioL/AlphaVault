from __future__ import annotations

import hashlib
import json
import re
import time
from typing import Any, Callable

import pandas as pd

from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    turso_connect_autocommit,
)
from alphavault.domains.thread_tree.service import build_post_tree_map
from alphavault.research_backfill_cache import (
    list_stock_backfill_dirty_keys,
    load_stock_backfill_meta,
    remove_stock_backfill_dirty_keys,
    replace_stock_backfill_posts,
    save_stock_backfill_meta,
)
from alphavault.research_stock_cache import mark_stock_dirty
from alphavault.worker.job_state import (
    release_worker_job_lock,
    try_acquire_worker_job_lock,
)


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
SELECT post_uid, author, created_at, url
FROM posts
WHERE processed_at IS NOT NULL
{text_clause}
{cursor_clause}
ORDER BY created_at DESC, post_uid DESC
LIMIT :limit
"""
    rows = conn.execute(sql, params).mappings().all()
    return [dict(row) for row in rows if row]


def _select_posts_text(
    conn: TursoConnection,
    *,
    post_uids: list[str],
) -> dict[str, dict[str, str]]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in post_uids:
        uid = str(raw or "").strip()
        if not uid or uid in seen:
            continue
        seen.add(uid)
        cleaned.append(uid)
    if not cleaned:
        return {}

    params: dict[str, Any] = {}
    placeholders: list[str] = []
    for idx, uid in enumerate(cleaned):
        key = f"uid_{idx}"
        params[key] = uid
        placeholders.append(f":{key}")
    clause = ", ".join(placeholders)
    sql = f"""
SELECT post_uid, raw_text, display_md
FROM posts
WHERE post_uid IN ({clause})
"""
    rows = conn.execute(sql, params).mappings().all()
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        if not row:
            continue
        post_uid = str(row.get("post_uid") or "").strip()
        if not post_uid:
            continue
        out[post_uid] = {
            "raw_text": str(row.get("raw_text") or "").strip(),
            "display_md": str(row.get("display_md") or "").strip(),
        }
    return out


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
FROM assertions a
JOIN assertion_entities ae
  ON ae.post_uid = a.post_uid AND ae.assertion_idx = a.idx
WHERE ae.entity_key = :stock_key
  AND ae.entity_type = 'stock'
  AND a.action LIKE 'trade.%'
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


def _attach_tree_text(
    rows: list[dict[str, str]],
    *,
    posts_rows: list[dict[str, str]],
) -> None:
    if not rows:
        return
    if not posts_rows:
        for row in rows:
            row["tree_text"] = ""
        return
    posts = pd.DataFrame(posts_rows)
    post_uids = [
        str(row.get("post_uid") or "").strip()
        for row in rows
        if str(row.get("post_uid") or "").strip()
    ]
    tree_map = build_post_tree_map(post_uids=post_uids, posts=posts)
    for row in rows:
        post_uid = str(row.get("post_uid") or "").strip()
        if not post_uid:
            continue
        _label, tree_text = tree_map.get(post_uid, ("", ""))
        row["tree_text"] = str(tree_text or "").strip()


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
    selected_meta: dict[str, dict[str, str]] = {}
    selected_uids: list[str] = []
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
        if (
            next_cursor_created_at == cursor_created_at
            and next_cursor_post_uid == cursor_post_uid
        ):
            scan_exhausted = True
            break
        cursor_created_at = next_cursor_created_at
        cursor_post_uid = next_cursor_post_uid

        for row in rows:
            if len(selected_uids) >= max_items:
                break
            post_uid = str(row.get("post_uid") or "").strip()
            if not post_uid:
                continue
            if post_uid in asserted_post_uids:
                continue
            if post_uid in selected_meta:
                continue
            selected_uids.append(post_uid)
            selected_meta[post_uid] = {
                "post_uid": post_uid,
                "author": str(row.get("author") or "").strip(),
                "created_at": str(row.get("created_at") or "").strip(),
                "url": str(row.get("url") or "").strip(),
            }
    scan_truncated = bool(
        len(selected_uids) < max_items
        and not scan_exhausted
        and scan_batches >= max_batches
    )
    post_text = _select_posts_text(conn, post_uids=selected_uids)
    posts_rows: list[dict[str, str]] = []
    for post_uid in selected_uids:
        meta = selected_meta.get(post_uid) or {}
        raw_text = str((post_text.get(post_uid) or {}).get("raw_text") or "").strip()
        display_md = str(
            (post_text.get(post_uid) or {}).get("display_md") or ""
        ).strip()
        combined_lower = f"{raw_text}\n{display_md}".lower()
        matched_terms = [
            term
            for term, term_lower in zip(terms, terms_lower, strict=False)
            if term_lower and term_lower in combined_lower
        ]
        haystack = (raw_text or display_md).strip()
        out.append(
            {
                "post_uid": post_uid,
                "author": str(meta.get("author") or "").strip(),
                "created_at": str(meta.get("created_at") or "").strip(),
                "url": str(meta.get("url") or "").strip(),
                "matched_terms": ", ".join(matched_terms[:3]),
                "preview": _preview_text(haystack),
            }
        )
        posts_rows.append(
            {
                "post_uid": post_uid,
                "author": str(meta.get("author") or "").strip(),
                "created_at": str(meta.get("created_at") or "").strip(),
                "url": str(meta.get("url") or "").strip(),
                "raw_text": raw_text,
                "display_md": display_md,
            }
        )
    _attach_tree_text(out, posts_rows=posts_rows)
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
            str(row.get("tree_text") or "").strip(),
        )
        for row in rows
        if str(row.get("post_uid") or "").strip()
    ]
    return sorted(normalized, key=lambda item: (item[2], item[0]), reverse=True)


def _signature_digest(rows: list[dict[str, Any]]) -> tuple[str, int]:
    signature_rows = _backfill_rows_signature(rows)
    if not signature_rows:
        return "", 0
    payload = json.dumps(signature_rows, ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
    return digest, len(signature_rows)


def _has_more_dirty_stock_keys(conn: TursoConnection) -> bool:
    return bool(list_stock_backfill_dirty_keys(conn, limit=1))


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
        with (
            turso_connect_autocommit(engine_or_conn)
            if isinstance(engine_or_conn, TursoEngine)
            else engine_or_conn
        ) as conn:
            stock_keys = list_stock_backfill_dirty_keys(
                conn,
                limit=max(1, int(max_stocks_per_run)),
            )
            if not stock_keys:
                _log_backfill_sync(verbose=verbose, message="dirty_keys_empty")
                return {"processed": 0, "written": 0, "has_more": False}

            can_continue = should_continue or (lambda: True)
            processed_count = 0
            total_written = 0
            total_changed = 0
            removable_keys: list[str] = []

            for stock_key in stock_keys:
                rows, scan_truncated = _build_backfill_candidates_for_stock(
                    conn,
                    stock_key=stock_key,
                    max_rows=int(max_rows_per_stock),
                    post_batch_size=int(post_batch_size),
                    max_scan_batches=int(BACKFILL_CACHE_MAX_SCAN_BATCHES_PER_STOCK),
                )
                candidate_signature, candidate_count = _signature_digest(rows)
                meta = load_stock_backfill_meta(conn, stock_key=stock_key)
                before_signature = str(meta.get("signature") or "").strip()
                raw_before_count = str(meta.get("row_count") or "").strip()
                try:
                    before_count = max(0, int(raw_before_count or "0"))
                except ValueError:
                    before_count = 0

                keep_existing = bool(scan_truncated and before_count > 0)
                skip_write = False
                changed = False
                replaced_rows = 0
                if keep_existing:
                    skip_write = True
                    _log_backfill_sync(
                        verbose=verbose,
                        message=(
                            f"scan_truncated_keep_existing stock_key={stock_key} "
                            f"before={int(before_count)} candidates={int(candidate_count)}"
                        ),
                    )
                else:
                    if (
                        before_signature == candidate_signature
                        and before_count == candidate_count
                    ):
                        skip_write = True
                    else:
                        replaced_rows = replace_stock_backfill_posts(
                            conn,
                            stock_key=stock_key,
                            posts=rows,
                        )
                        save_stock_backfill_meta(
                            conn,
                            stock_key=stock_key,
                            signature=candidate_signature,
                            row_count=int(candidate_count),
                        )
                        total_written += int(replaced_rows)
                        total_changed += 1
                        changed = True

                if changed:
                    mark_stock_dirty(conn, stock_key=stock_key, reason="backfill_cache")

                _log_backfill_sync(
                    verbose=verbose,
                    message=(
                        f"stock_done stock_key={stock_key} "
                        f"before={int(before_count)} "
                        f"candidates={int(candidate_count)} "
                        f"truncated={1 if scan_truncated else 0} "
                        f"changed={1 if changed else 0} "
                        f"skip_write={1 if skip_write else 0} "
                        f"written={int(replaced_rows)}"
                    ),
                )
                if not keep_existing:
                    removable_keys.append(stock_key)
                processed_count += 1
                try:
                    continue_now = bool(can_continue())
                except Exception:
                    continue_now = False
                if not continue_now:
                    remove_stock_backfill_dirty_keys(conn, stock_keys=removable_keys)
                    remaining_in_batch = bool(processed_count < len(stock_keys))
                    has_more = remaining_in_batch or _has_more_dirty_stock_keys(conn)
                    _log_backfill_sync(
                        verbose=verbose,
                        message=(
                            f"yield_to_rss processed={int(processed_count)} "
                            f"changed={int(total_changed)} "
                            f"written={int(total_written)} "
                            f"has_more={1 if has_more else 0}"
                        ),
                    )
                    return {
                        "processed": int(processed_count),
                        "changed": int(total_changed),
                        "written": int(total_written),
                        "has_more": bool(has_more),
                    }

            remove_stock_backfill_dirty_keys(conn, stock_keys=removable_keys)
            has_more = _has_more_dirty_stock_keys(conn)
            _log_backfill_sync(
                verbose=verbose,
                message=(
                    f"done processed={int(processed_count)} "
                    f"changed={int(total_changed)} "
                    f"written={int(total_written)} "
                    f"has_more={1 if has_more else 0}"
                ),
            )
            return {
                "processed": int(processed_count),
                "changed": int(total_changed),
                "written": int(total_written),
                "has_more": bool(has_more),
            }
    finally:
        release_worker_job_lock(engine_or_conn, lock_key=BACKFILL_CACHE_LOCK_KEY)


__all__ = ["sync_stock_backfill_cache"]
