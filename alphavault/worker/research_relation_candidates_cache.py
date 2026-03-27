from __future__ import annotations

import bisect
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
import time
from typing import Any, Callable

import pandas as pd

from alphavault.db.introspect import table_columns
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql.ui import build_assertions_query
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    turso_connect_autocommit,
    turso_savepoint,
)
from alphavault.db.turso_pandas import turso_read_sql_df
from alphavault.research_stock_cache import mark_stock_dirty
from alphavault.research_workbench import (
    RESEARCH_RELATION_CANDIDATES_TABLE,
    ensure_research_workbench_schema,
    upsert_relation_candidate,
)
from alphavault.rss.utils import RateLimiter
from alphavault.ui.follow_pages_key_match import parse_json_list
from alphavault_reflex.services.research_data import (
    build_sector_pending_candidates,
    build_stock_pending_candidates,
)
from alphavault.worker.job_state import (
    load_worker_job_cursor,
    release_worker_job_lock,
    save_worker_job_cursor,
    try_acquire_worker_job_lock,
)


RELATION_CANDIDATES_LOCK_KEY = "relation_candidates_cache.lock"
RELATION_CANDIDATES_LOCK_LEASE_SECONDS = 900

RELATION_CANDIDATES_STOCK_CURSOR_KEY = "relation_candidates_cache.stock_cursor"
RELATION_CANDIDATES_SECTOR_CURSOR_KEY = "relation_candidates_cache.sector_cursor"

RELATION_CANDIDATES_MAX_STOCKS_PER_RUN = 2
RELATION_CANDIDATES_MAX_SECTORS_PER_RUN = 1
RELATION_CANDIDATES_MAX_CANDIDATES_PER_LEFT_KEY = 30

AI_RANK_SCORE_WEIGHT = 1_000_000.0
AI_RANK_SCORE_BASE = 1000


WANTED_ASSERTION_COLUMNS = [
    "post_uid",
    "topic_key",
    "action",
    "summary",
    "author",
    "created_at",
    "stock_codes_json",
    "stock_names_json",
    "cluster_keys_json",
    "cluster_key",
]


def _load_trade_assertions(conn: TursoConnection) -> pd.DataFrame:
    assertion_cols = table_columns(conn, "assertions")
    selected = [col for col in WANTED_ASSERTION_COLUMNS if col in set(assertion_cols)]
    query = build_assertions_query(selected)
    if "action" in selected:
        query = f"{query} WHERE action LIKE 'trade.%'"
    df = turso_read_sql_df(conn, query)
    if df.empty:
        return df
    out = df.copy()
    if "cluster_keys" not in out.columns:
        if "cluster_keys_json" in out.columns:
            out["cluster_keys"] = out["cluster_keys_json"].apply(parse_json_list)
        else:
            out["cluster_keys"] = [[] for _ in range(len(out))]
    return out


def _unique_stock_keys(assertions: pd.DataFrame) -> list[str]:
    if assertions.empty or "topic_key" not in assertions.columns:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for raw in assertions["topic_key"].tolist():
        key = str(raw or "").strip()
        if not key.startswith("stock:"):
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    out.sort()
    return out


def _unique_sector_keys(assertions: pd.DataFrame) -> list[str]:
    if assertions.empty:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for item in assertions.get("cluster_keys", pd.Series(dtype=object)).tolist():
        if not isinstance(item, list):
            continue
        for raw in item:
            key = str(raw or "").strip()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(key)
    out.sort()
    return out


def _slice_after_cursor(
    items: list[str], *, cursor: str, limit: int
) -> tuple[list[str], bool]:
    if not items:
        return [], False
    n = max(0, int(limit))
    if n <= 0:
        return [], False
    after = str(cursor or "").strip()
    start = bisect.bisect(items, after) if after else 0
    picked = items[start : start + n]
    has_more = bool(start + len(picked) < len(items))
    return picked, has_more


def _ranked_score(base_score: float, *, rank_index: int) -> float:
    rank = max(0, int(rank_index))
    return float(base_score) + (float(AI_RANK_SCORE_BASE) - float(rank)) * float(
        AI_RANK_SCORE_WEIGHT
    )


def _delete_stale_pending_candidates(
    conn: TursoConnection,
    *,
    left_key: str,
    relation_types: list[str],
    keep_candidate_ids: list[str],
) -> int:
    cleaned_left = str(left_key or "").strip()
    cleaned_types = [
        str(rt or "").strip() for rt in relation_types if str(rt or "").strip()
    ]
    if not cleaned_left or not cleaned_types:
        return 0
    type_placeholders = make_in_placeholders(prefix="rt", count=len(cleaned_types))
    params: dict[str, Any] = {"left_key": cleaned_left}
    params.update(make_in_params(prefix="rt", values=cleaned_types))

    if keep_candidate_ids:
        cleaned_ids = [
            str(cid or "").strip()
            for cid in keep_candidate_ids
            if str(cid or "").strip()
        ]
    else:
        cleaned_ids = []
    if cleaned_ids:
        id_placeholders = make_in_placeholders(prefix="cid", count=len(cleaned_ids))
        params.update(make_in_params(prefix="cid", values=cleaned_ids))
        sql = f"""
DELETE FROM {RESEARCH_RELATION_CANDIDATES_TABLE}
WHERE left_key = :left_key
  AND status = 'pending'
  AND relation_type IN ({type_placeholders})
  AND candidate_id NOT IN ({id_placeholders})
"""
    else:
        sql = f"""
DELETE FROM {RESEARCH_RELATION_CANDIDATES_TABLE}
WHERE left_key = :left_key
  AND status = 'pending'
  AND relation_type IN ({type_placeholders})
"""
    res = conn.execute(sql, params)
    return int(res.rowcount or 0)


def _upsert_candidate_batch(
    conn: TursoConnection,
    *,
    candidates: list[dict[str, str]],
) -> tuple[int, list[str], list[str], str]:
    if not candidates:
        return 0, [], [], ""
    candidate_ids: list[str] = []
    relation_types: list[str] = []
    left_key = ""
    upserted = 0
    for index, row in enumerate(
        candidates[: int(RELATION_CANDIDATES_MAX_CANDIDATES_PER_LEFT_KEY)]
    ):
        candidate_id = str(row.get("candidate_id") or "").strip()
        relation_type = str(row.get("relation_type") or "").strip()
        left = str(row.get("left_key") or "").strip()
        if not candidate_id or not relation_type or not left:
            continue
        right_key = str(row.get("right_key") or "").strip()
        relation_label = str(row.get("relation_label") or "").strip()
        suggestion_reason = str(row.get("suggestion_reason") or "").strip()
        evidence_summary = str(row.get("evidence_summary") or "").strip()
        ai_status = str(row.get("ai_status") or "").strip()
        try:
            base_score = float(str(row.get("score") or "0") or 0)
        except ValueError:
            base_score = 0.0
        score = _ranked_score(base_score, rank_index=index)

        upsert_relation_candidate(
            conn,
            candidate_id=candidate_id,
            relation_type=relation_type,
            left_key=left,
            right_key=right_key,
            relation_label=relation_label,
            suggestion_reason=suggestion_reason,
            evidence_summary=evidence_summary,
            score=float(score),
            ai_status=ai_status,
        )
        upserted += 1
        candidate_ids.append(candidate_id)
        relation_types.append(relation_type)
        left_key = left

    unique_types = sorted({rt for rt in relation_types if rt})
    unique_ids = sorted({cid for cid in candidate_ids if cid})
    return upserted, unique_types, unique_ids, left_key


def _collect_candidates_parallel(
    *,
    keys: list[str],
    ai_enabled: bool,
    ai_max_inflight: int,
    should_continue: Callable[[], bool] | None,
    acquire_low_priority_slot: Callable[[], bool] | None,
    release_low_priority_slot: Callable[[], None] | None,
    limiter: RateLimiter,
    build_candidates: Callable[[str], list[dict[str, str]]],
) -> tuple[list[tuple[str, list[dict[str, str]]]], bool]:
    if not keys:
        return [], False
    if not bool(ai_enabled):
        return [(key, build_candidates(key)) for key in keys], False

    can_continue = should_continue or (lambda: True)
    acquire_slot = acquire_low_priority_slot or (lambda: True)
    release_slot = release_low_priority_slot or (lambda: None)
    max_workers = min(max(1, int(ai_max_inflight)), len(keys))
    next_submit_index = 0
    stopped_early = False
    futures: dict[Future[list[dict[str, str]]], str] = {}
    results_by_key: dict[str, list[dict[str, str]]] = {}

    def _submit_more(executor: ThreadPoolExecutor) -> None:
        nonlocal next_submit_index, stopped_early
        while next_submit_index < len(keys) and len(futures) < int(max_workers):
            try:
                continue_now = bool(can_continue())
            except Exception:
                continue_now = False
            if not continue_now:
                stopped_early = True
                return
            key = str(keys[next_submit_index] or "").strip()
            if not key:
                next_submit_index += 1
                continue
            try:
                acquired = bool(acquire_slot())
            except Exception:
                acquired = False
            if not acquired:
                break
            next_submit_index += 1

            def _run_one(target_key: str) -> list[dict[str, str]]:
                try:
                    limiter.wait()
                    return build_candidates(target_key)
                finally:
                    try:
                        release_slot()
                    except Exception:
                        pass

            try:
                future = executor.submit(_run_one, key)
            except Exception:
                try:
                    release_slot()
                except Exception:
                    pass
                raise
            futures[future] = key

    with ThreadPoolExecutor(max_workers=int(max_workers)) as executor:
        _submit_more(executor)
        while futures:
            done, _pending = wait(set(futures.keys()), return_when=FIRST_COMPLETED)
            for done_future in done:
                key = futures.pop(done_future, "")
                if not key:
                    continue
                results_by_key[key] = done_future.result()
            _submit_more(executor)

    ordered_results = [
        (key, results_by_key[key])
        for key in keys
        if str(key or "").strip() in results_by_key
    ]
    if next_submit_index < len(keys):
        stopped_early = True
    return ordered_results, bool(stopped_early)


def sync_relation_candidates_cache(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    limiter: RateLimiter,
    ai_enabled: bool,
    max_stocks_per_run: int = RELATION_CANDIDATES_MAX_STOCKS_PER_RUN,
    max_sectors_per_run: int = RELATION_CANDIDATES_MAX_SECTORS_PER_RUN,
    lock_lease_seconds: int = RELATION_CANDIDATES_LOCK_LEASE_SECONDS,
    ai_max_inflight: int = 1,
    should_continue: Callable[[], bool] | None = None,
    acquire_low_priority_slot: Callable[[], bool] | None = None,
    release_low_priority_slot: Callable[[], None] | None = None,
) -> dict[str, int | bool]:
    now_epoch = int(time.time())
    if not try_acquire_worker_job_lock(
        engine_or_conn,
        lock_key=RELATION_CANDIDATES_LOCK_KEY,
        now_epoch=now_epoch,
        lease_seconds=int(lock_lease_seconds),
    ):
        return {
            "processed": 0,
            "upserted": 0,
            "deleted": 0,
            "has_more": False,
            "locked": True,
        }
    try:
        ensure_research_workbench_schema(engine_or_conn)
        stock_cursor = load_worker_job_cursor(
            engine_or_conn, state_key=RELATION_CANDIDATES_STOCK_CURSOR_KEY
        )
        sector_cursor = load_worker_job_cursor(
            engine_or_conn, state_key=RELATION_CANDIDATES_SECTOR_CURSOR_KEY
        )
        with (
            turso_connect_autocommit(engine_or_conn)
            if isinstance(engine_or_conn, TursoEngine)
            else engine_or_conn
        ) as conn:
            assertions = _load_trade_assertions(conn)
            if assertions.empty:
                return {"processed": 0, "upserted": 0, "deleted": 0, "has_more": False}

            stock_keys = _unique_stock_keys(assertions)
            sector_keys = _unique_sector_keys(assertions)

            stocks_to_process, stocks_has_more = _slice_after_cursor(
                stock_keys,
                cursor=stock_cursor,
                limit=max(0, int(max_stocks_per_run)),
            )
            sectors_to_process, sectors_has_more = _slice_after_cursor(
                sector_keys,
                cursor=sector_cursor,
                limit=max(0, int(max_sectors_per_run)),
            )

            processed = 0
            upserted = 0
            deleted = 0

            stock_results, stocks_stopped_early = _collect_candidates_parallel(
                keys=stocks_to_process,
                ai_enabled=bool(ai_enabled),
                ai_max_inflight=max(1, int(ai_max_inflight)),
                should_continue=should_continue,
                acquire_low_priority_slot=acquire_low_priority_slot,
                release_low_priority_slot=release_low_priority_slot,
                limiter=limiter,
                build_candidates=lambda stock_key: build_stock_pending_candidates(
                    assertions,
                    stock_key=stock_key,
                    ai_enabled=bool(ai_enabled),
                    should_continue=should_continue,
                ),
            )
            for stock_key, candidates in stock_results:
                left_key_for_stock = stock_key
                changed_for_stock = False
                with turso_savepoint(conn):
                    batch_upserted, rel_types, keep_ids, left_key = (
                        _upsert_candidate_batch(conn, candidates=candidates)
                    )
                    left_key_for_stock = left_key or stock_key
                    if not rel_types:
                        rel_types = ["stock_alias", "stock_sector"]
                    upserted += int(batch_upserted)
                    batch_deleted = _delete_stale_pending_candidates(
                        conn,
                        left_key=left_key_for_stock,
                        relation_types=rel_types,
                        keep_candidate_ids=keep_ids,
                    )
                    deleted += int(batch_deleted)
                    changed_for_stock = bool(
                        int(batch_upserted) > 0 or int(batch_deleted) > 0
                    )
                if changed_for_stock and left_key_for_stock.startswith("stock:"):
                    mark_stock_dirty(
                        conn,
                        stock_key=left_key_for_stock,
                        reason="relation_candidates_cache",
                    )
                processed += 1
                save_worker_job_cursor(
                    engine_or_conn,
                    state_key=RELATION_CANDIDATES_STOCK_CURSOR_KEY,
                    cursor=stock_key,
                )
            if stocks_stopped_early:
                stocks_has_more = True

            sector_results, sectors_stopped_early = _collect_candidates_parallel(
                keys=sectors_to_process,
                ai_enabled=bool(ai_enabled),
                ai_max_inflight=max(1, int(ai_max_inflight)),
                should_continue=should_continue,
                acquire_low_priority_slot=acquire_low_priority_slot,
                release_low_priority_slot=release_low_priority_slot,
                limiter=limiter,
                build_candidates=lambda sector_key: build_sector_pending_candidates(
                    assertions,
                    sector_key=sector_key,
                    ai_enabled=bool(ai_enabled),
                    should_continue=should_continue,
                ),
            )
            for sector_key, candidates in sector_results:
                left_key = f"cluster:{sector_key}" if sector_key else ""
                with turso_savepoint(conn):
                    batch_upserted, rel_types, keep_ids, left_from_rows = (
                        _upsert_candidate_batch(conn, candidates=candidates)
                    )
                    upserted += int(batch_upserted)
                    deleted += _delete_stale_pending_candidates(
                        conn,
                        left_key=left_from_rows or left_key,
                        relation_types=rel_types or ["sector_sector"],
                        keep_candidate_ids=keep_ids,
                    )
                processed += 1
                save_worker_job_cursor(
                    engine_or_conn,
                    state_key=RELATION_CANDIDATES_SECTOR_CURSOR_KEY,
                    cursor=sector_key,
                )
            if sectors_stopped_early:
                sectors_has_more = True

            has_more = bool(stocks_has_more or sectors_has_more)
            return {
                "processed": int(processed),
                "upserted": int(upserted),
                "deleted": int(deleted),
                "has_more": bool(has_more),
            }
    finally:
        release_worker_job_lock(engine_or_conn, lock_key=RELATION_CANDIDATES_LOCK_KEY)


__all__ = ["sync_relation_candidates_cache"]
