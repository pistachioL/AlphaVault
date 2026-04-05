from __future__ import annotations

import bisect
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
import time
from typing import Any, Callable

from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    turso_connect_autocommit,
    turso_savepoint,
)
from alphavault.research_stock_cache import mark_entity_page_dirty
from alphavault.research_workbench import (
    RESEARCH_RELATION_CANDIDATES_TABLE,
    upsert_relation_candidate,
)
from alphavault.rss.utils import RateLimiter
from alphavault.domains.relation.ids import make_candidate_id
from alphavault.domains.stock.key_match import is_stock_code_value
from alphavault.domains.stock.keys import (
    STOCK_KEY_PREFIX,
    normalize_stock_key,
    stock_value,
)
from alphavault.infra.ai.relation_candidate_ranker import enrich_candidates_with_ai
from alphavault.worker.job_state import (
    load_worker_job_cursor,
    release_worker_job_lock,
    save_worker_job_cursor,
    try_acquire_worker_job_lock,
)
from alphavault.worker.local_cache import (
    CACHE_ASSERTIONS_TABLE,
    CACHE_STOCK_CODES_TABLE,
    CACHE_STOCK_NAMES_TABLE,
    TRADE_ACTION_PREFIX,
    open_local_cache,
    resolve_local_cache_db_path,
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


def _log_relation_cache(*, verbose: bool, message: str) -> None:
    if not verbose:
        return
    print(f"[relation_cache] {message}", flush=True)


def _finalize_candidate_rows(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                str(key): str(raw or "").strip()
                for key, raw in item.items()
                if str(key).strip()
            }
        )
    return out


def _load_distinct_trade_stock_keys(*, db_path: Path) -> list[str]:
    action_like = f"{TRADE_ACTION_PREFIX}%"
    try:
        with open_local_cache(db_path=db_path) as cache_conn:
            rows = cache_conn.execute(
                f"""
SELECT DISTINCT topic_key
FROM {CACHE_ASSERTIONS_TABLE}
WHERE action LIKE ?
  AND topic_key LIKE 'stock:%'
""",
                (action_like,),
            ).fetchall()
            keys: list[str] = []
            seen: set[str] = set()
            for row in rows:
                if not row:
                    continue
                key = normalize_stock_key(str(row[0] or "").strip())
                if not key or key in seen:
                    continue
                if not key.startswith(STOCK_KEY_PREFIX):
                    continue
                if not is_stock_code_value(stock_value(key)):
                    continue
                seen.add(key)
                keys.append(key)
            keys.sort()
            return keys
    except Exception:
        return []


def _build_stock_alias_candidates(
    *,
    db_path: Path,
    stock_key: str,
) -> list[dict[str, Any]]:
    left = normalize_stock_key(str(stock_key or "").strip())
    if not left:
        return []
    code_value = stock_value(left)
    if not is_stock_code_value(code_value):
        return []

    action_like = f"{TRADE_ACTION_PREFIX}%"
    alias_counts: Counter[str] = Counter()
    with open_local_cache(db_path=db_path) as cache_conn:
        # 1) From rows where the stock code is present (even if topic_key is alias).
        rows = cache_conn.execute(
            f"""
SELECT a.topic_key, COUNT(1) AS n
FROM {CACHE_ASSERTIONS_TABLE} a
JOIN {CACHE_STOCK_CODES_TABLE} c
  ON c.post_uid = a.post_uid AND c.idx = a.idx
WHERE a.action LIKE ?
  AND c.stock_code = ?
GROUP BY a.topic_key
ORDER BY n DESC, a.topic_key ASC
LIMIT 200
""",
            (action_like, code_value),
        ).fetchall()
        for raw_key, raw_count in rows:
            key = normalize_stock_key(str(raw_key or "").strip())
            if not key.startswith(STOCK_KEY_PREFIX):
                continue
            value = stock_value(key)
            if not value or ":" in value or is_stock_code_value(value):
                continue
            if key == left:
                continue
            try:
                count = int(raw_count or 0)
            except Exception:
                count = 0
            if count > 0:
                alias_counts[key] += int(count)

        # 2) From stock_names on rows where topic_key is the stock code.
        name_rows = cache_conn.execute(
            f"""
SELECT n.stock_name, COUNT(1) AS n
FROM {CACHE_STOCK_NAMES_TABLE} n
JOIN {CACHE_ASSERTIONS_TABLE} a
  ON a.post_uid = n.post_uid AND a.idx = n.idx
WHERE a.action LIKE ?
  AND a.topic_key = ?
GROUP BY n.stock_name
ORDER BY n DESC, n.stock_name ASC
LIMIT 200
""",
            (action_like, left),
        ).fetchall()
        for raw_name, raw_count in name_rows:
            name = str(raw_name or "").strip()
            if not name or ":" in name or is_stock_code_value(name):
                continue
            alias_key = normalize_stock_key(f"{STOCK_KEY_PREFIX}{name}")
            if not alias_key or alias_key == left:
                continue
            try:
                count = int(raw_count or 0)
            except Exception:
                count = 0
            if count > 0:
                alias_counts[alias_key] += int(count)

    if not alias_counts:
        return []

    ranked = sorted(alias_counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    out: list[dict[str, Any]] = []
    for alias_key, score in ranked[
        : int(RELATION_CANDIDATES_MAX_CANDIDATES_PER_LEFT_KEY)
    ]:
        out.append(
            {
                "relation_type": "stock_alias",
                "left_key": left,
                "right_key": alias_key,
                "relation_label": "alias_of",
                "candidate_id": make_candidate_id(
                    relation_type="stock_alias",
                    left_key=left,
                    right_key=alias_key,
                    relation_label="alias_of",
                ),
                "candidate_key": alias_key,
                "suggestion_reason": f"同票名称共现 {int(score)} 次",
                "evidence_summary": f"同票名称共现 {int(score)} 次",
                "score": str(int(score)),
            }
        )
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
    source_name: str = "",
    max_stocks_per_run: int = RELATION_CANDIDATES_MAX_STOCKS_PER_RUN,
    max_sectors_per_run: int = RELATION_CANDIDATES_MAX_SECTORS_PER_RUN,
    lock_lease_seconds: int = RELATION_CANDIDATES_LOCK_LEASE_SECONDS,
    ai_max_inflight: int = 1,
    should_continue: Callable[[], bool] | None = None,
    acquire_low_priority_slot: Callable[[], bool] | None = None,
    release_low_priority_slot: Callable[[], None] | None = None,
    verbose: bool = False,
) -> dict[str, int | bool]:
    _log_relation_cache(
        verbose=verbose,
        message=(
            "start "
            f"ai_enabled={1 if ai_enabled else 0} "
            f"ai_max_inflight={max(1, int(ai_max_inflight))} "
            f"max_stocks_per_run={max(0, int(max_stocks_per_run))} "
            f"max_sectors_per_run={max(0, int(max_sectors_per_run))}"
        ),
    )
    now_epoch = int(time.time())
    if not try_acquire_worker_job_lock(
        engine_or_conn,
        lock_key=RELATION_CANDIDATES_LOCK_KEY,
        now_epoch=now_epoch,
        lease_seconds=int(lock_lease_seconds),
    ):
        _log_relation_cache(verbose=verbose, message="lock_busy skip=1")
        return {
            "processed": 0,
            "upserted": 0,
            "deleted": 0,
            "has_more": False,
            "locked": True,
        }
    try:
        stock_cursor = load_worker_job_cursor(
            engine_or_conn, state_key=RELATION_CANDIDATES_STOCK_CURSOR_KEY
        )
        sector_cursor = load_worker_job_cursor(
            engine_or_conn, state_key=RELATION_CANDIDATES_SECTOR_CURSOR_KEY
        )
        db_path = resolve_local_cache_db_path(source_name=str(source_name or ""))
        stock_keys = _load_distinct_trade_stock_keys(db_path=db_path)
        sector_keys: list[str] = []
        if not stock_keys and not sector_keys:
            _log_relation_cache(verbose=verbose, message="cache_empty skip=1")
            return {"processed": 0, "upserted": 0, "deleted": 0, "has_more": False}

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
        _log_relation_cache(
            verbose=verbose,
            message=(
                f"keys_total stocks={int(len(stock_keys))} sectors={int(len(sector_keys))} "
                f"picked_stocks={int(len(stocks_to_process))} picked_sectors={int(len(sectors_to_process))}"
            ),
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
            build_candidates=lambda stock_key: _finalize_candidate_rows(
                enrich_candidates_with_ai(
                    _build_stock_alias_candidates(
                        db_path=db_path,
                        stock_key=stock_key,
                    ),
                    relation_type="stock_alias",
                    ai_enabled=bool(ai_enabled),
                    should_continue=should_continue,
                )
            ),
        )
        with (
            turso_connect_autocommit(engine_or_conn)
            if isinstance(engine_or_conn, TursoEngine)
            else engine_or_conn
        ) as conn:
            for stock_key, candidates in stock_results:
                left_key_for_stock = stock_key
                changed_for_stock = False
                batch_upserted = 0
                batch_deleted = 0
                with turso_savepoint(conn):
                    batch_upserted, rel_types, keep_ids, left_key = (
                        _upsert_candidate_batch(conn, candidates=candidates)
                    )
                    left_key_for_stock = left_key or stock_key
                    if not rel_types:
                        rel_types = ["stock_alias"]
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
                    mark_entity_page_dirty(
                        conn,
                        stock_key=left_key_for_stock,
                        reason="relation_candidates_cache",
                    )
                processed += 1
                _log_relation_cache(
                    verbose=verbose,
                    message=(
                        f"stock_done left_key={left_key_for_stock} "
                        f"candidates={int(len(candidates))} "
                        f"upserted={int(batch_upserted)} "
                        f"deleted={int(batch_deleted)}"
                    ),
                )
                save_worker_job_cursor(
                    engine_or_conn,
                    state_key=RELATION_CANDIDATES_STOCK_CURSOR_KEY,
                    cursor=stock_key,
                )
        if stocks_stopped_early:
            stocks_has_more = True
            _log_relation_cache(
                verbose=verbose,
                message="stocks_stopped_early=1",
            )

        if sectors_to_process:
            _log_relation_cache(verbose=verbose, message="sectors_skip reason=no_data")

        has_more = bool(stocks_has_more or sectors_has_more)
        _log_relation_cache(
            verbose=verbose,
            message=(
                f"done processed={int(processed)} "
                f"upserted={int(upserted)} "
                f"deleted={int(deleted)} "
                f"has_more={1 if has_more else 0}"
            ),
        )
        return {
            "processed": int(processed),
            "upserted": int(upserted),
            "deleted": int(deleted),
            "has_more": bool(has_more),
        }
    finally:
        release_worker_job_lock(engine_or_conn, lock_key=RELATION_CANDIDATES_LOCK_KEY)


__all__ = ["sync_relation_candidates_cache"]
