from __future__ import annotations

from contextlib import contextmanager
import os
import sqlite3
from typing import Callable, Iterator

import pandas as pd

from alphavault.constants import (
    DEFAULT_WORKER_STOCK_ALIAS_MAX_RETRIES,
    ENV_WORKER_STOCK_ALIAS_MAX_RETRIES,
)
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    turso_connect_autocommit,
)
from alphavault.db.turso_pandas import turso_read_sql_df
from alphavault.research_stock_cache import mark_stock_dirty
from alphavault.research_workbench import (
    ALIAS_TASK_STATUS_BLOCKED,
    ALIAS_TASK_STATUS_MANUAL,
    ALIAS_TASK_STATUS_RESOLVED,
    ensure_research_workbench_schema,
    get_alias_resolve_tasks_map,
    increment_alias_resolve_attempts,
    record_stock_alias_relation,
    set_alias_resolve_task_status,
)
from alphavault.domains.stock.key_match import is_stock_code_value
from alphavault.domains.stock.keys import (
    STOCK_KEY_PREFIX,
    normalize_stock_key,
    stock_value,
)
from alphavault.worker.local_cache import (
    CACHE_ASSERTIONS_TABLE,
    CACHE_STOCK_CODES_TABLE,
    CACHE_STOCK_NAMES_TABLE,
    TRADE_ACTION_PREFIX,
    open_local_cache,
    resolve_local_cache_db_path,
)

ALIAS_SYNC_SOURCE = "ai_worker"
ALIAS_SYNC_MAX_KEYS_PER_RUN = 8
ALIAS_SYNC_UNKNOWN_REMAINING = -1

STOCK_ALIAS_RELATIONS_SQL = """
SELECT relation_type, left_key, right_key, relation_label, source, updated_at
FROM research_relations
WHERE relation_type = 'stock_alias' OR relation_label = 'alias_of'
"""


def _log_alias_sync(*, verbose: bool, message: str) -> None:
    if not verbose:
        return
    print(f"[alias_sync] {message}", flush=True)


def _resolve_alias_max_retries() -> int:
    raw = os.getenv(ENV_WORKER_STOCK_ALIAS_MAX_RETRIES, "").strip()
    if not raw:
        return int(DEFAULT_WORKER_STOCK_ALIAS_MAX_RETRIES)
    try:
        value = int(raw)
    except ValueError:
        return int(DEFAULT_WORKER_STOCK_ALIAS_MAX_RETRIES)
    return max(1, int(value))


@contextmanager
def _use_conn(
    engine_or_conn: TursoEngine | TursoConnection,
) -> Iterator[TursoConnection]:
    if isinstance(engine_or_conn, TursoConnection):
        yield engine_or_conn
        return
    with turso_connect_autocommit(engine_or_conn) as conn:
        yield conn


def _load_stock_alias_relations(conn) -> pd.DataFrame:
    return turso_read_sql_df(conn, STOCK_ALIAS_RELATIONS_SQL)


def _existing_alias_pairs(stock_relations: pd.DataFrame) -> set[tuple[str, str]]:
    if stock_relations.empty:
        return set()
    out: set[tuple[str, str]] = set()
    for _, row in stock_relations.iterrows():
        left_key = str(row.get("left_key") or "").strip()
        right_key = str(row.get("right_key") or "").strip()
        if left_key and right_key:
            out.add((left_key, right_key))
    return out


def _candidate_alias_pairs(
    ai_alias_map: dict[str, str],
) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for alias_key, target_key in ai_alias_map.items():
        alias = str(alias_key or "").strip()
        target = str(target_key or "").strip()
        if not alias or not target or alias == target:
            continue
        if not alias.startswith("stock:") or not target.startswith("stock:"):
            continue
        pair = (target, alias)
        if pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    return pairs


def _load_distinct_trade_stock_keys(cache_conn: sqlite3.Connection) -> list[str]:
    rows = cache_conn.execute(
        f"""
SELECT DISTINCT topic_key
FROM {CACHE_ASSERTIONS_TABLE}
WHERE action LIKE ?
  AND topic_key LIKE 'stock:%'
""",
        (f"{TRADE_ACTION_PREFIX}%",),
    ).fetchall()
    keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if not row:
            continue
        key = str(row[0] or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        keys.append(key)
    keys.sort()
    return keys


def _is_unresolved_alias_key(stock_key: str) -> bool:
    key = normalize_stock_key(str(stock_key or "").strip())
    if not key.startswith(STOCK_KEY_PREFIX):
        return False
    value = stock_value(key)
    if not value or ":" in value:
        return False
    return not bool(is_stock_code_value(value))


def _pick_top_candidates(rows: list[tuple[object, object]]) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    for raw_value, raw_count in rows:
        value = str(raw_value or "").strip()
        if not value:
            continue
        try:
            count = int(str(raw_count or 0).strip() or 0)
        except Exception:
            count = 0
        if count <= 0:
            continue
        out.append((value, int(count)))
    return out


def _is_unambiguous(top: int, second: int) -> bool:
    if top <= 0:
        return False
    if second <= 0:
        return top >= 2
    return top >= max(3, int(second) * 2)


def _resolve_alias_target_from_cache(
    cache_conn: sqlite3.Connection,
    *,
    alias_key: str,
) -> str:
    alias = normalize_stock_key(str(alias_key or "").strip())
    if not alias:
        return ""
    alias_value = stock_value(alias)
    if not alias_value:
        return ""

    action_like = f"{TRADE_ACTION_PREFIX}%"
    code_rows = cache_conn.execute(
        f"""
SELECT c.stock_code, COUNT(1) AS n
FROM {CACHE_STOCK_CODES_TABLE} c
JOIN {CACHE_ASSERTIONS_TABLE} a
  ON a.post_uid = c.post_uid AND a.idx = c.idx
LEFT JOIN {CACHE_STOCK_NAMES_TABLE} hit
  ON hit.post_uid = a.post_uid AND hit.idx = a.idx AND hit.stock_name = ?
WHERE a.action LIKE ?
  AND (a.topic_key = ? OR hit.stock_name IS NOT NULL)
GROUP BY c.stock_code
ORDER BY n DESC, c.stock_code ASC
LIMIT 2
""",
        (alias_value, action_like, alias),
    ).fetchall()
    code_candidates = [
        (code, count)
        for code, count in _pick_top_candidates(code_rows)
        if is_stock_code_value(code)
    ]

    name_rows = cache_conn.execute(
        f"""
SELECT n.stock_name, COUNT(1) AS n
FROM {CACHE_STOCK_NAMES_TABLE} n
JOIN {CACHE_ASSERTIONS_TABLE} a
  ON a.post_uid = n.post_uid AND a.idx = n.idx
LEFT JOIN {CACHE_STOCK_NAMES_TABLE} hit
  ON hit.post_uid = a.post_uid AND hit.idx = a.idx AND hit.stock_name = ?
WHERE a.action LIKE ?
  AND (a.topic_key = ? OR hit.stock_name IS NOT NULL)
  AND n.stock_name <> ?
GROUP BY n.stock_name
ORDER BY n DESC, n.stock_name ASC
LIMIT 2
""",
        (alias_value, action_like, alias, alias_value),
    ).fetchall()
    name_candidates = [
        (name, count)
        for name, count in _pick_top_candidates(name_rows)
        if name and (":" not in name) and (not is_stock_code_value(name))
    ]

    if code_candidates:
        top = int(code_candidates[0][1])
        second = int(code_candidates[1][1]) if len(code_candidates) > 1 else 0
        if _is_unambiguous(top, second):
            return normalize_stock_key(f"{STOCK_KEY_PREFIX}{code_candidates[0][0]}")
    if name_candidates:
        top = int(name_candidates[0][1])
        second = int(name_candidates[1][1]) if len(name_candidates) > 1 else 0
        if _is_unambiguous(top, second):
            return normalize_stock_key(f"{STOCK_KEY_PREFIX}{name_candidates[0][0]}")
    return ""


def sync_stock_alias_relations(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    source: str = ALIAS_SYNC_SOURCE,
    source_name: str = "",
    max_alias_keys_per_run: int = ALIAS_SYNC_MAX_KEYS_PER_RUN,
    ai_max_inflight: int = 1,
    should_continue: Callable[[], bool] | None = None,
    acquire_low_priority_slot: Callable[[], bool] | None = None,
    release_low_priority_slot: Callable[[], None] | None = None,
    verbose: bool = False,
) -> dict[str, int | bool]:
    _log_alias_sync(
        verbose=verbose,
        message=(
            "start "
            f"max_alias_keys_per_run={max(0, int(max_alias_keys_per_run))} "
            f"ai_max_inflight={max(1, int(ai_max_inflight))}"
        ),
    )
    if acquire_low_priority_slot is not None:
        gate_open = False
        try:
            gate_open = bool(acquire_low_priority_slot())
        except Exception:
            gate_open = False
        if not gate_open:
            remaining_aliases = ALIAS_SYNC_UNKNOWN_REMAINING
            _log_alias_sync(
                verbose=verbose,
                message=(f"skip gate_busy=1 remaining={int(remaining_aliases)}"),
            )
            return {
                "assertions": 0,
                "resolved": 0,
                "candidates": 0,
                "inserted": 0,
                "attempted": 0,
                "eligible": 0,
                "queued": 0,
                "has_more": True,
                "remaining_aliases": int(remaining_aliases),
                "locked": True,
            }
        if release_low_priority_slot is not None:
            try:
                release_low_priority_slot()
            except Exception:
                pass
    ensure_research_workbench_schema(engine_or_conn)
    with _use_conn(engine_or_conn) as conn:
        stock_relations = _load_stock_alias_relations(conn)
    db_path = resolve_local_cache_db_path(source_name=str(source_name or ""))
    unresolved_aliases: list[str] = []
    try:
        with open_local_cache(db_path=db_path) as cache_conn:
            stock_keys = _load_distinct_trade_stock_keys(cache_conn)
            existing_pairs = _existing_alias_pairs(stock_relations)
            existing_alias_keys = {alias_key for _left, alias_key in existing_pairs}
            unresolved_aliases = [
                normalize_stock_key(key)
                for key in stock_keys
                if _is_unresolved_alias_key(key)
                and normalize_stock_key(key) not in existing_alias_keys
            ]
    except Exception as err:
        _log_alias_sync(
            verbose=verbose,
            message=f"cache_error skip=1 err={type(err).__name__}",
        )
        unresolved_aliases = []

    tasks_map = get_alias_resolve_tasks_map(engine_or_conn, unresolved_aliases)
    max_retries = _resolve_alias_max_retries()

    eligible_aliases: list[str] = []
    for alias_key in unresolved_aliases:
        task = tasks_map.get(alias_key)
        status = str(task["status"]).strip() if task else ""
        attempt_count = int(task["attempt_count"]) if task else 0
        if status in {
            ALIAS_TASK_STATUS_MANUAL,
            ALIAS_TASK_STATUS_BLOCKED,
            ALIAS_TASK_STATUS_RESOLVED,
        }:
            continue
        if attempt_count >= max_retries:
            set_alias_resolve_task_status(
                engine_or_conn,
                alias_key=alias_key,
                status=ALIAS_TASK_STATUS_MANUAL,
                attempt_count=attempt_count,
            )
            continue
        eligible_aliases.append(alias_key)

    aliases_to_process = eligible_aliases[: max(0, int(max_alias_keys_per_run))]
    _log_alias_sync(
        verbose=verbose,
        message=(
            f"selected unresolved={int(len(unresolved_aliases))} "
            f"eligible={int(len(eligible_aliases))} "
            f"queued={int(len(aliases_to_process))} "
            f"max_retries={int(max_retries)}"
        ),
    )
    attempted_aliases: list[str] = []
    alias_map: dict[str, str] = {}
    attempted_aliases = list(aliases_to_process)
    if aliases_to_process:
        try:
            with open_local_cache(db_path=db_path) as cache_conn:
                for alias_key in aliases_to_process:
                    target = _resolve_alias_target_from_cache(
                        cache_conn,
                        alias_key=alias_key,
                    )
                    if target:
                        alias_map[alias_key] = target
        except Exception as err:
            _log_alias_sync(
                verbose=verbose,
                message=f"cache_read_error skip=1 err={type(err).__name__}",
            )

    attempt_counts: dict[str, int] = {}
    if attempted_aliases:
        attempt_counts = increment_alias_resolve_attempts(
            engine_or_conn, attempted_aliases
        )

    for alias_key in attempted_aliases:
        attempts = int(attempt_counts.get(alias_key, 0) or 0)
        if alias_key in alias_map:
            set_alias_resolve_task_status(
                engine_or_conn,
                alias_key=alias_key,
                status=ALIAS_TASK_STATUS_RESOLVED,
                attempt_count=attempts,
            )
            continue
        if attempts >= int(max_retries):
            set_alias_resolve_task_status(
                engine_or_conn,
                alias_key=alias_key,
                status=ALIAS_TASK_STATUS_MANUAL,
                attempt_count=attempts,
            )

    candidate_pairs = _candidate_alias_pairs(alias_map)
    existing_pairs = _existing_alias_pairs(stock_relations)
    new_pairs = [pair for pair in candidate_pairs if pair not in existing_pairs]
    _log_alias_sync(
        verbose=verbose,
        message=(
            f"pairs candidates={int(len(candidate_pairs))} "
            f"new={int(len(new_pairs))} existing={int(len(existing_pairs))}"
        ),
    )

    inserted = 0
    if new_pairs:
        with _use_conn(engine_or_conn) as conn:
            ensure_research_workbench_schema(conn)
            for stock_key, alias_key in new_pairs:
                record_stock_alias_relation(
                    conn,
                    stock_key=stock_key,
                    alias_key=alias_key,
                    source=source,
                )
                if stock_key.startswith("stock:"):
                    mark_stock_dirty(
                        conn,
                        stock_key=stock_key,
                        reason="alias_relation",
                    )
                if alias_key.startswith("stock:"):
                    mark_stock_dirty(
                        conn,
                        stock_key=alias_key,
                        reason="alias_relation",
                    )
                inserted += 1
    else:
        _log_alias_sync(verbose=verbose, message="write_skip reason=no_new_pairs")

    attempted_count = int(len(attempted_aliases))
    remaining_aliases = max(0, int(len(eligible_aliases) - attempted_count))
    has_more = bool(len(eligible_aliases) > attempted_count)
    _log_alias_sync(
        verbose=verbose,
        message=(
            f"done inserted={int(inserted)} resolved={int(len(alias_map))} "
            f"attempted={attempted_count} remaining={remaining_aliases} "
            f"has_more={1 if has_more else 0}"
        ),
    )

    return {
        "assertions": 0,
        "resolved": int(len(alias_map)),
        "candidates": int(len(candidate_pairs)),
        "inserted": int(inserted),
        "attempted": attempted_count,
        "eligible": int(len(eligible_aliases)),
        "queued": int(len(aliases_to_process)),
        "has_more": has_more,
        "remaining_aliases": remaining_aliases,
    }


__all__ = [
    "sync_stock_alias_relations",
]
