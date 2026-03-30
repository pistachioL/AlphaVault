from __future__ import annotations

from contextlib import contextmanager
import json
import os
from typing import Callable, Iterator, Optional

import pandas as pd

from alphavault.constants import (
    DEFAULT_WORKER_STOCK_ALIAS_MAX_RETRIES,
    ENV_WORKER_STOCK_ALIAS_MAX_RETRIES,
)
from alphavault.db.introspect import table_columns
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
from alphavault_reflex.services.stock_objects import (
    AiRuntimeConfig,
    build_ai_stock_alias_map,
    pick_unresolved_stock_alias_keys,
)

ALIAS_SYNC_SOURCE = "ai_worker"
ALIAS_SYNC_MAX_KEYS_PER_RUN = 8
ALIAS_SYNC_UNKNOWN_REMAINING = -1

WANTED_ALIAS_ASSERTION_COLUMNS = [
    "topic_key",
    "stock_codes_json",
    "stock_names_json",
    "cluster_keys_json",
]
ALIAS_ASSERTION_MAX_ID_COLUMN = "max_id"

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


def _parse_json_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def _build_alias_assertion_where_clause(
    *, assertion_cols: set[str], last_id: int
) -> tuple[str, dict[str, object]]:
    clauses: list[str] = []
    params: dict[str, object] = {}
    if "action" in assertion_cols:
        clauses.append("action LIKE 'trade.%'")
    if "id" in assertion_cols and int(last_id) > 0:
        clauses.append("id > :last_id")
        params["last_id"] = int(last_id)
    if not clauses:
        return "", params
    return f" WHERE {' AND '.join(clauses)}", params


def _load_alias_assertions(conn, *, last_id: int = 0) -> tuple[pd.DataFrame, int]:
    assertion_cols = set(table_columns(conn, "assertions"))
    selected = [col for col in WANTED_ALIAS_ASSERTION_COLUMNS if col in assertion_cols]
    where_clause, params = _build_alias_assertion_where_clause(
        assertion_cols=assertion_cols,
        last_id=int(last_id),
    )

    new_max_id = int(last_id)
    if "id" in assertion_cols:
        max_id_df = turso_read_sql_df(
            conn,
            f"SELECT MAX(id) AS {ALIAS_ASSERTION_MAX_ID_COLUMN} FROM assertions{where_clause}",
            params=params,
        )
        if (
            not max_id_df.empty
            and ALIAS_ASSERTION_MAX_ID_COLUMN in max_id_df.columns
            and pd.notna(max_id_df.iloc[0][ALIAS_ASSERTION_MAX_ID_COLUMN])
        ):
            raw_max_id = max_id_df.iloc[0][ALIAS_ASSERTION_MAX_ID_COLUMN]
            try:
                new_max_id = max(int(last_id), int(raw_max_id))
            except (TypeError, ValueError):
                new_max_id = int(last_id)

    if not selected:
        return pd.DataFrame(), int(new_max_id)
    assertions = turso_read_sql_df(
        conn,
        f"SELECT DISTINCT {', '.join(selected)} FROM assertions{where_clause}",
        params=params,
    )
    if assertions.empty:
        return assertions, int(new_max_id)

    out = assertions.copy()
    if "cluster_keys" not in out.columns:
        if "cluster_keys_json" in out.columns:
            out["cluster_keys"] = out["cluster_keys_json"].apply(_parse_json_list)
        else:
            out["cluster_keys"] = [[] for _ in range(len(out))]
    if "created_at" in out.columns:
        out["created_at"] = pd.to_datetime(out["created_at"], errors="coerce", utc=True)
        out["created_at"] = out["created_at"].dt.tz_convert(None)
    return out, int(new_max_id)


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


def sync_stock_alias_relations(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    source: str = ALIAS_SYNC_SOURCE,
    ai_runtime_config: AiRuntimeConfig | None = None,
    max_alias_keys_per_run: int = ALIAS_SYNC_MAX_KEYS_PER_RUN,
    ai_max_inflight: int = 1,
    should_continue: Callable[[], bool] | None = None,
    acquire_low_priority_slot: Callable[[], bool] | None = None,
    release_low_priority_slot: Callable[[], None] | None = None,
    cached_assertions: Optional[pd.DataFrame] = None,
    last_assertion_id: int = 0,
    on_data_loaded: Optional[Callable[[pd.DataFrame, pd.DataFrame, int], None]] = None,
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
        new_rows, new_max_id = _load_alias_assertions(
            conn, last_id=int(last_assertion_id)
        )
        stock_relations = _load_stock_alias_relations(conn)
    if cached_assertions is not None:
        if not new_rows.empty:
            assertions = pd.concat([cached_assertions, new_rows], ignore_index=True)
        else:
            assertions = cached_assertions
    else:
        assertions = new_rows
    if on_data_loaded is not None:
        try:
            on_data_loaded(new_rows, stock_relations, new_max_id)
        except Exception:
            pass
    _log_alias_sync(
        verbose=verbose,
        message=(
            f"loaded assertions={int(len(assertions))} "
            f"new_rows={int(len(new_rows))} "
            f"stock_relations={int(len(stock_relations))}"
        ),
    )

    alias_stats: dict[str, int] = {}
    unresolved_aliases = pick_unresolved_stock_alias_keys(
        assertions, stock_relations=stock_relations
    )
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
    ai_alias_map: dict[str, str] = {}
    if aliases_to_process:
        _log_alias_sync(
            verbose=verbose,
            message=f"ai_map_start alias_keys={int(len(aliases_to_process))}",
        )
        ai_alias_map = build_ai_stock_alias_map(
            assertions,
            stock_relations=stock_relations,
            alias_keys=aliases_to_process,
            runtime_config=ai_runtime_config,
            max_alias_keys=len(aliases_to_process),
            stats_out=alias_stats,
            ai_max_inflight=max(1, int(ai_max_inflight)),
            should_continue=should_continue,
            acquire_low_priority_slot=acquire_low_priority_slot,
            release_low_priority_slot=release_low_priority_slot,
            attempted_aliases_out=attempted_aliases,
        )
        _log_alias_sync(
            verbose=verbose,
            message=(
                f"ai_map_done attempted={int(len(attempted_aliases))} "
                f"resolved={int(len(ai_alias_map))}"
            ),
        )
    else:
        _log_alias_sync(verbose=verbose, message="ai_map_skip reason=no_eligible_alias")

    attempt_counts: dict[str, int] = {}
    if attempted_aliases:
        attempt_counts = increment_alias_resolve_attempts(
            engine_or_conn, attempted_aliases
        )

    for alias_key in attempted_aliases:
        attempts = int(attempt_counts.get(alias_key, 0) or 0)
        if alias_key in ai_alias_map:
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

    candidate_pairs = _candidate_alias_pairs(ai_alias_map)
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
            f"done inserted={int(inserted)} resolved={int(len(ai_alias_map))} "
            f"attempted={attempted_count} remaining={remaining_aliases} "
            f"has_more={1 if has_more else 0}"
        ),
    )

    return {
        "assertions": int(len(assertions)),
        "resolved": int(len(ai_alias_map)),
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
