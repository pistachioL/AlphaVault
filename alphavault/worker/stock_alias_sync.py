from __future__ import annotations

import json

import pandas as pd
from sqlalchemy.engine import Engine

from alphavault.db.introspect import table_columns
from alphavault.db.sql.ui import build_assertions_query
from alphavault.db.turso_db import turso_connect_autocommit
from alphavault.db.turso_pandas import turso_read_sql_df
from alphavault.research_workbench import (
    ensure_research_workbench_schema,
    record_stock_alias_relation,
)
from alphavault_reflex.services.stock_objects import (
    AiRuntimeConfig,
    build_ai_stock_alias_map,
)

ALIAS_SYNC_SOURCE = "ai_worker"
ALIAS_SYNC_MAX_KEYS_PER_RUN = 8

WANTED_ALIAS_ASSERTION_COLUMNS = [
    "post_uid",
    "topic_key",
    "action",
    "summary",
    "author",
    "created_at",
    "stock_codes_json",
    "stock_names_json",
    "cluster_keys_json",
]

STOCK_ALIAS_RELATIONS_SQL = """
SELECT relation_type, left_key, right_key, relation_label, source, updated_at
FROM research_relations
WHERE relation_type = 'stock_alias' OR relation_label = 'alias_of'
"""


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


def _load_alias_assertions(conn) -> pd.DataFrame:
    assertion_cols = table_columns(conn, "assertions")
    selected = [
        col for col in WANTED_ALIAS_ASSERTION_COLUMNS if col in set(assertion_cols)
    ]
    query = build_assertions_query(selected)
    if "action" in selected:
        query = f"{query} WHERE action LIKE 'trade.%'"
    assertions = turso_read_sql_df(conn, query)
    if assertions.empty:
        return assertions

    out = assertions.copy()
    if "cluster_keys" not in out.columns:
        if "cluster_keys_json" in out.columns:
            out["cluster_keys"] = out["cluster_keys_json"].apply(_parse_json_list)
        else:
            out["cluster_keys"] = [[] for _ in range(len(out))]
    if "created_at" in out.columns:
        out["created_at"] = pd.to_datetime(out["created_at"], errors="coerce", utc=True)
        out["created_at"] = out["created_at"].dt.tz_convert(None)
    return out


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
    engine: Engine,
    *,
    source: str = ALIAS_SYNC_SOURCE,
    ai_runtime_config: AiRuntimeConfig | None = None,
    max_alias_keys_per_run: int = ALIAS_SYNC_MAX_KEYS_PER_RUN,
) -> dict[str, int | bool]:
    ensure_research_workbench_schema(engine)
    with turso_connect_autocommit(engine) as conn:
        assertions = _load_alias_assertions(conn)
        stock_relations = _load_stock_alias_relations(conn)

    alias_stats: dict[str, int] = {}
    ai_alias_map = build_ai_stock_alias_map(
        assertions,
        stock_relations=stock_relations,
        runtime_config=ai_runtime_config,
        max_alias_keys=int(max_alias_keys_per_run),
        stats_out=alias_stats,
    )
    candidate_pairs = _candidate_alias_pairs(ai_alias_map)
    existing_pairs = _existing_alias_pairs(stock_relations)
    new_pairs = [pair for pair in candidate_pairs if pair not in existing_pairs]

    inserted = 0
    if new_pairs:
        with turso_connect_autocommit(engine) as conn:
            ensure_research_workbench_schema(conn)
            for stock_key, alias_key in new_pairs:
                record_stock_alias_relation(
                    conn,
                    stock_key=stock_key,
                    alias_key=alias_key,
                    source=source,
                )
                inserted += 1

    return {
        "assertions": int(len(assertions)),
        "resolved": int(len(ai_alias_map)),
        "candidates": int(len(candidate_pairs)),
        "inserted": int(inserted),
        "has_more": bool(int(alias_stats.get("remaining_aliases", 0)) > 0),
        "remaining_aliases": int(alias_stats.get("remaining_aliases", 0)),
    }


__all__ = [
    "sync_stock_alias_relations",
]
