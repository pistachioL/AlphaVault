from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.postgres_db import (
    PostgresConnection,
    qualify_postgres_table,
    require_postgres_schema_name,
)
from alphavault.db.sql_rows import read_sql_rows
from alphavault.domains.stock.keys import (
    normalize_stock_key as _normalize_stock_key,
    stock_key_lookup_candidates,
)
from alphavault.research_signal_view import (
    coerce_signal_timestamp,
    default_signal_reference_time,
    format_signal_created_at_line,
    merge_post_fields,
)
from alphavault.domains.thread_tree.service import build_post_tree_map
from alphavault.research_workbench import RESEARCH_RELATIONS_TABLE


WANTED_ASSERTION_COLUMNS = [
    "assertion_id",
    "post_uid",
    "action",
    "action_strength",
    "summary",
    "created_at",
    "resolved_entity_key",
]

WANTED_POST_COLUMNS = [
    "post_uid",
    "platform_post_id",
    "author",
    "created_at",
    "url",
    "raw_text",
]

STOCK_ALIAS_RELATIONS_SQL = f"""
SELECT relation_type, left_key, right_key, relation_label, source, updated_at
FROM {RESEARCH_RELATIONS_TABLE}
WHERE relation_type = 'stock_alias' OR relation_label = 'alias_of'
"""


def normalize_stock_key(value: str) -> str:
    return _normalize_stock_key(value)


def _window_cutoff_str(days: int) -> str:
    window_days = max(1, int(days))
    cutoff = datetime.now(tz=UTC) - timedelta(days=window_days)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


def _source_table(conn: object, table_name: str) -> str:
    if not isinstance(conn, PostgresConnection):
        return str(table_name)
    return qualify_postgres_table(
        require_postgres_schema_name(conn),
        table_name,
    )


def _load_stock_alias_relations(
    conn: PostgresConnection,
) -> list[dict[str, object]]:
    try:
        return read_sql_rows(conn, STOCK_ALIAS_RELATIONS_SQL)
    except BaseException:
        return []


def _alias_keys_for_stock(
    relations: list[dict[str, object]],
    *,
    stock_key: str,
) -> list[str]:
    if not relations:
        return []
    left = str(stock_key or "").strip()
    if not left:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for row in relations:
        relation_type = str(row.get("relation_type") or "").strip()
        relation_label = str(row.get("relation_label") or "").strip()
        if relation_type != "stock_alias" and relation_label != "alias_of":
            continue
        left_key = str(row.get("left_key") or "").strip()
        right_key = str(row.get("right_key") or "").strip()
        if left_key != left or not right_key or right_key in seen:
            continue
        seen.add(right_key)
        out.append(right_key)
    return out


def _load_stock_assertions(
    conn: PostgresConnection,
    *,
    stock_key: str,
    stock_relations: list[dict[str, object]],
    window_days: int,
    max_rows: int,
) -> list[dict[str, object]]:
    posts_table = _source_table(conn, "posts")
    assertions_table = _source_table(conn, "assertions")
    assertion_entities_table = _source_table(conn, "assertion_entities")
    params: dict[str, Any] = {
        "stock_key": str(stock_key or "").strip(),
        "cutoff": _window_cutoff_str(window_days),
        "limit": max(1, int(max_rows)),
    }

    keys = stock_key_lookup_candidates(stock_key) + _alias_keys_for_stock(
        stock_relations,
        stock_key=stock_key,
    )
    keys = [key for key in keys if key]
    key_clause = "ae.entity_key = :stock_key"
    if len(keys) > 1:
        key_values = keys[1:]
        key_placeholders = make_in_placeholders(prefix="k", count=len(key_values))
        params.update(make_in_params(prefix="k", values=key_values))
        key_clause = (
            f"(ae.entity_key = :stock_key OR ae.entity_key IN ({key_placeholders}))"
        )

    select_expr = ", ".join(
        [
            "a.assertion_id",
            "a.post_uid",
            "a.action",
            "a.action_strength",
            "a.summary",
            "p.created_at AS created_at",
            "ae.entity_key AS resolved_entity_key",
        ]
    )
    query = f"""
SELECT {select_expr}
FROM {assertions_table} a
JOIN {posts_table} p
  ON p.post_uid = a.post_uid
JOIN {assertion_entities_table} ae
  ON ae.assertion_id = a.assertion_id
WHERE a.action LIKE 'trade.%'
  AND ae.entity_type = 'stock'
  AND {key_clause}
  AND p.created_at >= :cutoff
ORDER BY p.created_at DESC
LIMIT :limit
"""

    assertions = read_sql_rows(conn, query, params=params)
    if not assertions:
        return assertions
    sector_keys_by_assertion_id = _load_sector_keys_by_assertion_id(
        conn,
        assertion_ids=[
            str(row.get("assertion_id") or "").strip()
            for row in assertions
            if str(row.get("assertion_id") or "").strip()
        ],
    )
    out: list[dict[str, object]] = []
    for raw_row in assertions:
        row = dict(raw_row)
        for col in [
            "assertion_id",
            "post_uid",
            "summary",
            "action",
            "resolved_entity_key",
        ]:
            if col in row:
                row[col] = str(row.get(col) or "").strip()
        assertion_id = str(row.get("assertion_id") or "").strip()
        row["sector_keys"] = list(sector_keys_by_assertion_id.get(assertion_id, []))
        out.append(row)
    return out


def _load_sector_keys_by_assertion_id(
    conn: PostgresConnection,
    *,
    assertion_ids: list[str],
) -> dict[str, list[str]]:
    assertion_entities_table = _source_table(conn, "assertion_entities")
    topic_cluster_topics_table = _source_table(conn, "topic_cluster_topics")
    cleaned = [
        str(item or "").strip() for item in assertion_ids if str(item or "").strip()
    ]
    if not cleaned:
        return {}
    placeholders = make_in_placeholders(prefix="a", count=len(cleaned))
    params = make_in_params(prefix="a", values=cleaned)
    query = f"""
SELECT ae.assertion_id, tct.cluster_key
FROM {assertion_entities_table} ae
JOIN {topic_cluster_topics_table} tct
  ON tct.topic_key = ae.entity_key
WHERE ae.entity_type = 'industry'
  AND ae.assertion_id IN ({placeholders})
"""
    try:
        rows = conn.execute(query, params).mappings().all()
    except BaseException:
        return {}
    out: dict[str, list[str]] = {}
    for row in rows:
        assertion_id = str(row.get("assertion_id") or "").strip()
        cluster_key = str(row.get("cluster_key") or "").strip()
        if not assertion_id or not cluster_key:
            continue
        bucket = out.setdefault(assertion_id, [])
        if cluster_key not in bucket:
            bucket.append(cluster_key)
    return out


def _load_posts_for_assertions(
    conn: PostgresConnection,
    *,
    post_uids: list[str],
) -> list[dict[str, object]]:
    posts_table = _source_table(conn, "posts")
    cleaned = [str(uid or "").strip() for uid in post_uids if str(uid or "").strip()]
    if not cleaned:
        return []
    placeholders = ", ".join(["?"] * len(cleaned))
    sql = f"""
SELECT {", ".join(WANTED_POST_COLUMNS)}
FROM {posts_table}
WHERE processed_at IS NOT NULL
  AND post_uid IN ({placeholders})
"""
    posts = read_sql_rows(conn, sql, params=cleaned)
    out: list[dict[str, object]] = []
    for raw_row in posts:
        row = dict(raw_row)
        for col in ["post_uid", "author", "url", "raw_text"]:
            row[col] = str(row.get(col) or "").strip()
        out.append(row)
    return out


def _format_signal_timestamp(value: object) -> str:
    ts = coerce_signal_timestamp(value)
    if ts is None:
        return ""
    return ts.strftime("%Y-%m-%d %H:%M")


def _sort_rows_by_created_at(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    def _sort_key(row: dict[str, object]) -> tuple[int, float]:
        ts = coerce_signal_timestamp(row.get("created_at"))
        if ts is None:
            return (1, 0.0)
        return (0, -ts.timestamp())

    return [dict(row) for row in sorted(rows, key=_sort_key)]


def _build_related_sectors(rows: list[dict[str, object]]) -> list[dict[str, str]]:
    counts: dict[str, int] = {}
    for row in rows:
        item = row.get("sector_keys")
        if not isinstance(item, list):
            continue
        for raw in item:
            key = str(raw or "").strip()
            if not key:
                continue
            counts[key] = int(counts.get(key, 0)) + 1
    ranked = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return [
        {
            "entity_key": f"cluster:{sector_key}",
            "entity_type": "sector",
            "mention_count": str(count),
        }
        for sector_key, count in ranked
    ]


def build_stock_hot_payload(
    conn: PostgresConnection,
    *,
    stock_key: str,
    signal_window_days: int,
    signal_cap: int,
) -> dict[str, object]:
    normalized_key = normalize_stock_key(stock_key)
    if not normalized_key:
        return {
            "entity_key": "",
            "entity_type": "",
            "header": {},
            "signal_top": [],
            "related": [],
            "counters": {"signal_total": 0},
        }
    stock_relations = _load_stock_alias_relations(conn)
    assertions = _load_stock_assertions(
        conn,
        stock_key=normalized_key,
        stock_relations=stock_relations,
        window_days=signal_window_days,
        max_rows=max(1, int(signal_cap) * 4),
    )
    if not assertions:
        stock_value = normalized_key.removeprefix("stock:")
        return {
            "entity_key": normalized_key,
            "entity_type": "stock",
            "header": {"title": stock_value},
            "signal_top": [],
            "related": [],
            "counters": {"signal_total": 0},
        }
    rows = _sort_rows_by_created_at(assertions)
    entity_key = normalized_key
    total = int(len(rows))
    rows = rows[: max(1, int(signal_cap))]
    post_uids = [
        str(uid or "").strip()
        for uid in [row.get("post_uid") for row in rows]
        if str(uid or "").strip()
    ]
    posts = _load_posts_for_assertions(conn, post_uids=post_uids)
    rows = merge_post_fields(rows, posts)
    tree_map = build_post_tree_map(post_uids=post_uids, posts=posts)
    signals: list[dict[str, str]] = []
    reference_now = default_signal_reference_time()
    for row in rows:
        post_uid = str(row.get("post_uid") or "").strip()
        tree_label, tree_text = tree_map.get(post_uid, ("", ""))
        signals.append(
            {
                "post_uid": post_uid,
                "summary": str(row.get("summary") or "").strip(),
                "action": str(row.get("action") or "").strip(),
                "action_strength": str(row.get("action_strength") or "").strip(),
                "author": str(row.get("author") or "").strip(),
                "created_at": _format_signal_timestamp(row.get("created_at")),
                "created_at_line": format_signal_created_at_line(
                    row.get("created_at"),
                    now=reference_now,
                ),
                "url": str(row.get("url") or "").strip(),
                "raw_text": str(row.get("raw_text") or "").strip(),
                "tree_label": str(tree_label or "").strip(),
                "tree_text": str(tree_text or "").strip(),
            }
        )
    return {
        "entity_key": entity_key,
        "entity_type": "stock",
        "header": {"title": entity_key.removeprefix("stock:")},
        "signal_top": signals,
        "related": _build_related_sectors(rows),
        "counters": {"signal_total": total},
    }


__all__ = [
    "STOCK_ALIAS_RELATIONS_SQL",
    "WANTED_ASSERTION_COLUMNS",
    "WANTED_POST_COLUMNS",
    "build_stock_hot_payload",
    "normalize_stock_key",
]
