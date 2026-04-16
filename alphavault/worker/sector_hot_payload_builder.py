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
from alphavault.research_signal_view import (
    build_related_stock_rows,
    build_signal_rows,
    merge_post_fields,
)


WANTED_ASSERTION_COLUMNS = [
    "assertion_id",
    "post_uid",
    "stock_key",
    "action",
    "action_strength",
    "summary",
    "created_at",
]

WANTED_POST_COLUMNS = [
    "post_uid",
    "platform_post_id",
    "author",
    "created_at",
    "url",
    "raw_text",
]


def normalize_sector_key(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text if text.startswith("cluster:") else f"cluster:{text}"


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


def _load_sector_assertions(
    conn: PostgresConnection,
    *,
    sector_slug: str,
    window_days: int,
    max_rows: int,
) -> list[dict[str, object]]:
    posts_table = _source_table(conn, "posts")
    assertions_table = _source_table(conn, "assertions")
    assertion_entities_table = _source_table(conn, "assertion_entities")
    topic_cluster_topics_table = _source_table(conn, "topic_cluster_topics")
    params: dict[str, Any] = {
        "cluster_key": str(sector_slug or "").strip(),
        "cutoff": _window_cutoff_str(window_days),
        "limit": max(1, int(max_rows)),
    }

    query = f"""
SELECT
  a.assertion_id,
  a.post_uid,
  stock_entities.entity_key AS stock_key,
  a.action,
  a.action_strength,
  a.summary,
  p.created_at AS created_at
FROM {assertions_table} a
JOIN {posts_table} p
  ON p.post_uid = a.post_uid
JOIN {assertion_entities_table} sector_entities
  ON sector_entities.assertion_id = a.assertion_id
JOIN {topic_cluster_topics_table} tct
  ON tct.topic_key = sector_entities.entity_key
LEFT JOIN {assertion_entities_table} stock_entities
  ON stock_entities.assertion_id = a.assertion_id
 AND stock_entities.entity_type = 'stock'
WHERE sector_entities.entity_type = 'industry'
  AND tct.cluster_key = :cluster_key
  AND p.created_at >= :cutoff
ORDER BY p.created_at DESC
LIMIT :limit
"""

    assertions = read_sql_rows(conn, query, params=params)
    if not assertions:
        return assertions
    out: list[dict[str, object]] = []
    for raw_row in assertions:
        row = dict(raw_row)
        for col in ["assertion_id", "post_uid", "stock_key", "summary", "action"]:
            row[col] = str(row.get(col) or "").strip()
        row["sector_key"] = normalize_sector_key(sector_slug)
        out.append(row)
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
    placeholders = make_in_placeholders(prefix="p", count=len(cleaned))
    params = make_in_params(prefix="p", values=cleaned)
    query = f"""
SELECT {", ".join(WANTED_POST_COLUMNS)}
FROM {posts_table}
WHERE post_uid IN ({placeholders})
"""
    posts = read_sql_rows(conn, query, params=params)
    out: list[dict[str, object]] = []
    for raw_row in posts:
        row = dict(raw_row)
        for col in WANTED_POST_COLUMNS:
            row[col] = str(row.get(col) or "").strip()
        out.append(row)
    return out


def build_sector_hot_payload(
    conn: PostgresConnection,
    *,
    sector_key: str,
    signal_cap: int,
    signal_window_days: int = 30,
) -> dict[str, object]:
    normalized_key = normalize_sector_key(sector_key)
    if not normalized_key:
        return {
            "entity_key": "",
            "entity_type": "",
            "header": {},
            "signal_top": [],
            "related": [],
            "counters": {"signal_total": 0},
        }
    sector_slug = normalized_key.removeprefix("cluster:")
    assertions = _load_sector_assertions(
        conn,
        sector_slug=sector_slug,
        window_days=int(signal_window_days),
        max_rows=max(1, int(signal_cap) * 4),
    )
    if not assertions:
        return {
            "entity_key": normalized_key,
            "entity_type": "sector",
            "header": {"title": sector_slug},
            "signal_top": [],
            "related": [],
            "counters": {"signal_total": 0},
        }

    post_uids = [
        str(uid or "").strip()
        for uid in [row.get("post_uid") for row in assertions]
        if str(uid or "").strip()
    ]
    posts = _load_posts_for_assertions(conn, post_uids=post_uids)
    view = merge_post_fields(assertions, posts)
    all_signals = build_signal_rows(view, posts=posts)
    related_rows: list[dict[str, str]] = []
    for row in build_related_stock_rows(view):
        stock_key = str(row.get("stock_key") or "").strip()
        if not stock_key:
            continue
        related_rows.append(
            {
                "entity_key": stock_key,
                "entity_type": "stock",
                "mention_count": str(row.get("mention_count") or "").strip(),
            }
        )
    return {
        "entity_key": normalized_key,
        "entity_type": "sector",
        "header": {"title": sector_slug},
        "signal_top": all_signals[: max(1, int(signal_cap))],
        "related": related_rows,
        "counters": {"signal_total": len(all_signals)},
    }


__all__ = ["build_sector_hot_payload", "normalize_sector_key"]
