from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.turso_db import TursoConnection
from alphavault.db.turso_pandas import turso_read_sql_df
from alphavault.domains.common.json_list import parse_json_list
from alphavault.domains.stock.object_index import (
    build_stock_object_index,
    filter_assertions_for_stock_object,
)
from alphavault.domains.thread_tree.service import build_post_tree_map
from alphavault.research_workbench import RESEARCH_RELATIONS_TABLE


WANTED_ASSERTION_COLUMNS = [
    "post_uid",
    "topic_key",
    "action",
    "action_strength",
    "summary",
    "author",
    "created_at",
    "stock_codes_json",
    "stock_names_json",
    "cluster_keys_json",
]

WANTED_POST_COLUMNS = [
    "post_uid",
    "platform_post_id",
    "author",
    "created_at",
    "url",
    "raw_text",
    "display_md",
]

STOCK_ALIAS_RELATIONS_SQL = f"""
SELECT relation_type, left_key, right_key, relation_label, source, updated_at
FROM {RESEARCH_RELATIONS_TABLE}
WHERE relation_type = 'stock_alias' OR relation_label = 'alias_of'
"""


def normalize_stock_key(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text if text.startswith("stock:") else f"stock:{text}"


def _window_cutoff_str(days: int) -> str:
    window_days = max(1, int(days))
    cutoff = datetime.now(tz=UTC) - timedelta(days=window_days)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


def _load_stock_alias_relations(conn: TursoConnection) -> pd.DataFrame:
    try:
        return turso_read_sql_df(conn, STOCK_ALIAS_RELATIONS_SQL)
    except BaseException:
        return pd.DataFrame()


def _alias_keys_for_stock(relations: pd.DataFrame, *, stock_key: str) -> list[str]:
    if relations.empty:
        return []
    left = str(stock_key or "").strip()
    if not left:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for _, row in relations.iterrows():
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
    conn: TursoConnection,
    *,
    stock_key: str,
    stock_relations: pd.DataFrame,
    window_days: int,
    max_rows: int,
) -> pd.DataFrame:
    params: dict[str, Any] = {
        "stock_key": str(stock_key or "").strip(),
        "cutoff": _window_cutoff_str(window_days),
        "limit": max(1, int(max_rows)),
    }

    keys = [str(stock_key or "").strip()] + _alias_keys_for_stock(
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
            *(f"a.{col}" for col in WANTED_ASSERTION_COLUMNS),
            "ae.entity_key AS resolved_entity_key",
        ]
    )
    query = f"""
SELECT {select_expr}
FROM assertions a
JOIN assertion_entities ae
  ON ae.post_uid = a.post_uid AND ae.assertion_idx = a.idx
WHERE a.action LIKE 'trade.%'
  AND ae.entity_type = 'stock'
  AND {key_clause}
  AND a.created_at >= :cutoff
ORDER BY a.created_at DESC
LIMIT :limit
"""

    assertions = turso_read_sql_df(conn, query, params=params)
    if assertions.empty:
        return assertions
    out = assertions.copy()
    for col in ["post_uid", "topic_key", "summary", "author", "action"]:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str)
    if "stock_codes_json" not in out.columns:
        out["stock_codes_json"] = "[]"
    if "stock_names_json" not in out.columns:
        out["stock_names_json"] = "[]"
    out["stock_codes_json"] = out["stock_codes_json"].fillna("[]").astype(str)
    out["stock_names_json"] = out["stock_names_json"].fillna("[]").astype(str)
    out["cluster_keys"] = out["cluster_keys_json"].fillna("[]").astype(str)
    out["cluster_keys"] = out["cluster_keys"].apply(parse_json_list)
    return out


def _load_posts_for_assertions(
    conn: TursoConnection,
    *,
    post_uids: list[str],
) -> pd.DataFrame:
    cleaned = [str(uid or "").strip() for uid in post_uids if str(uid or "").strip()]
    if not cleaned:
        return pd.DataFrame()
    placeholders = ", ".join(["?"] * len(cleaned))
    sql = f"""
SELECT {", ".join(WANTED_POST_COLUMNS)}
FROM posts
WHERE processed_at IS NOT NULL
  AND post_uid IN ({placeholders})
"""
    posts = turso_read_sql_df(conn, sql, params=cleaned)
    if posts.empty:
        return posts
    out = posts.copy()
    for col in ["post_uid", "author", "url", "raw_text", "display_md"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].fillna("").astype(str)
    return out


def _merge_post_fields(assertions: pd.DataFrame, posts: pd.DataFrame) -> pd.DataFrame:
    if assertions.empty or posts.empty or "post_uid" not in assertions.columns:
        return assertions
    # Include post url so UI can show an "open original" link.
    wanted_post_cols = [
        "post_uid",
        "raw_text",
        "display_md",
        "author",
        "url",
        "created_at",
    ]
    post_cols = posts[[col for col in wanted_post_cols if col in posts.columns]].copy()
    post_cols = post_cols.rename(
        columns={
            "raw_text": "_post_raw_text",
            "display_md": "_post_display_md",
            "author": "_post_author",
            "url": "_post_url",
            "created_at": "_post_created_at",
        }
    )
    merged = assertions.merge(post_cols, on="post_uid", how="left")
    for col in ["raw_text", "display_md", "author", "url"]:
        if col not in merged.columns:
            merged[col] = ""
        merged[col] = merged[col].fillna("").astype(str)
    merged.loc[merged["raw_text"].eq(""), "raw_text"] = (
        merged.loc[merged["raw_text"].eq(""), "_post_raw_text"].fillna("").astype(str)
    )
    merged.loc[merged["display_md"].eq(""), "display_md"] = (
        merged.loc[merged["display_md"].eq(""), "_post_display_md"]
        .fillna("")
        .astype(str)
    )
    merged.loc[merged["author"].eq(""), "author"] = (
        merged.loc[merged["author"].eq(""), "_post_author"].fillna("").astype(str)
    )
    if "_post_url" in merged.columns:
        merged.loc[merged["url"].eq(""), "url"] = (
            merged.loc[merged["url"].eq(""), "_post_url"].fillna("").astype(str)
        )

    if "_post_created_at" in merged.columns:
        post_created_at = pd.to_datetime(merged["_post_created_at"], errors="coerce")
        if "created_at" not in merged.columns:
            merged["created_at"] = post_created_at
        else:
            merged["created_at"] = pd.to_datetime(merged["created_at"], errors="coerce")
            merged.loc[merged["created_at"].isna(), "created_at"] = post_created_at

    return merged.drop(
        columns=[
            "_post_raw_text",
            "_post_display_md",
            "_post_author",
            "_post_url",
            "_post_created_at",
        ],
        errors="ignore",
    )


def _coerce_timestamp(value: object) -> pd.Timestamp | None:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    if getattr(ts, "tzinfo", None) is not None:
        return ts.tz_convert(None)
    return pd.Timestamp(ts)


def _format_signal_timestamp(value: object) -> str:
    ts = _coerce_timestamp(value)
    if ts is None:
        return ""
    return ts.strftime("%Y-%m-%d %H:%M")


def _format_signal_created_at_line(value: object) -> str:
    text = _format_signal_timestamp(value)
    if not text:
        return ""
    now = pd.Timestamp.now(tz="UTC").tz_convert(None)
    ts = _coerce_timestamp(value)
    if ts is None:
        return text
    delta_seconds = max((now - ts).total_seconds(), 0.0)
    minutes = int(delta_seconds // 60)
    if minutes < 60:
        age = f"{minutes}分钟前"
    elif minutes < 24 * 60:
        age = f"{minutes // 60}小时前"
    else:
        age = f"{minutes // (24 * 60)}天前"
    return f"{text} · {age}"


def _build_related_sectors(rows: pd.DataFrame) -> list[dict[str, str]]:
    counts: dict[str, int] = {}
    for item in rows.get("cluster_keys", pd.Series(dtype=object)).tolist():
        if not isinstance(item, list):
            continue
        for raw in item:
            key = str(raw or "").strip()
            if not key:
                continue
            counts[key] = int(counts.get(key, 0)) + 1
    ranked = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return [
        {"sector_key": str(sector_key), "mention_count": str(count)}
        for sector_key, count in ranked
    ]


def build_stock_hot_payload(
    conn: TursoConnection,
    *,
    stock_key: str,
    signal_window_days: int,
    signal_cap: int,
) -> dict[str, object]:
    normalized_key = normalize_stock_key(stock_key)
    if not normalized_key:
        return {
            "entity_key": "",
            "header_title": "",
            "signals": [],
            "signal_total": 0,
            "related_sectors": [],
        }
    stock_relations = _load_stock_alias_relations(conn)
    assertions = _load_stock_assertions(
        conn,
        stock_key=normalized_key,
        stock_relations=stock_relations,
        window_days=signal_window_days,
        max_rows=max(1, int(signal_cap) * 4),
    )
    if assertions.empty:
        stock_value = normalized_key.removeprefix("stock:")
        return {
            "entity_key": normalized_key,
            "header_title": stock_value,
            "signals": [],
            "signal_total": 0,
            "related_sectors": [],
        }
    stock_index = build_stock_object_index(assertions, stock_relations=stock_relations)
    entity_key = stock_index.resolve(normalized_key) or normalized_key
    rows = filter_assertions_for_stock_object(
        assertions,
        stock_key=entity_key,
        stock_relations=stock_relations,
        stock_index=stock_index,
    )
    if rows.empty:
        return {
            "entity_key": entity_key,
            "header_title": stock_index.header_title(entity_key),
            "signals": [],
            "signal_total": 0,
            "related_sectors": [],
        }
    if "created_at" in rows.columns:
        rows["created_at"] = pd.to_datetime(rows["created_at"], errors="coerce")
        rows = rows.sort_values(by="created_at", ascending=False, na_position="last")
    total = int(len(rows.index))
    rows = rows.head(max(1, int(signal_cap))).copy()
    post_uids = [
        str(uid or "").strip()
        for uid in rows.get("post_uid", pd.Series(dtype=str)).tolist()
        if str(uid or "").strip()
    ]
    posts = _load_posts_for_assertions(conn, post_uids=post_uids)
    rows = _merge_post_fields(rows, posts)
    tree_map = build_post_tree_map(post_uids=post_uids, posts=posts)
    signals: list[dict[str, str]] = []
    for _, row in rows.iterrows():
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
                "created_at_line": _format_signal_created_at_line(
                    row.get("created_at")
                ),
                "url": str(row.get("url") or "").strip(),
                "raw_text": str(row.get("raw_text") or "").strip(),
                "display_md": str(row.get("display_md") or "").strip(),
                "tree_label": str(tree_label or "").strip(),
                "tree_text": str(tree_text or "").strip(),
            }
        )
    return {
        "entity_key": entity_key,
        "header_title": stock_index.header_title(entity_key),
        "signals": signals,
        "signal_total": total,
        "related_sectors": _build_related_sectors(rows),
    }


__all__ = [
    "STOCK_ALIAS_RELATIONS_SQL",
    "WANTED_ASSERTION_COLUMNS",
    "WANTED_POST_COLUMNS",
    "build_stock_hot_payload",
    "normalize_stock_key",
]
