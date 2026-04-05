from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.turso_db import TursoConnection
from alphavault.db.turso_pandas import turso_read_sql_df
from alphavault.domains.common.json_list import parse_json_list
from alphavault.research_sector_view import build_sector_research_view


WANTED_ASSERTION_COLUMNS = [
    "post_uid",
    "topic_key",
    "action",
    "action_strength",
    "summary",
    "author",
    "created_at",
    "cluster_keys_json",
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


def _load_sector_assertions(
    conn: TursoConnection,
    *,
    sector_slug: str,
    window_days: int,
    max_rows: int,
) -> pd.DataFrame:
    params: dict[str, Any] = {
        "cluster_like": f'%"{str(sector_slug or "").strip()}"%',
        "cutoff": _window_cutoff_str(window_days),
        "limit": max(1, int(max_rows)),
    }

    query = f"""
SELECT {", ".join(WANTED_ASSERTION_COLUMNS)}
FROM assertions
WHERE cluster_keys_json LIKE :cluster_like
  AND created_at >= :cutoff
ORDER BY created_at DESC
LIMIT :limit
"""

    assertions = turso_read_sql_df(conn, query, params=params)
    if assertions.empty:
        return assertions
    out = assertions.copy()
    for col in ["post_uid", "topic_key", "summary", "author", "action"]:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str)
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
    placeholders = make_in_placeholders(prefix="p", count=len(cleaned))
    params = make_in_params(prefix="p", values=cleaned)
    query = f"""
SELECT {", ".join(WANTED_POST_COLUMNS)}
FROM posts
WHERE post_uid IN ({placeholders})
"""
    posts = turso_read_sql_df(conn, query, params=params)
    if posts.empty:
        return posts
    out = posts.copy()
    for col in WANTED_POST_COLUMNS:
        out[col] = out[col].fillna("").astype(str)
    return out


def build_sector_hot_payload(
    conn: TursoConnection,
    *,
    sector_key: str,
    signal_cap: int,
    signal_window_days: int = 30,
) -> dict[str, object]:
    normalized_key = normalize_sector_key(sector_key)
    if not normalized_key:
        return {
            "entity_key": "",
            "header_title": "",
            "signals": [],
            "signal_total": 0,
            "related_stocks": [],
        }
    sector_slug = normalized_key.removeprefix("cluster:")
    assertions = _load_sector_assertions(
        conn,
        sector_slug=sector_slug,
        window_days=int(signal_window_days),
        max_rows=max(1, int(signal_cap) * 4),
    )
    if assertions.empty:
        return {
            "entity_key": normalized_key,
            "header_title": sector_slug,
            "signals": [],
            "signal_total": 0,
            "related_stocks": [],
        }

    post_uids = [
        str(uid or "").strip()
        for uid in assertions.get("post_uid", pd.Series(dtype=str)).tolist()
        if str(uid or "").strip()
    ]
    posts = _load_posts_for_assertions(conn, post_uids=post_uids)
    view = build_sector_research_view(posts, assertions, sector_key=sector_slug)
    all_signals = list(view.signals)
    return {
        "entity_key": normalized_key,
        "header_title": view.header_title,
        "signals": all_signals[: max(1, int(signal_cap))],
        "signal_total": len(all_signals),
        "related_stocks": list(view.related_stocks),
    }


__all__ = ["build_sector_hot_payload", "normalize_sector_key"]
