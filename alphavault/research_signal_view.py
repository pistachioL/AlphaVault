from __future__ import annotations

import pandas as pd

from alphavault.domains.thread_tree.service import build_post_tree_map


MINUTES_PER_HOUR = 60
MINUTES_PER_DAY = 24 * MINUTES_PER_HOUR
MAX_SIGNAL_ROWS = 60


def merge_post_fields(assertions: pd.DataFrame, posts: pd.DataFrame) -> pd.DataFrame:
    if assertions.empty or posts.empty or "post_uid" not in assertions.columns:
        return assertions
    if "post_uid" not in posts.columns:
        return assertions

    merged = assertions.copy()
    post_cols = posts[["post_uid", "raw_text", "display_md", "author"]].copy()
    post_cols = post_cols.rename(
        columns={
            "raw_text": "_post_raw_text",
            "display_md": "_post_display_md",
            "author": "_post_author",
        }
    )
    merged = merged.merge(post_cols, on="post_uid", how="left")
    if "raw_text" not in merged.columns:
        merged["raw_text"] = ""
    if "display_md" not in merged.columns:
        merged["display_md"] = ""
    if "author" not in merged.columns:
        merged["author"] = ""
    merged["raw_text"] = merged["raw_text"].fillna("").astype(str)
    merged["display_md"] = merged["display_md"].fillna("").astype(str)
    merged["author"] = merged["author"].fillna("").astype(str)
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
    return merged.drop(
        columns=["_post_raw_text", "_post_display_md", "_post_author"],
        errors="ignore",
    )


def build_signal_rows(
    view: pd.DataFrame,
    *,
    posts: pd.DataFrame,
    now: pd.Timestamp | None = None,
) -> list[dict[str, str]]:
    if view.empty:
        return []
    rows = view.copy()
    if "created_at" in rows.columns:
        rows["created_at"] = pd.to_datetime(rows["created_at"], errors="coerce")
        rows = rows.sort_values(by="created_at", ascending=False, na_position="last")
    rows = rows.head(MAX_SIGNAL_ROWS)
    out: list[dict[str, str]] = []
    tree_map = build_post_tree_map(
        post_uids=[
            str(uid or "").strip()
            for uid in rows.get("post_uid", pd.Series(dtype=str)).tolist()
            if str(uid or "").strip()
        ],
        posts=posts,
    )
    reference_now = coerce_signal_timestamp(now) or default_signal_reference_time()
    for _, row in rows.iterrows():
        created = row.get("created_at")
        created_text = _format_signal_timestamp(created)
        created_at_line = format_signal_created_at_line(created, now=reference_now)
        post_uid = str(row.get("post_uid") or "").strip()
        tree_label = ""
        tree_text = ""
        if post_uid:
            tree_label, tree_text = tree_map.get(post_uid, ("", ""))
        out.append(
            {
                "post_uid": post_uid,
                "summary": str(row.get("summary") or "").strip(),
                "action": str(row.get("action") or "").strip(),
                "action_strength": str(row.get("action_strength") or "").strip(),
                "author": str(row.get("author") or "").strip(),
                "created_at": created_text,
                "created_at_line": created_at_line,
                "raw_text": str(row.get("raw_text") or "").strip(),
                "display_md": str(row.get("display_md") or "").strip(),
                "tree_label": tree_label,
                "tree_text": tree_text,
            }
        )
    return out


def build_related_stock_rows(view: pd.DataFrame) -> list[dict[str, str]]:
    counts: dict[str, int] = {}
    for raw_key in view.get("topic_key", pd.Series(dtype=str)).tolist():
        stock_key = str(raw_key or "").strip()
        if not stock_key.startswith("stock:"):
            continue
        counts[stock_key] = int(counts.get(stock_key, 0)) + 1
    ranked = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return [
        {"stock_key": stock_key, "mention_count": str(count)}
        for stock_key, count in ranked
    ]


def sector_mask(assertions: pd.DataFrame, sector_key: str) -> pd.Series:
    if "cluster_keys" in assertions.columns:
        return assertions["cluster_keys"].apply(
            lambda item: sector_key in _coerce_list(item)
        )
    if "cluster_key" in assertions.columns:
        return assertions["cluster_key"].astype(str).str.strip().eq(sector_key)
    return pd.Series([False] * len(assertions), index=assertions.index)


def coerce_signal_timestamp(value: object) -> pd.Timestamp | None:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    if getattr(ts, "tzinfo", None) is not None:
        return ts.tz_convert(None)
    return pd.Timestamp(ts)


def default_signal_reference_time() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").tz_convert(None)


def _format_signal_timestamp(value: object) -> str:
    ts = coerce_signal_timestamp(value)
    if ts is None:
        return ""
    return ts.strftime("%Y-%m-%d %H:%M")


def _format_signal_age(value: object, *, now: pd.Timestamp) -> str:
    ts = coerce_signal_timestamp(value)
    if ts is None:
        return ""
    delta_seconds = max((now - ts).total_seconds(), 0)
    minutes = int(delta_seconds // MINUTES_PER_HOUR)
    if minutes < MINUTES_PER_HOUR:
        return f"{minutes}分钟前"
    if minutes < MINUTES_PER_DAY:
        return f"{minutes // MINUTES_PER_HOUR}小时前"
    return f"{minutes // MINUTES_PER_DAY}天前"


def format_signal_created_at_line(value: object, *, now: pd.Timestamp) -> str:
    created_at_text = _format_signal_timestamp(value)
    if not created_at_text:
        return ""
    age_text = _format_signal_age(value, now=now)
    if not age_text:
        return created_at_text
    return f"{created_at_text} · {age_text}"


def _coerce_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


__all__ = [
    "MAX_SIGNAL_ROWS",
    "build_related_stock_rows",
    "build_signal_rows",
    "coerce_signal_timestamp",
    "default_signal_reference_time",
    "format_signal_created_at_line",
    "merge_post_fields",
    "sector_mask",
]
