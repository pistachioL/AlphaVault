from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


STOCK_KEY_PREFIX = "stock:"


@dataclass(frozen=True)
class StockResearchView:
    header_title: str
    signals: list[dict[str, str]]
    related_sectors: list[dict[str, str]]
    pending_candidates: list[dict[str, str]]


@dataclass(frozen=True)
class SectorResearchView:
    header_title: str
    signals: list[dict[str, str]]
    related_stocks: list[dict[str, str]]
    pending_candidates: list[dict[str, str]]


def build_stock_research_view(
    posts: pd.DataFrame,
    assertions: pd.DataFrame,
    *,
    stock_key: str,
) -> StockResearchView:
    stock_key = str(stock_key or "").strip()
    if assertions.empty or not stock_key:
        return StockResearchView(
            header_title=_stock_title(stock_key),
            signals=[],
            related_sectors=[],
            pending_candidates=[],
        )

    stock_view = assertions[
        assertions["topic_key"].astype(str).str.strip() == stock_key
    ]
    stock_view = _merge_post_fields(stock_view.copy(), posts)
    return StockResearchView(
        header_title=_stock_title(stock_key),
        signals=_build_signal_rows(stock_view),
        related_sectors=_build_related_sector_rows(stock_view),
        pending_candidates=[],
    )


def build_sector_research_view(
    posts: pd.DataFrame,
    assertions: pd.DataFrame,
    *,
    sector_key: str,
) -> SectorResearchView:
    sector_key = str(sector_key or "").strip()
    if assertions.empty or not sector_key:
        return SectorResearchView(
            header_title=sector_key,
            signals=[],
            related_stocks=[],
            pending_candidates=[],
        )

    sector_view = assertions[_sector_mask(assertions, sector_key)].copy()
    sector_view = _merge_post_fields(sector_view, posts)
    return SectorResearchView(
        header_title=sector_key,
        signals=_build_signal_rows(sector_view),
        related_stocks=_build_related_stock_rows(sector_view),
        pending_candidates=[],
    )


def _merge_post_fields(assertions: pd.DataFrame, posts: pd.DataFrame) -> pd.DataFrame:
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


def _build_signal_rows(view: pd.DataFrame) -> list[dict[str, str]]:
    if view.empty:
        return []
    rows = view.copy()
    if "created_at" in rows.columns:
        rows["created_at"] = pd.to_datetime(rows["created_at"], errors="coerce")
        rows = rows.sort_values(by="created_at", ascending=False, na_position="last")
    out: list[dict[str, str]] = []
    for _, row in rows.iterrows():
        created = row.get("created_at")
        created_text = ""
        if pd.notna(created):
            created_text = str(pd.Timestamp(created))
        out.append(
            {
                "post_uid": str(row.get("post_uid") or "").strip(),
                "summary": str(row.get("summary") or "").strip(),
                "action": str(row.get("action") or "").strip(),
                "action_strength": str(row.get("action_strength") or "").strip(),
                "author": str(row.get("author") or "").strip(),
                "created_at": created_text,
                "raw_text": str(row.get("raw_text") or "").strip(),
                "display_md": str(row.get("display_md") or "").strip(),
            }
        )
    return out


def _build_related_sector_rows(view: pd.DataFrame) -> list[dict[str, str]]:
    counts: dict[str, int] = {}
    for item in view.get("cluster_keys", pd.Series(dtype=object)).tolist():
        for sector_key in _coerce_list(item):
            counts[sector_key] = int(counts.get(sector_key, 0)) + 1
    ranked = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return [
        {"sector_key": sector_key, "mention_count": str(count)}
        for sector_key, count in ranked
    ]


def _build_related_stock_rows(view: pd.DataFrame) -> list[dict[str, str]]:
    counts: dict[str, int] = {}
    for raw_key in view.get("topic_key", pd.Series(dtype=str)).tolist():
        stock_key = str(raw_key or "").strip()
        if not stock_key.startswith(STOCK_KEY_PREFIX):
            continue
        counts[stock_key] = int(counts.get(stock_key, 0)) + 1
    ranked = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return [
        {"stock_key": stock_key, "mention_count": str(count)}
        for stock_key, count in ranked
    ]


def _sector_mask(assertions: pd.DataFrame, sector_key: str) -> pd.Series:
    if "cluster_keys" in assertions.columns:
        return assertions["cluster_keys"].apply(
            lambda item: sector_key in _coerce_list(item)
        )
    if "cluster_key" in assertions.columns:
        return assertions["cluster_key"].astype(str).str.strip().eq(sector_key)
    return pd.Series([False] * len(assertions), index=assertions.index)


def _coerce_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _stock_title(stock_key: str) -> str:
    stock_key = str(stock_key or "").strip()
    if stock_key.startswith(STOCK_KEY_PREFIX):
        return stock_key[len(STOCK_KEY_PREFIX) :]
    return stock_key
