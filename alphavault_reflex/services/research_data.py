from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from alphavault.research_workbench import make_candidate_id
from alphavault_reflex.services.relation_candidates import (
    RELATION_LABEL_RELATED,
    build_sector_relation_candidates,
    build_stock_alias_candidates,
    build_stock_sector_candidates,
    enrich_candidates_with_ai,
)
from alphavault_reflex.services.research_models import (
    build_sector_route,
    build_stock_route,
)
from alphavault_reflex.services.thread_tree import build_post_tree


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


def build_search_index(
    posts: pd.DataFrame,
    assertions: pd.DataFrame,
) -> list[dict[str, str]]:
    del posts
    if assertions.empty:
        return []

    stock_hits: dict[str, dict[str, str]] = {}
    sector_hits: dict[str, dict[str, str]] = {}

    for raw_key in assertions.get("topic_key", pd.Series(dtype=str)).tolist():
        stock_key = str(raw_key or "").strip()
        if not stock_key.startswith(STOCK_KEY_PREFIX):
            continue
        stock_hits.setdefault(
            stock_key,
            {
                "entity_type": "stock",
                "entity_key": stock_key,
                "label": _stock_title(stock_key),
                "href": build_stock_route(stock_key),
            },
        )

    for item in assertions.get("cluster_keys", pd.Series(dtype=object)).tolist():
        for sector_key in _coerce_list(item):
            sector_hits.setdefault(
                sector_key,
                {
                    "entity_type": "sector",
                    "entity_key": f"cluster:{sector_key}",
                    "label": sector_key,
                    "href": build_sector_route(f"cluster:{sector_key}"),
                },
            )
    ranked_stocks = sorted(stock_hits.values(), key=lambda row: row["label"])
    ranked_sectors = sorted(sector_hits.values(), key=lambda row: row["label"])
    return ranked_stocks + ranked_sectors


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
        signals=_build_signal_rows(stock_view, posts=posts),
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
        signals=_build_signal_rows(sector_view, posts=posts),
        related_stocks=_build_related_stock_rows(sector_view),
        pending_candidates=[],
    )


def build_stock_pending_candidates(
    assertions: pd.DataFrame,
    *,
    stock_key: str,
    ai_enabled: bool,
) -> list[dict[str, str]]:
    alias_rows = build_stock_alias_candidates(assertions, stock_key=stock_key)
    sector_rows = build_stock_sector_candidates(assertions, stock_key=stock_key)

    candidates: list[dict[str, str]] = []
    for row in alias_rows:
        alias_key = str(row.get("alias_key") or "").strip()
        if not alias_key:
            continue
        candidates.append(
            {
                **row,
                "relation_type": "stock_alias",
                "left_key": stock_key,
                "right_key": alias_key,
                "relation_label": "alias_of",
                "candidate_id": make_candidate_id(
                    relation_type="stock_alias",
                    left_key=stock_key,
                    right_key=alias_key,
                    relation_label="alias_of",
                ),
                "candidate_key": alias_key,
                "suggestion_reason": str(row.get("evidence_summary") or "").strip(),
            }
        )
    for row in sector_rows:
        sector_key = str(row.get("sector_key") or "").strip()
        if not sector_key:
            continue
        candidates.append(
            {
                **row,
                "relation_type": "stock_sector",
                "left_key": stock_key,
                "right_key": f"cluster:{sector_key}",
                "relation_label": "member_of",
                "candidate_id": make_candidate_id(
                    relation_type="stock_sector",
                    left_key=stock_key,
                    right_key=f"cluster:{sector_key}",
                    relation_label="member_of",
                ),
                "candidate_key": sector_key,
                "suggestion_reason": str(row.get("evidence_summary") or "").strip(),
            }
        )
    return _finalize_candidate_rows(
        enrich_candidates_with_ai(
            candidates,
            relation_type="stock_sector",
            ai_enabled=ai_enabled,
        )
    )


def build_sector_pending_candidates(
    assertions: pd.DataFrame,
    *,
    sector_key: str,
    ai_enabled: bool,
) -> list[dict[str, str]]:
    candidates = build_sector_relation_candidates(assertions, sector_key=sector_key)
    rows: list[dict[str, str]] = []
    for row in candidates:
        candidate_sector = str(row.get("sector_key") or "").strip()
        if not candidate_sector:
            continue
        rows.append(
            {
                **row,
                "relation_type": "sector_sector",
                "left_key": f"cluster:{sector_key}",
                "right_key": f"cluster:{candidate_sector}",
                "relation_label": RELATION_LABEL_RELATED,
                "candidate_id": make_candidate_id(
                    relation_type="sector_sector",
                    left_key=f"cluster:{sector_key}",
                    right_key=f"cluster:{candidate_sector}",
                    relation_label=RELATION_LABEL_RELATED,
                ),
                "candidate_key": candidate_sector,
                "suggestion_reason": str(row.get("evidence_summary") or "").strip(),
            }
        )
    return _finalize_candidate_rows(
        enrich_candidates_with_ai(
            rows,
            relation_type="sector_sector",
            ai_enabled=ai_enabled,
        )
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


def _build_signal_rows(
    view: pd.DataFrame,
    *,
    posts: pd.DataFrame,
) -> list[dict[str, str]]:
    if view.empty:
        return []
    rows = view.copy()
    if "created_at" in rows.columns:
        rows["created_at"] = pd.to_datetime(rows["created_at"], errors="coerce")
        rows = rows.sort_values(by="created_at", ascending=False, na_position="last")
    out: list[dict[str, str]] = []
    tree_cache: dict[str, tuple[str, str]] = {}
    for _, row in rows.iterrows():
        created = row.get("created_at")
        created_text = ""
        if pd.notna(created):
            created_text = str(pd.Timestamp(created))
        post_uid = str(row.get("post_uid") or "").strip()
        tree_label = ""
        tree_text = ""
        if post_uid:
            tree_label, tree_text = tree_cache.setdefault(
                post_uid,
                build_post_tree(post_uid=post_uid, posts=posts),
            )
        out.append(
            {
                "post_uid": post_uid,
                "summary": str(row.get("summary") or "").strip(),
                "action": str(row.get("action") or "").strip(),
                "action_strength": str(row.get("action_strength") or "").strip(),
                "author": str(row.get("author") or "").strip(),
                "created_at": created_text,
                "raw_text": str(row.get("raw_text") or "").strip(),
                "display_md": str(row.get("display_md") or "").strip(),
                "tree_label": tree_label,
                "tree_text": tree_text,
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


def _stock_title(stock_key: str) -> str:
    stock_key = str(stock_key or "").strip()
    if stock_key.startswith(STOCK_KEY_PREFIX):
        return stock_key[len(STOCK_KEY_PREFIX) :]
    return stock_key
