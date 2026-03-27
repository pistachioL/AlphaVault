from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Callable

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
from alphavault_reflex.services.stock_objects import (
    build_stock_object_index,
    build_stock_search_rows,
    filter_assertions_for_stock_object,
)
from alphavault_reflex.services.thread_tree import build_post_tree_map


STOCK_KEY_PREFIX = "stock:"
MINUTES_PER_HOUR = 60
MINUTES_PER_DAY = 24 * MINUTES_PER_HOUR
MAX_SIGNAL_ROWS = 60
MAX_BACKFILL_SCAN_ROWS = 2000
MAX_BACKFILL_TERM_COUNT = 12
MAX_BACKFILL_ROWS = 12


@dataclass(frozen=True)
class StockResearchView:
    entity_key: str
    header_title: str
    signals: list[dict[str, str]]
    signal_total: int
    signal_page: int
    signal_page_size: int
    related_sectors: list[dict[str, str]]
    pending_candidates: list[dict[str, str]]
    backfill_posts: list[dict[str, str]]


@dataclass(frozen=True)
class SectorResearchView:
    header_title: str
    signals: list[dict[str, str]]
    related_stocks: list[dict[str, str]]
    pending_candidates: list[dict[str, str]]


def build_search_index(
    posts: pd.DataFrame,
    assertions: pd.DataFrame,
    *,
    stock_relations: pd.DataFrame | None = None,
    ai_alias_map: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    del posts
    if assertions.empty:
        return []

    stock_hits = build_stock_search_rows(
        assertions,
        stock_relations=stock_relations,
        ai_alias_map=ai_alias_map,
    )
    sector_hits: dict[str, dict[str, str]] = {}

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
    ranked_stocks = sorted(
        [
            {
                **row,
                "href": build_stock_route(str(row.get("entity_key") or "").strip()),
            }
            for row in stock_hits
        ],
        key=lambda row: row["label"],
    )
    ranked_sectors = sorted(sector_hits.values(), key=lambda row: row["label"])
    return ranked_stocks + ranked_sectors


def build_stock_research_view(
    posts: pd.DataFrame,
    assertions: pd.DataFrame,
    *,
    stock_key: str,
    stock_relations: pd.DataFrame | None = None,
    ai_alias_map: dict[str, str] | None = None,
    signal_page: int = 1,
    signal_page_size: int = MAX_SIGNAL_ROWS,
    now: pd.Timestamp | None = None,
) -> StockResearchView:
    stock_key = str(stock_key or "").strip()
    if assertions.empty or not stock_key:
        return StockResearchView(
            entity_key=stock_key,
            header_title=_stock_title(stock_key),
            signals=[],
            signal_total=0,
            signal_page=1,
            signal_page_size=_clamp_signal_page_size(signal_page_size),
            related_sectors=[],
            pending_candidates=[],
            backfill_posts=[],
        )

    stock_index = build_stock_object_index(
        assertions,
        stock_relations=stock_relations,
        ai_alias_map=ai_alias_map,
    )
    entity_key = stock_index.resolve(stock_key)
    stock_view = filter_assertions_for_stock_object(
        assertions,
        stock_key=entity_key,
        stock_relations=stock_relations,
        ai_alias_map=ai_alias_map,
        stock_index=stock_index,
    )
    stock_view = _merge_post_fields(stock_view.copy(), posts)
    signal_slice, signal_total, signal_page = _slice_signal_view(
        stock_view,
        page=signal_page,
        page_size=signal_page_size,
    )
    return StockResearchView(
        entity_key=entity_key,
        header_title=stock_index.header_title(entity_key),
        signals=_build_signal_rows(signal_slice, posts=posts, now=now),
        signal_total=signal_total,
        signal_page=signal_page,
        signal_page_size=_clamp_signal_page_size(signal_page_size),
        related_sectors=_build_related_sector_rows(stock_view),
        pending_candidates=[],
        backfill_posts=[],
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
    should_continue: Callable[[], bool] | None = None,
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
            should_continue=should_continue,
        )
    )


def build_sector_pending_candidates(
    assertions: pd.DataFrame,
    *,
    sector_key: str,
    ai_enabled: bool,
    should_continue: Callable[[], bool] | None = None,
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
            should_continue=should_continue,
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


def _coerce_positive_int(value: object, *, default: int) -> int:
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        return int(default)
    if parsed <= 0:
        return int(default)
    return int(parsed)


def _clamp_signal_page_size(value: object) -> int:
    size = _coerce_positive_int(value, default=MAX_SIGNAL_ROWS)
    return max(1, min(size, MAX_SIGNAL_ROWS))


def _slice_signal_view(
    view: pd.DataFrame,
    *,
    page: int,
    page_size: int,
) -> tuple[pd.DataFrame, int, int]:
    if view.empty:
        return view, 0, 1

    safe_page_size = _clamp_signal_page_size(page_size)
    safe_page = _coerce_positive_int(page, default=1)

    rows = view.copy()
    if "created_at" in rows.columns:
        rows["created_at"] = pd.to_datetime(rows["created_at"], errors="coerce")
        rows = rows.sort_values(by="created_at", ascending=False, na_position="last")

    total = int(len(rows.index))
    if total <= 0:
        return rows.head(0), 0, 1

    total_pages = max(1, (total + safe_page_size - 1) // safe_page_size)
    safe_page = min(safe_page, total_pages)

    start = (safe_page - 1) * safe_page_size
    end = start + safe_page_size
    return rows.iloc[start:end], total, safe_page


def _build_signal_rows(
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
    reference_now = _coerce_signal_timestamp(now) or _default_signal_reference_time()
    for _, row in rows.iterrows():
        created = row.get("created_at")
        created_text = _format_signal_timestamp(created)
        created_at_line = _format_signal_created_at_line(created, now=reference_now)
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


def _coerce_signal_timestamp(value: object) -> pd.Timestamp | None:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    if getattr(ts, "tzinfo", None) is not None:
        return ts.tz_convert(None)
    return pd.Timestamp(ts)


def _default_signal_reference_time() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").tz_convert(None)


def _format_signal_timestamp(value: object) -> str:
    ts = _coerce_signal_timestamp(value)
    if ts is None:
        return ""
    return ts.strftime("%Y-%m-%d %H:%M")


def _format_signal_age(value: object, *, now: pd.Timestamp) -> str:
    ts = _coerce_signal_timestamp(value)
    if ts is None:
        return ""
    delta_seconds = max((now - ts).total_seconds(), 0)
    minutes = int(delta_seconds // MINUTES_PER_HOUR)
    if minutes < MINUTES_PER_HOUR:
        return f"{minutes}分钟前"
    if minutes < MINUTES_PER_DAY:
        return f"{minutes // MINUTES_PER_HOUR}小时前"
    return f"{minutes // MINUTES_PER_DAY}天前"


def _format_signal_created_at_line(value: object, *, now: pd.Timestamp) -> str:
    created_at_text = _format_signal_timestamp(value)
    if not created_at_text:
        return ""
    age_text = _format_signal_age(value, now=now)
    if not age_text:
        return created_at_text
    return f"{created_at_text} · {age_text}"


def _build_stock_backfill_rows(
    posts: pd.DataFrame,
    stock_view: pd.DataFrame,
    *,
    object_terms: list[str],
) -> list[dict[str, str]]:
    if posts.empty or not object_terms:
        return []
    cleaned_terms = [str(term or "").strip() for term in object_terms]
    scan_terms = [term for term in cleaned_terms if term][:MAX_BACKFILL_TERM_COUNT]
    if not scan_terms:
        return []
    scan_terms_lower = [term.lower() for term in scan_terms]
    existing_post_uids = {
        str(uid or "").strip()
        for uid in stock_view.get("post_uid", pd.Series(dtype=str)).tolist()
        if str(uid or "").strip()
    }
    rows = posts.copy()
    if "post_uid" in rows.columns:
        rows["post_uid"] = rows["post_uid"].fillna("").astype(str).str.strip()
        rows = rows[rows["post_uid"].ne("")]
        rows = rows[~rows["post_uid"].isin(existing_post_uids)]
    if rows.empty:
        return []
    if "created_at" in rows.columns:
        rows["created_at"] = pd.to_datetime(rows["created_at"], errors="coerce")
        rows = rows.sort_values(by="created_at", ascending=False, na_position="last")
    rows = rows.head(MAX_BACKFILL_SCAN_ROWS)

    out: list[dict[str, str]] = []
    for _, row in rows.iterrows():
        raw_text = str(row.get("raw_text") or "").strip()
        display_md = str(row.get("display_md") or "").strip()
        haystack = (raw_text or display_md).strip()
        if not haystack:
            continue
        haystack_lower = haystack.lower()
        matched_terms = [
            term
            for term, term_lower in zip(scan_terms, scan_terms_lower)
            if term_lower and term_lower in haystack_lower
        ]
        if not matched_terms:
            continue
        preview = re.sub(r"\s+", " ", haystack).strip()
        if len(preview) > 180:
            preview = f"{preview[:177]}..."
        created_text = ""
        created = row.get("created_at")
        if pd.notna(created):
            created_text = str(pd.Timestamp(created))
        out.append(
            {
                "post_uid": str(row.get("post_uid") or "").strip(),
                "author": str(row.get("author") or "").strip(),
                "created_at": created_text,
                "url": str(row.get("url") or "").strip(),
                "matched_terms": ", ".join(matched_terms[:3]),
                "preview": preview,
            }
        )
        if len(out) >= MAX_BACKFILL_ROWS:
            break
    return out


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


def _stock_object_terms(stock_index, entity_key: str) -> list[str]:
    member_keys = set(stock_index.member_keys_by_object_key.get(entity_key, set()))
    member_keys.add(entity_key)
    terms: list[str] = []
    for member_key in member_keys:
        stock_value = _stock_title(member_key)
        if not stock_value:
            continue
        terms.append(stock_value)
        if "." in stock_value:
            short_code = stock_value.split(".", 1)[0].strip()
            if short_code:
                terms.append(short_code)
    deduped: list[str] = []
    seen: set[str] = set()
    for term in sorted(terms, key=lambda item: (-len(item), item)):
        text = str(term or "").strip()
        if not text or text in seen:
            continue
        if len(text) < 2 and not any(char.isdigit() for char in text):
            continue
        seen.add(text)
        deduped.append(text)
    return deduped


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
