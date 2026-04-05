from __future__ import annotations

from dataclasses import dataclass
import re

import pandas as pd

from alphavault.research_sector_view import (
    SectorResearchView,
    build_sector_research_view,
)
from alphavault.research_signal_view import build_signal_rows, merge_post_fields
from alphavault.weibo.display import strip_image_label_lines
from alphavault_reflex.services.research_models import (
    build_sector_route,
    build_stock_route,
)
from alphavault.domains.stock.object_index import (
    build_stock_object_index,
    build_stock_search_rows,
    filter_assertions_for_stock_object,
)


STOCK_KEY_PREFIX = "stock:"
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
    backfill_posts: list[dict[str, str]]


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
    stock_view = merge_post_fields(stock_view.copy(), posts)
    signal_slice, signal_total, signal_page = _slice_signal_view(
        stock_view,
        page=signal_page,
        page_size=signal_page_size,
    )
    return StockResearchView(
        entity_key=entity_key,
        header_title=stock_index.header_title(entity_key),
        signals=build_signal_rows(signal_slice, posts=posts, now=now),
        signal_total=signal_total,
        signal_page=signal_page,
        signal_page_size=_clamp_signal_page_size(signal_page_size),
        related_sectors=_build_related_sector_rows(stock_view),
        backfill_posts=[],
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
        haystack = strip_image_label_lines(raw_text).strip()
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


__all__ = [
    "SectorResearchView",
    "StockResearchView",
    "build_search_index",
    "build_sector_research_view",
    "build_stock_research_view",
]
