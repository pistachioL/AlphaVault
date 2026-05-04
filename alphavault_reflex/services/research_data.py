from __future__ import annotations

from datetime import datetime

from alphavault.domains.content.row_mapper import map_posts
from alphavault.domains.signal.aggregator import (
    attach_signal_tree_context,
    filter_signals_for_sector,
    merge_assertions_with_posts,
    slice_signals,
)
from alphavault.domains.signal.row_mapper import map_assertions
from alphavault.domains.stock.service import (
    build_stock_search_index,
    build_stock_signal_page,
)
from alphavault_reflex.view_models.research_page import (
    SectorResearchPageView,
    StockResearchPageView,
    build_related_sector_view_rows,
    build_related_stock_view_rows,
    build_signal_row_views,
)
from alphavault.research_sector_view import build_sector_research_view
from alphavault_reflex.services.research_models import (
    build_sector_route,
    build_stock_route,
)


def build_search_index(
    posts: list[dict[str, object]],
    assertions: list[dict[str, object]],
    *,
    stock_relations: list[dict[str, object]] | None = None,
    ai_alias_map: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    del posts
    if not assertions:
        return []
    assertion_models = map_assertions(assertions)
    stock_hits = build_stock_search_index(
        assertion_models,
        stock_relations=stock_relations,
        ai_alias_map=ai_alias_map,
    )
    sector_hits: dict[str, dict[str, str]] = {}

    for assertion in assertion_models:
        for sector_key in assertion.cluster_keys:
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
    posts: list[dict[str, object]],
    assertions: list[dict[str, object]],
    *,
    stock_key: str,
    stock_relations: list[dict[str, object]] | None = None,
    ai_alias_map: dict[str, str] | None = None,
    signal_page: int = 1,
    signal_page_size: int = 60,
    now: datetime | None = None,
) -> StockResearchPageView:
    stock_key = str(stock_key or "").strip()
    if not assertions or not stock_key:
        return StockResearchPageView(
            entity_key=stock_key,
            page_title=stock_key.removeprefix("stock:"),
            signals=[],
            signal_total=0,
            signal_page=1,
            signal_page_size=_clamp_signal_page_size(signal_page_size),
            related_sectors=[],
        )
    post_models = map_posts(posts)
    assertion_models = map_assertions(assertions)
    stock_page = build_stock_signal_page(
        stock_key=stock_key,
        posts=post_models,
        assertions=assertion_models,
        stock_relations=stock_relations,
        ai_alias_map=ai_alias_map,
        signal_page=signal_page,
        signal_page_size=_clamp_signal_page_size(signal_page_size),
    )
    return StockResearchPageView(
        entity_key=stock_page.entity_key,
        page_title=stock_page.page_title,
        signals=build_signal_row_views(stock_page.signals, now=now),
        signal_total=stock_page.signal_total,
        signal_page=stock_page.signal_page,
        signal_page_size=stock_page.signal_page_size,
        related_sectors=build_related_sector_view_rows(stock_page.related_sectors),
    )


def build_sector_research_page_view(
    posts: list[dict[str, object]],
    assertions: list[dict[str, object]],
    *,
    sector_key: str,
    now: datetime | None = None,
) -> SectorResearchPageView:
    post_models = map_posts(posts)
    assertion_models = map_assertions(assertions)
    signals = attach_signal_tree_context(
        merge_assertions_with_posts(assertion_models, post_models),
        posts=post_models,
    )
    sector_signals = filter_signals_for_sector(signals, sector_key=sector_key)
    signal_slice, _signal_total, _signal_page = slice_signals(
        sector_signals,
        page=1,
        page_size=60,
    )
    return SectorResearchPageView(
        page_title=str(sector_key or "").strip(),
        signals=build_signal_row_views(signal_slice, now=now),
        related_stocks=build_related_stock_view_rows(
            build_sector_research_view(
                posts, assertions, sector_key=sector_key
            ).related_stocks
        ),
    )


def _clamp_signal_page_size(value: object) -> int:
    try:
        size = int(str(value or "").strip())
    except (TypeError, ValueError):
        size = 60
    if size <= 0:
        size = 60
    return max(1, min(size, 60))


__all__ = [
    "SectorResearchPageView",
    "StockResearchPageView",
    "build_sector_research_page_view",
    "build_search_index",
    "build_stock_research_view",
]
