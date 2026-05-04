from __future__ import annotations

from dataclasses import dataclass

from alphavault.domains.content.row_mapper import map_posts
from alphavault.domains.signal.aggregator import (
    attach_signal_tree_context,
    build_related_stock_rows,
    filter_signals_for_sector,
    merge_assertions_with_posts,
)
from alphavault.domains.signal.row_mapper import map_assertions
from alphavault.domains.signal.view_rows import (
    build_related_stock_view_rows,
    build_signal_row_views,
)


@dataclass(frozen=True)
class SectorResearchView:
    page_title: str
    signals: list[dict[str, str]]
    related_stocks: list[dict[str, str]]


def build_sector_research_view(
    posts: list[dict[str, object]],
    assertions: list[dict[str, object]],
    *,
    sector_key: str,
) -> SectorResearchView:
    sector_key = str(sector_key or "").strip()
    if not assertions or not sector_key:
        return SectorResearchView(
            page_title=sector_key,
            signals=[],
            related_stocks=[],
        )

    post_models = map_posts(posts)
    assertion_models = map_assertions(assertions)
    signals = attach_signal_tree_context(
        merge_assertions_with_posts(assertion_models, post_models),
        posts=post_models,
    )
    sector_view = filter_signals_for_sector(signals, sector_key=sector_key)
    return SectorResearchView(
        page_title=sector_key,
        signals=build_signal_row_views(sector_view),
        related_stocks=build_related_stock_view_rows(
            build_related_stock_rows(sector_view)
        ),
    )


__all__ = ["SectorResearchView", "build_sector_research_view"]
