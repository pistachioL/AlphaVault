from __future__ import annotations

from dataclasses import dataclass

from alphavault.research_signal_view import (
    build_related_stock_rows,
    build_signal_rows,
    merge_post_fields,
    sector_filter_rows,
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

    sector_view = sector_filter_rows(assertions, sector_key)
    sector_view = merge_post_fields(sector_view, posts)
    return SectorResearchView(
        page_title=sector_key,
        signals=build_signal_rows(sector_view, posts=posts),
        related_stocks=build_related_stock_rows(sector_view),
    )


__all__ = ["SectorResearchView", "build_sector_research_view"]
