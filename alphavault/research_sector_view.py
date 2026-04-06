from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from alphavault.research_signal_view import (
    build_related_stock_rows,
    build_signal_rows,
    merge_post_fields,
    sector_mask,
)


@dataclass(frozen=True)
class SectorResearchView:
    header_title: str
    signals: list[dict[str, str]]
    related_stocks: list[dict[str, str]]


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
        )

    sector_view = assertions[sector_mask(assertions, sector_key)].copy()
    sector_view = merge_post_fields(sector_view, posts)
    return SectorResearchView(
        header_title=sector_key,
        signals=build_signal_rows(sector_view, posts=posts),
        related_stocks=build_related_stock_rows(sector_view),
    )


__all__ = ["SectorResearchView", "build_sector_research_view"]
