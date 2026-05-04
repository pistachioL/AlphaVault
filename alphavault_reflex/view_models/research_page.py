from __future__ import annotations

from dataclasses import dataclass

from alphavault.domains.signal.view_rows import (
    RelatedSectorRowView,
    RelatedStockRowView,
    SignalRowView,
    build_related_sector_view_rows,
    build_related_stock_view_rows,
    build_signal_row_views,
)


@dataclass(frozen=True)
class StockResearchPageView:
    entity_key: str
    page_title: str
    signals: list[dict[str, str]]
    signal_total: int
    signal_page: int
    signal_page_size: int
    related_sectors: list[dict[str, str]]


@dataclass(frozen=True)
class SectorResearchPageView:
    page_title: str
    signals: list[dict[str, str]]
    related_stocks: list[dict[str, str]]


__all__ = [
    "RelatedSectorRowView",
    "RelatedStockRowView",
    "SectorResearchPageView",
    "SignalRowView",
    "StockResearchPageView",
    "build_related_sector_view_rows",
    "build_related_stock_view_rows",
    "build_signal_row_views",
]
