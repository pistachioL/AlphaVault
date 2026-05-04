from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime

from alphavault.domains.signal.aggregator import (
    coerce_signal_timestamp,
    default_signal_reference_time,
    format_signal_created_at_line,
    format_signal_timestamp,
)
from alphavault.domains.signal.models import Signal


@dataclass(frozen=True)
class SignalRowView:
    post_uid: str
    summary: str
    action: str
    action_strength: str
    author: str
    created_at: str
    created_at_line: str
    raw_text: str
    tree_label: str
    tree_text: str


@dataclass(frozen=True)
class RelatedSectorRowView:
    sector_key: str
    mention_count: str


@dataclass(frozen=True)
class RelatedStockRowView:
    stock_key: str
    mention_count: str


def build_signal_row_views(
    signals: list[Signal],
    *,
    now: datetime | None = None,
) -> list[dict[str, str]]:
    reference_now = coerce_signal_timestamp(now) or default_signal_reference_time()
    return [
        asdict(
            SignalRowView(
                post_uid=str(signal.post_uid or "").strip(),
                summary=str(signal.summary or "").strip(),
                action=str(signal.action or "").strip(),
                action_strength=str(signal.action_strength),
                author=str(signal.author or "").strip(),
                created_at=format_signal_timestamp(signal.created_at),
                created_at_line=format_signal_created_at_line(
                    signal.created_at,
                    now=reference_now,
                ),
                raw_text=str(signal.raw_text or "").strip(),
                tree_label=str(signal.tree_label or "").strip(),
                tree_text=str(signal.tree_text or "").strip(),
            )
        )
        for signal in signals
    ]


def build_related_sector_view_rows(
    rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    return [
        asdict(
            RelatedSectorRowView(
                sector_key=str(row.get("sector_key") or "").strip(),
                mention_count=str(row.get("mention_count") or "").strip(),
            )
        )
        for row in rows
    ]


def build_related_stock_view_rows(
    rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    return [
        asdict(
            RelatedStockRowView(
                stock_key=str(row.get("stock_key") or "").strip(),
                mention_count=str(row.get("mention_count") or "").strip(),
            )
        )
        for row in rows
    ]


__all__ = [
    "RelatedSectorRowView",
    "RelatedStockRowView",
    "SignalRowView",
    "build_related_sector_view_rows",
    "build_related_stock_view_rows",
    "build_signal_row_views",
]
