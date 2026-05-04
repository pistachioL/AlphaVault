from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from alphavault.domains.content.models import Post
from alphavault.domains.signal.aggregator import (
    attach_signal_tree_context,
    build_related_sector_rows,
    merge_assertions_with_posts,
    slice_signals,
)
from alphavault.domains.signal.models import Assertion, Signal
from alphavault.domains.signal.row_mapper import map_assertion_rows

if TYPE_CHECKING:
    from alphavault.domains.stock.object_index import StockObjectIndex


@dataclass(frozen=True)
class StockSignalPage:
    entity_key: str
    page_title: str
    signals: list[Signal]
    signal_total: int
    signal_page: int
    signal_page_size: int
    related_sectors: list[dict[str, str]]
    stock_index: StockObjectIndex


def build_stock_signal_page(
    *,
    stock_key: str,
    posts: list[Post],
    assertions: list[Assertion],
    stock_relations: list[dict[str, object]] | None = None,
    ai_alias_map: dict[str, str] | None = None,
    signal_page: int,
    signal_page_size: int,
) -> StockSignalPage:
    from alphavault.domains.stock.object_index import (
        build_stock_object_index,
        filter_assertions_for_stock_object,
    )

    assertion_rows = map_assertion_rows(assertions)
    stock_index = build_stock_object_index(
        assertion_rows,
        stock_relations=stock_relations,
        ai_alias_map=ai_alias_map,
    )
    entity_key = stock_index.resolve(stock_key)
    if not entity_key:
        return StockSignalPage(
            entity_key=str(stock_key or "").strip(),
            page_title=str(stock_key or "").strip().removeprefix("stock:"),
            signals=[],
            signal_total=0,
            signal_page=1,
            signal_page_size=max(1, int(signal_page_size)),
            related_sectors=[],
            stock_index=stock_index,
        )

    filtered_rows = filter_assertions_for_stock_object(
        assertion_rows,
        stock_key=entity_key,
        stock_relations=stock_relations,
        ai_alias_map=ai_alias_map,
        stock_index=stock_index,
    )
    filtered_assertions = [
        assertion
        for assertion in assertions
        if str(assertion.post_uid or "").strip()
        and any(_assertion_matches_row(assertion, row) for row in filtered_rows)
    ]
    signals = merge_assertions_with_posts(filtered_assertions, posts)
    signals = attach_signal_tree_context(signals, posts=posts)
    signal_slice, signal_total, safe_page = slice_signals(
        signals,
        page=signal_page,
        page_size=signal_page_size,
    )
    return StockSignalPage(
        entity_key=entity_key,
        page_title=stock_index.page_title(entity_key),
        signals=signal_slice,
        signal_total=signal_total,
        signal_page=safe_page,
        signal_page_size=max(1, int(signal_page_size)),
        related_sectors=build_related_sector_rows(signals),
        stock_index=stock_index,
    )


def _coerce_row_idx(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(text)
    except ValueError:
        return 0


def _assertion_matches_row(assertion: Assertion, row: dict[str, object]) -> bool:
    return (
        str(row.get("post_uid") or "").strip() == str(assertion.post_uid or "").strip()
        and str(row.get("entity_key") or "").strip()
        == str(assertion.entity_key or "").strip()
        and _coerce_row_idx(row.get("idx")) == int(assertion.idx)
    )


def build_stock_search_index(
    assertions: list[Assertion],
    *,
    stock_relations: list[dict[str, object]] | None = None,
    ai_alias_map: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    from alphavault.domains.stock.object_index import build_stock_search_rows

    return build_stock_search_rows(
        map_assertion_rows(assertions),
        stock_relations=stock_relations,
        ai_alias_map=ai_alias_map,
    )


def prepare_board_assertion_rows(
    assertions: list[dict[str, object]],
    *,
    stock_relations: list[dict[str, object]],
) -> tuple[list[dict[str, object]], dict[str, str]]:
    from alphavault.domains.stock.object_index import build_stock_object_index

    if not assertions:
        return [], {}
    board_assertions = [dict(row) for row in assertions]
    stock_index = build_stock_object_index(
        board_assertions,
        stock_relations=stock_relations,
    )
    board_topic_labels: dict[str, str] = {}
    for row in board_assertions:
        entity_key = str(row.get("entity_key") or "").strip()
        if entity_key.startswith("stock:"):
            group_key = stock_index.resolve(entity_key)
            row["board_group_key"] = group_key
            if group_key:
                board_topic_labels[group_key] = stock_index.page_title(group_key)
            continue
        row["board_group_key"] = entity_key
        if entity_key:
            board_topic_labels[entity_key] = entity_key
    return board_assertions, board_topic_labels


def unique_stock_keys(assertions: list[dict[str, object]]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for row in assertions:
        stock_key = str(row.get("entity_key") or "").strip()
        if not stock_key.startswith("stock:") or stock_key in seen:
            continue
        seen.add(stock_key)
        out.append(stock_key)
    return out


def unique_sector_keys(assertions: list[dict[str, object]]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for row in assertions:
        raw_value = row.get("cluster_keys")
        values = raw_value if isinstance(raw_value, list) else []
        for item in values:
            sector_key = str(item or "").strip()
            if not sector_key or sector_key in seen:
                continue
            seen.add(sector_key)
            out.append(sector_key)
    return out


__all__ = [
    "StockSignalPage",
    "build_stock_search_index",
    "build_stock_signal_page",
    "prepare_board_assertion_rows",
    "unique_sector_keys",
    "unique_stock_keys",
]
