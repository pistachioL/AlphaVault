from __future__ import annotations

from typing import TypedDict

from alphavault.capabilities.stock_lookup import (
    DEFAULT_STOCK_RESULT_LIMIT,
    StockLookupRow,
    resolve_stock as _resolve_stock,
)
from alphavault_reflex.services.research_models import build_stock_route


class StockSearchRow(TypedDict):
    stock_key: str
    label: str
    subtitle: str
    href: str
    match_reason: str
    is_exact: str


class StockSearchResult(TypedDict):
    exact_href: str
    rows: list[StockSearchRow]
    error: str


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _rows_to_search_rows(rows: list[StockLookupRow]) -> list[StockSearchRow]:
    out: list[StockSearchRow] = []
    for row in rows:
        stock_key = _clean_text(row.get("stock_key"))
        if not stock_key:
            continue
        out.append(
            {
                "stock_key": stock_key,
                "label": _clean_text(row.get("label")),
                "subtitle": _clean_text(row.get("subtitle")),
                "href": build_stock_route(stock_key),
                "match_reason": _clean_text(row.get("match_reason")) or "股票命中",
                "is_exact": _clean_text(row.get("is_exact")),
            }
        )
    return out


def resolve_exact_stock_keys(query: str) -> list[str]:
    result = _resolve_stock(query, limit=DEFAULT_STOCK_RESULT_LIMIT)
    return [
        _clean_text(row.get("stock_key"))
        for row in result.get("rows") or []
        if _clean_text(row.get("stock_key")) and _clean_text(row.get("is_exact")) == "1"
    ]


def search_stocks(
    query: str, *, limit: int = DEFAULT_STOCK_RESULT_LIMIT
) -> StockSearchResult:
    result = _resolve_stock(query, limit=limit)
    resolved_stock_key = _clean_text(result.get("resolved_stock_key"))
    exact_href = build_stock_route(resolved_stock_key) if resolved_stock_key else ""
    return {
        "exact_href": exact_href,
        "rows": _rows_to_search_rows(result.get("rows") or []),
        "error": _clean_text(result.get("error")),
    }


__all__ = [
    "StockSearchResult",
    "StockSearchRow",
    "resolve_exact_stock_keys",
    "search_stocks",
]
