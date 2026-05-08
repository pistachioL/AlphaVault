from __future__ import annotations

import importlib
from typing import Any


_EXPORTS = {
    "DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS": (
        "alphavault.capabilities.stock_analysis",
        "DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS",
    ),
    "DEFAULT_POST_SEARCH_LIMIT": (
        "alphavault.capabilities.post_search",
        "DEFAULT_POST_SEARCH_LIMIT",
    ),
    "DEFAULT_OBVIOUS_TRADE_FILTER": (
        "alphavault.capabilities.stock_obvious_trades",
        "DEFAULT_OBVIOUS_TRADE_FILTER",
    ),
    "DEFAULT_OBVIOUS_TRADE_LIMIT": (
        "alphavault.capabilities.stock_obvious_trades",
        "DEFAULT_OBVIOUS_TRADE_LIMIT",
    ),
    "DEFAULT_OBVIOUS_TRADE_LOOKBACK_DAYS": (
        "alphavault.capabilities.stock_obvious_trades",
        "DEFAULT_OBVIOUS_TRADE_LOOKBACK_DAYS",
    ),
    "DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH": (
        "alphavault.capabilities.stock_obvious_trades",
        "DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH",
    ),
    "DEFAULT_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE": (
        "alphavault.capabilities.stock_obvious_trades",
        "DEFAULT_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE",
    ),
    "DEFAULT_STOCK_EVIDENCE_MAX_POSTS": (
        "alphavault.capabilities.stock_analysis",
        "DEFAULT_STOCK_EVIDENCE_MAX_POSTS",
    ),
    "DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS": (
        "alphavault.capabilities.stock_analysis",
        "DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS",
    ),
    "DEFAULT_STOCK_OBVIOUS_TRADE_LOOKBACK_DAYS": (
        "alphavault.capabilities.stock_obvious_trades",
        "DEFAULT_STOCK_OBVIOUS_TRADE_LOOKBACK_DAYS",
    ),
    "DEFAULT_STOCK_RESULT_LIMIT": (
        "alphavault.capabilities.stock_lookup",
        "DEFAULT_STOCK_RESULT_LIMIT",
    ),
    "ObviousTradeListResult": (
        "alphavault.capabilities.stock_obvious_trades",
        "ObviousTradeListResult",
    ),
    "ObviousTradeRow": (
        "alphavault.capabilities.stock_obvious_trades",
        "ObviousTradeRow",
    ),
    "PortfolioCandidateBuckets": (
        "alphavault.capabilities.stock_analysis",
        "PortfolioCandidateBuckets",
    ),
    "PortfolioCandidateRow": (
        "alphavault.capabilities.stock_analysis",
        "PortfolioCandidateRow",
    ),
    "PortfolioContext": ("alphavault.capabilities.stock_analysis", "PortfolioContext"),
    "PostDetailResult": ("alphavault.capabilities.post_detail", "PostDetailResult"),
    "PostSearchResult": ("alphavault.capabilities.post_search", "PostSearchResult"),
    "PostSearchRow": ("alphavault.capabilities.post_search", "PostSearchRow"),
    "StockObviousTradeResult": (
        "alphavault.capabilities.stock_obvious_trades",
        "StockObviousTradeResult",
    ),
    "StockEvidencePack": (
        "alphavault.capabilities.stock_analysis",
        "StockEvidencePack",
    ),
    "StockSummaryResult": (
        "alphavault.capabilities.stock_summary",
        "StockSummaryResult",
    ),
    "StockLookupResult": ("alphavault.capabilities.stock_lookup", "StockLookupResult"),
    "StockLookupRow": ("alphavault.capabilities.stock_lookup", "StockLookupRow"),
    "get_portfolio_context": (
        "alphavault.capabilities.stock_analysis",
        "get_portfolio_context",
    ),
    "get_post_detail": ("alphavault.capabilities.post_detail", "get_post_detail"),
    "get_stock_evidence_pack": (
        "alphavault.capabilities.stock_analysis",
        "get_stock_evidence_pack",
    ),
    "get_stock_obvious_trades": (
        "alphavault.capabilities.stock_obvious_trades",
        "get_stock_obvious_trades",
    ),
    "get_stock_summary": (
        "alphavault.capabilities.stock_summary",
        "get_stock_summary",
    ),
    "get_stock_page": ("alphavault.capabilities.stock_page", "get_stock_page"),
    "get_stock_sidebar": ("alphavault.capabilities.stock_page", "get_stock_sidebar"),
    "list_obvious_trades": (
        "alphavault.capabilities.stock_obvious_trades",
        "list_obvious_trades",
    ),
    "resolve_exact_stock_key": (
        "alphavault.capabilities.stock_lookup",
        "resolve_exact_stock_key",
    ),
    "resolve_stock": ("alphavault.capabilities.stock_lookup", "resolve_stock"),
    "search_posts": ("alphavault.capabilities.post_search", "search_posts"),
    "validate_post_search_query": (
        "alphavault.capabilities.post_search",
        "validate_post_search_query",
    ),
}


def __getattr__(name: str) -> Any:
    export = _EXPORTS.get(name)
    if export is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = export
    value = getattr(importlib.import_module(module_name), attr_name)
    globals()[name] = value
    return value


__all__ = [
    "DEFAULT_OBVIOUS_TRADE_FILTER",
    "DEFAULT_OBVIOUS_TRADE_LIMIT",
    "DEFAULT_OBVIOUS_TRADE_LOOKBACK_DAYS",
    "DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH",
    "DEFAULT_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE",
    "DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS",
    "DEFAULT_POST_SEARCH_LIMIT",
    "DEFAULT_STOCK_EVIDENCE_MAX_POSTS",
    "DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS",
    "DEFAULT_STOCK_OBVIOUS_TRADE_LOOKBACK_DAYS",
    "DEFAULT_STOCK_RESULT_LIMIT",
    "ObviousTradeListResult",
    "ObviousTradeRow",
    "PortfolioCandidateBuckets",
    "PortfolioCandidateRow",
    "PortfolioContext",
    "PostDetailResult",
    "PostSearchResult",
    "PostSearchRow",
    "StockObviousTradeResult",
    "StockEvidencePack",
    "StockSummaryResult",
    "StockLookupResult",
    "StockLookupRow",
    "get_portfolio_context",
    "get_post_detail",
    "get_stock_evidence_pack",
    "get_stock_obvious_trades",
    "get_stock_summary",
    "get_stock_page",
    "get_stock_sidebar",
    "list_obvious_trades",
    "resolve_exact_stock_key",
    "resolve_stock",
    "search_posts",
    "validate_post_search_query",
]
