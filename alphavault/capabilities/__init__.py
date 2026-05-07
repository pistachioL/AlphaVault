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
    "DEFAULT_STOCK_EVIDENCE_MAX_POSTS": (
        "alphavault.capabilities.stock_analysis",
        "DEFAULT_STOCK_EVIDENCE_MAX_POSTS",
    ),
    "DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS": (
        "alphavault.capabilities.stock_analysis",
        "DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS",
    ),
    "DEFAULT_STOCK_RESULT_LIMIT": (
        "alphavault.capabilities.stock_lookup",
        "DEFAULT_STOCK_RESULT_LIMIT",
    ),
    "PortfolioContext": ("alphavault.capabilities.stock_analysis", "PortfolioContext"),
    "PostDetailResult": ("alphavault.capabilities.post_detail", "PostDetailResult"),
    "PostSearchResult": ("alphavault.capabilities.post_search", "PostSearchResult"),
    "PostSearchRow": ("alphavault.capabilities.post_search", "PostSearchRow"),
    "StockEvidencePack": (
        "alphavault.capabilities.stock_analysis",
        "StockEvidencePack",
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
    "get_stock_page": ("alphavault.capabilities.stock_page", "get_stock_page"),
    "get_stock_sidebar": ("alphavault.capabilities.stock_page", "get_stock_sidebar"),
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
    "DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS",
    "DEFAULT_POST_SEARCH_LIMIT",
    "DEFAULT_STOCK_EVIDENCE_MAX_POSTS",
    "DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS",
    "DEFAULT_STOCK_RESULT_LIMIT",
    "PortfolioContext",
    "PostDetailResult",
    "PostSearchResult",
    "PostSearchRow",
    "StockEvidencePack",
    "StockLookupResult",
    "StockLookupRow",
    "get_portfolio_context",
    "get_post_detail",
    "get_stock_evidence_pack",
    "get_stock_page",
    "get_stock_sidebar",
    "resolve_exact_stock_key",
    "resolve_stock",
    "search_posts",
    "validate_post_search_query",
]
