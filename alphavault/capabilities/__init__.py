from __future__ import annotations

import importlib
from typing import Any


_EXPORTS = {
    "DEFAULT_POST_SEARCH_LIMIT": (
        "alphavault.capabilities.post_search",
        "DEFAULT_POST_SEARCH_LIMIT",
    ),
    "DEFAULT_STOCK_RESULT_LIMIT": (
        "alphavault.capabilities.stock_lookup",
        "DEFAULT_STOCK_RESULT_LIMIT",
    ),
    "PostDetailResult": ("alphavault.capabilities.post_detail", "PostDetailResult"),
    "PostSearchResult": ("alphavault.capabilities.post_search", "PostSearchResult"),
    "PostSearchRow": ("alphavault.capabilities.post_search", "PostSearchRow"),
    "StockLookupResult": ("alphavault.capabilities.stock_lookup", "StockLookupResult"),
    "StockLookupRow": ("alphavault.capabilities.stock_lookup", "StockLookupRow"),
    "get_post_detail": ("alphavault.capabilities.post_detail", "get_post_detail"),
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
    "DEFAULT_POST_SEARCH_LIMIT",
    "DEFAULT_STOCK_RESULT_LIMIT",
    "PostDetailResult",
    "PostSearchResult",
    "PostSearchRow",
    "StockLookupResult",
    "StockLookupRow",
    "get_post_detail",
    "get_stock_page",
    "get_stock_sidebar",
    "resolve_exact_stock_key",
    "resolve_stock",
    "search_posts",
    "validate_post_search_query",
]
