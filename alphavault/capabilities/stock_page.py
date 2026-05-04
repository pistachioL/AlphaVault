from __future__ import annotations

import importlib
from functools import cache
from types import ModuleType

from alphavault.domains.stock.keys import normalize_stock_key

DEFAULT_SIGNAL_PAGE_SIZE = 20
MAX_SIGNAL_PAGE_SIZE = 500


@cache
def _load_stock_hot_read_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.stock_hot_read")


def _normalize_signal_page(value: object) -> int:
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        return 1
    if parsed <= 0:
        return 1
    return int(parsed)


def _normalize_signal_page_size(value: object) -> int:
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        return DEFAULT_SIGNAL_PAGE_SIZE
    if parsed <= 0:
        return DEFAULT_SIGNAL_PAGE_SIZE
    return max(1, min(int(parsed), int(MAX_SIGNAL_PAGE_SIZE)))


def _empty_stock_page_view(
    stock_key: str,
    *,
    signal_page_size: int,
    load_error: str = "",
) -> dict[str, object]:
    return {
        "entity_key": stock_key,
        "requested_stock_key": stock_key,
        "page_title": stock_key.removeprefix("stock:"),
        "signals": [],
        "signal_total": 0,
        "signal_page": 1,
        "signal_page_size": _normalize_signal_page_size(signal_page_size),
        "related_sectors": [],
        "same_company_stocks": [],
        "load_error": str(load_error or "").strip(),
    }


def _empty_stock_sidebar_view(*, load_error: str = "") -> dict[str, object]:
    return {
        "related_sectors": [],
        "extras_updated_at": "",
        "load_error": str(load_error or "").strip(),
    }


def get_stock_page(
    stock_key: str,
    *,
    signal_page: int = 1,
    signal_page_size: int = DEFAULT_SIGNAL_PAGE_SIZE,
    author: str = "",
    related_filter: str = "all",
) -> dict[str, object]:
    normalized_stock_key = normalize_stock_key(stock_key)
    author_filter = str(author or "").strip()
    normalized_signal_page = _normalize_signal_page(signal_page)
    normalized_signal_page_size = _normalize_signal_page_size(signal_page_size)
    stock_hot_read = _load_stock_hot_read_module()
    if author_filter:
        view = stock_hot_read.load_stock_cached_view_from_env(
            normalized_stock_key,
            signal_page=normalized_signal_page,
            signal_page_size=normalized_signal_page_size,
            author=author_filter,
            related_filter=related_filter,
        )
    else:
        view = stock_hot_read.load_stock_cached_view_from_env(
            normalized_stock_key,
            signal_page=normalized_signal_page,
            signal_page_size=normalized_signal_page_size,
            related_filter=related_filter,
        )
    if str(view.get("entity_key") or "").strip() == "":
        return _empty_stock_page_view(
            normalized_stock_key,
            signal_page_size=signal_page_size,
            load_error=str(view.get("load_error") or "").strip(),
        )
    return view


def get_stock_sidebar(stock_key: str) -> dict[str, object]:
    normalized_stock_key = normalize_stock_key(stock_key)
    view = _load_stock_hot_read_module().load_stock_sidebar_cached_view(
        normalized_stock_key
    )
    if not view.get("related_sectors") and str(view.get("load_error") or "").strip():
        return _empty_stock_sidebar_view(
            load_error=str(view.get("load_error") or "").strip(),
        )
    return {
        "related_sectors": view.get("related_sectors") or [],
        "load_error": str(view.get("load_error") or "").strip(),
    }


__all__ = [
    "DEFAULT_SIGNAL_PAGE_SIZE",
    "MAX_SIGNAL_PAGE_SIZE",
    "get_stock_page",
    "get_stock_sidebar",
]
