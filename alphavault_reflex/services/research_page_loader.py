from __future__ import annotations

from alphavault.capabilities.post_detail import PostDetailResult
from alphavault.capabilities.post_detail import get_post_detail
from alphavault.capabilities.stock_page import get_stock_page, get_stock_sidebar
from alphavault.domains.stock.view_scope import STOCK_VIEW_SCOPE_COMPANY
from dataclasses import asdict
import importlib
from functools import cache
from types import ModuleType


@cache
def _load_research_data_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.research_data")


@cache
def _load_source_read_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.source_read")


def load_stock_page_cached_view(
    stock_slug: str,
    *,
    signal_page: int,
    signal_page_size: int,
    author: str = "",
    related_filter: str = "all",
) -> dict[str, object]:
    return get_stock_page(
        stock_slug,
        signal_page=signal_page,
        signal_page_size=signal_page_size,
        author=author,
        related_filter=related_filter,
        view_scope=STOCK_VIEW_SCOPE_COMPANY,
    )


def load_stock_sidebar_cached_view(stock_slug: str) -> dict[str, object]:
    return get_stock_sidebar(stock_slug)


def load_stock_signal_detail_view(post_uid: str) -> PostDetailResult:
    return get_post_detail(post_uid)


def load_sector_page_view(sector_slug: str) -> dict[str, object]:
    sector_key = str(sector_slug or "").strip()
    posts, assertions, err = _load_source_read_module().load_sources_from_env()
    if err:
        return {
            "page_title": sector_key,
            "signals": [],
            "related_stocks": [],
            "load_error": err,
        }
    view = _load_research_data_module().build_sector_research_page_view(
        posts,
        assertions,
        sector_key=sector_key,
    )
    result = asdict(view)
    result["load_error"] = ""
    return result


__all__ = [
    "load_sector_page_view",
    "load_stock_page_cached_view",
    "load_stock_sidebar_cached_view",
    "load_stock_signal_detail_view",
]
