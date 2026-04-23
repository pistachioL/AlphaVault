from __future__ import annotations

from dataclasses import asdict
import importlib
from functools import cache
from types import ModuleType

from alphavault.domains.thread_tree.service import (
    normalize_tree_lookup_post_uid,
    slice_posts_for_single_post_tree,
)

from .research_state_utils import (
    normalize_signal_page,
    normalize_signal_page_size,
    normalize_stock_key,
)

TREE_MESSAGE_EMPTY = "没有对话流。"
TREE_MESSAGE_LOAD_ERROR_PREFIX = "加载失败："


@cache
def _load_homework_board_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.homework_board")


@cache
def _load_research_data_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.research_data")


@cache
def _load_sector_hot_read_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.sector_hot_read")


@cache
def _load_source_read_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.source_read")


@cache
def _load_stock_hot_read_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.stock_hot_read")


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
        "signal_page_size": normalize_signal_page_size(signal_page_size),
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


def load_stock_page_cached_view(
    stock_slug: str,
    *,
    signal_page: int,
    signal_page_size: int,
    author: str = "",
    related_filter: str = "all",
) -> dict[str, object]:
    stock_key = normalize_stock_key(stock_slug)
    author_filter = str(author or "").strip()
    normalized_signal_page = normalize_signal_page(signal_page)
    normalized_signal_page_size = normalize_signal_page_size(signal_page_size)
    stock_hot_read = _load_stock_hot_read_module()
    if author_filter:
        view = stock_hot_read.load_stock_cached_view_from_env(
            stock_key,
            signal_page=normalized_signal_page,
            signal_page_size=normalized_signal_page_size,
            author=author_filter,
            related_filter=related_filter,
        )
    else:
        view = stock_hot_read.load_stock_cached_view_from_env(
            stock_key,
            signal_page=normalized_signal_page,
            signal_page_size=normalized_signal_page_size,
            related_filter=related_filter,
        )
    if str(view.get("entity_key") or "").strip() == "":
        return _empty_stock_page_view(
            stock_key,
            signal_page_size=signal_page_size,
            load_error=str(view.get("load_error") or "").strip(),
        )
    return view


def load_stock_sidebar_cached_view(stock_slug: str) -> dict[str, object]:
    stock_key = normalize_stock_key(stock_slug)
    view = _load_stock_hot_read_module().load_stock_sidebar_cached_view(stock_key)
    if not view.get("related_sectors") and str(view.get("load_error") or "").strip():
        return _empty_stock_sidebar_view(
            load_error=str(view.get("load_error") or "").strip(),
        )
    return {
        "related_sectors": view.get("related_sectors") or [],
        "load_error": str(view.get("load_error") or "").strip(),
    }


def load_stock_signal_detail_view(post_uid: str) -> dict[str, object]:
    uid = normalize_tree_lookup_post_uid(post_uid)
    if not uid:
        return {
            "post_uid": "",
            "raw_text": "",
            "tree_text": "",
            "message": TREE_MESSAGE_EMPTY,
            "load_error": "",
        }

    posts, err = _load_source_read_module().load_single_post_for_tree_from_env(uid)
    if err:
        return {
            "post_uid": uid,
            "raw_text": "",
            "tree_text": "",
            "message": f"{TREE_MESSAGE_LOAD_ERROR_PREFIX}{err}",
            "load_error": err,
        }

    if not posts:
        return {
            "post_uid": uid,
            "raw_text": "",
            "tree_text": "",
            "message": TREE_MESSAGE_EMPTY,
            "load_error": "",
        }

    matched_post = next(
        (
            dict(row)
            for row in posts
            if normalize_tree_lookup_post_uid(row.get("post_uid")) == uid
        ),
        dict(posts[0]),
    )
    raw_text = str(matched_post.get("raw_text") or "").strip()
    posts_view = slice_posts_for_single_post_tree(post_uid=uid, posts=posts)
    _label, tree_text = (
        _load_homework_board_module().build_tree(post_uid=uid, posts=posts_view)
        if posts_view
        else (
            "",
            "",
        )
    )
    if raw_text or tree_text:
        message = ""
    else:
        message = TREE_MESSAGE_EMPTY
    return {
        "post_uid": uid,
        "raw_text": raw_text,
        "tree_text": str(tree_text or "").strip(),
        "message": message,
        "load_error": "",
    }


def load_sector_page_view(sector_slug: str) -> dict[str, object]:
    sector_key = str(sector_slug or "").strip()
    cached_view = _load_sector_hot_read_module().load_sector_cached_view_from_env(
        sector_key
    )
    if bool(cached_view.get("snapshot_hit")):
        return {
            "page_title": str(cached_view.get("page_title") or "").strip(),
            "signals": cached_view.get("signals") or [],
            "related_stocks": cached_view.get("related_stocks") or [],
            "load_error": str(cached_view.get("load_error") or "").strip(),
        }
    cached_error = str(cached_view.get("load_error") or "").strip()
    if cached_error:
        return {
            "page_title": str(cached_view.get("page_title") or sector_key).strip(),
            "signals": [],
            "related_stocks": [],
            "load_error": cached_error,
        }
    posts, assertions, err = _load_source_read_module().load_sources_from_env()
    if err:
        return {
            "page_title": sector_key,
            "signals": [],
            "related_stocks": [],
            "load_error": err,
        }
    view = _load_research_data_module().build_sector_research_view(
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
