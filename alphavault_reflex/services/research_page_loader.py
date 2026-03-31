from __future__ import annotations

from dataclasses import asdict

from alphavault.research_workbench import (
    ensure_research_workbench_schema,
    get_research_workbench_engine_from_env,
    list_pending_candidates_for_left_key,
)
from alphavault_reflex.services.research_data import build_sector_research_view
from alphavault_reflex.services.stock_hot_read import load_stock_cached_view_from_env
from alphavault_reflex.services.turso_read import load_sources_from_env

from .research_state_utils import (
    normalize_signal_page,
    normalize_signal_page_size,
    normalize_stock_key,
)

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _empty_stock_page_view(
    stock_key: str,
    *,
    signal_page_size: int,
    load_error: str = "",
) -> dict[str, object]:
    return {
        "entity_key": stock_key,
        "header_title": stock_key.removeprefix("stock:"),
        "signals": [],
        "signal_total": 0,
        "signal_page": 1,
        "signal_page_size": normalize_signal_page_size(signal_page_size),
        "related_sectors": [],
        "pending_candidates": [],
        "backfill_posts": [],
        "load_error": str(load_error or "").strip(),
    }


def load_stock_page_cached_view(
    stock_slug: str,
    *,
    signal_page: int,
    signal_page_size: int,
) -> dict[str, object]:
    stock_key = normalize_stock_key(stock_slug)
    view = load_stock_cached_view_from_env(
        stock_key,
        signal_page=normalize_signal_page(signal_page),
        signal_page_size=normalize_signal_page_size(signal_page_size),
    )
    if str(view.get("entity_key") or "").strip() == "":
        return _empty_stock_page_view(
            stock_key,
            signal_page_size=signal_page_size,
            load_error=str(view.get("load_error") or "").strip(),
        )
    return view


def load_sector_page_view(sector_slug: str) -> dict[str, object]:
    sector_key = str(sector_slug or "").strip()
    posts, assertions, err = load_sources_from_env()
    if err:
        return {
            "header_title": sector_key,
            "signals": [],
            "related_stocks": [],
            "pending_candidates": [],
            "load_error": err,
        }
    view = build_sector_research_view(posts, assertions, sector_key=sector_key)
    result = asdict(view)
    left_key = f"cluster:{sector_key}" if sector_key else ""
    try:
        engine = get_research_workbench_engine_from_env()
        ensure_research_workbench_schema(engine)
        result["pending_candidates"] = list_pending_candidates_for_left_key(
            engine,
            left_key=left_key,
            limit=12,
        )
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        result["pending_candidates"] = []
    result["load_error"] = ""
    return result


__all__ = [
    "load_sector_page_view",
    "load_stock_page_cached_view",
]
