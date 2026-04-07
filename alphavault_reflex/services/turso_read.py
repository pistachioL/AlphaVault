from __future__ import annotations

import pandas as pd

from alphavault.homework_trade_feed import (
    HOMEWORK_DEFAULT_VIEW_KEY,
    load_homework_trade_feed,
    save_homework_trade_feed,
)
from alphavault.research_workbench.service import (
    get_research_workbench_engine_from_env,
)
from alphavault_reflex.services.cache_registry import clear_registered_caches
from alphavault_reflex.services.source_loader import (
    MISSING_TURSO_SOURCES_ERROR,
    WANTED_POST_COLUMNS_FOR_TREE,
    WANTED_TRADE_ASSERTION_COLUMNS,
    load_sources_from_env as _load_sources_from_env,
    load_trade_assertions_from_env,
    load_trade_sources_cached,
)
from alphavault_reflex.services.stock_fast_loader import (
    FAST_STOCK_ASSERTION_LIMIT_PER_SOURCE,
    FAST_STOCK_TOTAL_TIMEOUT_SECONDS,
    load_stock_alias_keys_cached,
    load_stock_sources_fast_from_env,
    load_stock_trade_sources_fast_cached,
)
from alphavault_reflex.services.trade_board_loader import (
    DEFAULT_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS,
    ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS,
    TRADE_BOARD_ASSERTION_COLUMNS,
    load_homework_board_payload_cached,
    load_homework_board_payload_from_env,
    load_stock_alias_relations_cached,
    load_stock_alias_relations_from_env,
    load_trade_board_assertions_cached,
    load_trade_board_assertions_from_env,
)
from alphavault_reflex.services.tree_loader import (
    load_posts_for_tree_from_env,
    load_single_post_for_tree_cached,
    load_single_post_for_tree_from_env,
)
from alphavault_reflex.services.url_loader import (
    load_post_urls_cached,
    load_post_urls_from_env,
)


def load_sources_from_env() -> tuple[pd.DataFrame, pd.DataFrame, str]:
    return _load_sources_from_env(
        load_posts_for_tree_from_env_fn=load_posts_for_tree_from_env
    )


def load_homework_trade_feed_from_env(
    *, view_key: str = HOMEWORK_DEFAULT_VIEW_KEY
) -> dict[str, object]:
    engine = get_research_workbench_engine_from_env()
    return load_homework_trade_feed(engine, view_key=view_key)


def save_homework_trade_feed_from_env(
    *,
    caption: str,
    used_window_days: int,
    rows: list[dict[str, object]] | list[dict[str, str]],
    view_key: str = HOMEWORK_DEFAULT_VIEW_KEY,
) -> None:
    engine = get_research_workbench_engine_from_env()
    save_homework_trade_feed(
        engine,
        view_key=view_key,
        caption=caption,
        used_window_days=used_window_days,
        rows=rows,
    )


def clear_reflex_source_caches() -> None:
    clear_registered_caches(
        load_trade_sources_cached.cache_clear,
        load_trade_board_assertions_cached.cache_clear,
        load_stock_alias_keys_cached.cache_clear,
        load_stock_trade_sources_fast_cached.cache_clear,
        load_homework_board_payload_cached.cache_clear,
        load_single_post_for_tree_cached.cache_clear,
        load_post_urls_cached.cache_clear,
        load_stock_alias_relations_cached.cache_clear,
    )


__all__ = [
    "DEFAULT_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS",
    "ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS",
    "FAST_STOCK_ASSERTION_LIMIT_PER_SOURCE",
    "FAST_STOCK_TOTAL_TIMEOUT_SECONDS",
    "HOMEWORK_DEFAULT_VIEW_KEY",
    "MISSING_TURSO_SOURCES_ERROR",
    "TRADE_BOARD_ASSERTION_COLUMNS",
    "WANTED_POST_COLUMNS_FOR_TREE",
    "WANTED_TRADE_ASSERTION_COLUMNS",
    "clear_reflex_source_caches",
    "load_homework_board_payload_from_env",
    "load_homework_trade_feed_from_env",
    "load_post_urls_from_env",
    "load_posts_for_tree_from_env",
    "load_single_post_for_tree_from_env",
    "load_sources_from_env",
    "save_homework_trade_feed_from_env",
    "load_stock_alias_relations_from_env",
    "load_stock_sources_fast_from_env",
    "load_trade_assertions_from_env",
    "load_trade_board_assertions_from_env",
]
