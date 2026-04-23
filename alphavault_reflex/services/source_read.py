from __future__ import annotations

import importlib
import sys
from functools import cache
from types import ModuleType
from typing import TYPE_CHECKING

HOMEWORK_DEFAULT_VIEW_KEY = "default"
MISSING_POSTGRES_DSN_ERROR = "Missing POSTGRES_DSN"

_SOURCE_LOADER_EXPORTS = frozenset(
    (
        "WANTED_POST_COLUMNS_FOR_TREE",
        "WANTED_TRADE_ASSERTION_COLUMNS",
    )
)
_STOCK_FAST_LOADER_EXPORTS = frozenset(
    (
        "FAST_STOCK_ASSERTION_LIMIT_PER_SOURCE",
        "FAST_STOCK_TOTAL_TIMEOUT_SECONDS",
    )
)
_TRADE_BOARD_LOADER_EXPORTS = frozenset(
    (
        "DEFAULT_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS",
        "ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS",
        "TRADE_BOARD_ASSERTION_COLUMNS",
    )
)
_CACHE_CLEAR_TARGETS = (
    (
        "alphavault_reflex.services.source_loader",
        (
            "load_trade_sources_cached",
            "load_trade_sources_rows_cached",
            "load_trade_assertion_rows_cached",
            "load_stock_alias_candidate_rows_cached",
        ),
    ),
    (
        "alphavault_reflex.services.stock_fast_loader",
        (
            "load_stock_alias_keys_cached",
            "load_stock_trade_sources_fast_cached",
        ),
    ),
    (
        "alphavault_reflex.services.trade_board_loader",
        (
            "load_trade_board_assertions_cached",
            "load_trade_board_assertion_rows_cached",
            "load_homework_board_payload_cached",
            "load_homework_board_payload_rows_cached",
            "load_stock_alias_relations_cached",
            "load_stock_alias_relation_rows_cached",
        ),
    ),
    (
        "alphavault_reflex.services.tree_loader",
        ("load_single_post_for_tree_cached",),
    ),
    (
        "alphavault_reflex.services.url_loader",
        ("load_post_urls_cached",),
    ),
    (
        "alphavault_reflex.services.stock_hot_read",
        ("clear_stock_hot_read_caches",),
    ),
)

if TYPE_CHECKING:
    from alphavault.db.postgres_db import PostgresEngine


@cache
def _load_env_module() -> ModuleType:
    return importlib.import_module("alphavault.env")


@cache
def _load_postgres_db_module() -> ModuleType:
    return importlib.import_module("alphavault.db.postgres_db")


@cache
def _load_homework_trade_feed_module() -> ModuleType:
    return importlib.import_module("alphavault.homework_trade_feed")


@cache
def _load_research_workbench_service_module() -> ModuleType:
    return importlib.import_module("alphavault.research_workbench.service")


@cache
def _load_source_loader_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.source_loader")


@cache
def _load_stock_fast_loader_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.stock_fast_loader")


@cache
def _load_trade_board_loader_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.trade_board_loader")


@cache
def _load_tree_loader_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.tree_loader")


@cache
def _load_url_loader_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.url_loader")


def load_sources_from_env() -> tuple[
    list[dict[str, object]], list[dict[str, object]], str
]:
    source_loader = _load_source_loader_module()
    return source_loader.load_sources_from_env(
        load_posts_for_tree_from_env_fn=load_posts_for_tree_from_env
    )


def load_stock_alias_candidates_from_env(
    *,
    window_days: int = 30,
    limit: int = 30,
    load_cached_fn=None,
) -> tuple[list[dict[str, str]], str]:
    source_loader = _load_source_loader_module()
    if load_cached_fn is None:
        return source_loader.load_stock_alias_candidates_from_env(
            window_days=window_days,
            limit=limit,
        )
    return source_loader.load_stock_alias_candidates_from_env(
        window_days=window_days,
        limit=limit,
        load_cached_fn=load_cached_fn,
    )


def _build_source_engine(db_url: str, *, source_name: str) -> PostgresEngine:
    postgres_db = _load_postgres_db_module()
    try:
        return postgres_db.ensure_postgres_engine(db_url, schema_name=source_name)
    except TypeError:
        return postgres_db.ensure_postgres_engine(db_url)


def load_source_engines_from_env() -> list[PostgresEngine]:
    _load_env_module().load_dotenv_if_present()
    source_loader = _load_source_loader_module()
    sources = source_loader.load_configured_source_schemas_from_env()
    if not sources:
        raise RuntimeError(MISSING_POSTGRES_DSN_ERROR)
    out: list[PostgresEngine] = []
    seen: set[tuple[str, str]] = set()
    for source in sources:
        db_url = str(getattr(source, "url", getattr(source, "dsn", "")) or "").strip()
        schema_name = str(
            getattr(source, "schema", getattr(source, "name", "")) or ""
        ).strip()
        if not db_url or not schema_name:
            continue
        key = (db_url, schema_name)
        if key in seen:
            continue
        seen.add(key)
        out.append(_build_source_engine(db_url, source_name=schema_name))
    if not out:
        raise RuntimeError(MISSING_POSTGRES_DSN_ERROR)
    return out


def load_homework_trade_feed_from_env(
    *, view_key: str = HOMEWORK_DEFAULT_VIEW_KEY
) -> dict[str, object]:
    homework_trade_feed = _load_homework_trade_feed_module()
    engine = _load_research_workbench_service_module().get_research_workbench_engine_from_env()
    return homework_trade_feed.load_homework_trade_feed(engine, view_key=view_key)


def save_homework_trade_feed_from_env(
    *,
    caption: str,
    used_window_days: int,
    rows: list[dict[str, object]] | list[dict[str, str]],
    view_key: str = HOMEWORK_DEFAULT_VIEW_KEY,
) -> None:
    homework_trade_feed = _load_homework_trade_feed_module()
    engine = _load_research_workbench_service_module().get_research_workbench_engine_from_env()
    homework_trade_feed.save_homework_trade_feed(
        engine,
        view_key=view_key,
        caption=caption,
        used_window_days=used_window_days,
        rows=rows,
    )


def _clear_module_target(module_name: str, attr_name: str) -> None:
    module = sys.modules.get(module_name)
    if module is None:
        return
    target = getattr(module, attr_name, None)
    if target is None:
        return
    cache_clear = getattr(target, "cache_clear", None)
    try:
        if callable(cache_clear):
            cache_clear()
            return
        if callable(target):
            target()
    except Exception:
        return


def clear_reflex_source_caches() -> None:
    for module_name, attr_names in _CACHE_CLEAR_TARGETS:
        for attr_name in attr_names:
            _clear_module_target(module_name, attr_name)


def load_homework_board_payload_from_env(
    start_time: str,
    end_time: str,
    *,
    load_cached_fn=None,
    resolve_workers_fn=None,
) -> tuple[list[dict[str, object]], list[dict[str, object]], str]:
    trade_board_loader = _load_trade_board_loader_module()
    if load_cached_fn is None and resolve_workers_fn is None:
        return trade_board_loader.load_homework_board_payload_from_env(
            start_time,
            end_time,
        )
    kwargs: dict[str, object] = {}
    if load_cached_fn is not None:
        kwargs["load_cached_fn"] = load_cached_fn
    if resolve_workers_fn is not None:
        kwargs["resolve_workers_fn"] = resolve_workers_fn
    return trade_board_loader.load_homework_board_payload_from_env(
        start_time,
        end_time,
        **kwargs,
    )


def load_trade_board_assertions_from_env(
    lookback_days: int,
    *,
    load_cached_fn=None,
) -> tuple[list[dict[str, object]], str]:
    trade_board_loader = _load_trade_board_loader_module()
    if load_cached_fn is None:
        return trade_board_loader.load_trade_board_assertions_from_env(lookback_days)
    return trade_board_loader.load_trade_board_assertions_from_env(
        lookback_days,
        load_cached_fn=load_cached_fn,
    )


def load_stock_alias_relations_from_env(
    *,
    load_cached_fn=None,
) -> tuple[list[dict[str, object]], str]:
    trade_board_loader = _load_trade_board_loader_module()
    if load_cached_fn is None:
        return trade_board_loader.load_stock_alias_relations_from_env()
    return trade_board_loader.load_stock_alias_relations_from_env(
        load_cached_fn=load_cached_fn
    )


def load_stock_sources_fast_from_env(
    stock_key: str,
    *,
    per_source_limit: int = 240,
    load_cached_fn=None,
) -> tuple[list[dict[str, object]], list[dict[str, object]], str]:
    stock_fast_loader = _load_stock_fast_loader_module()
    if load_cached_fn is None:
        return stock_fast_loader.load_stock_sources_fast_from_env(
            stock_key,
            per_source_limit=per_source_limit,
        )
    return stock_fast_loader.load_stock_sources_fast_from_env(
        stock_key,
        per_source_limit=per_source_limit,
        load_cached_fn=load_cached_fn,
    )


def load_trade_assertions_from_env(
    *,
    load_cached_fn=None,
) -> tuple[list[dict[str, object]], str]:
    source_loader = _load_source_loader_module()
    if load_cached_fn is None:
        return source_loader.load_trade_assertions_from_env()
    return source_loader.load_trade_assertions_from_env(load_cached_fn=load_cached_fn)


def load_posts_for_tree_from_env(
    *,
    load_cached_fn=None,
) -> tuple[list[dict[str, object]], str]:
    tree_loader = _load_tree_loader_module()
    if load_cached_fn is None:
        return tree_loader.load_posts_for_tree_from_env()
    return tree_loader.load_posts_for_tree_from_env(load_cached_fn=load_cached_fn)


def load_single_post_for_tree_from_env(
    post_uid: str,
    *,
    load_cached_fn=None,
) -> tuple[list[dict[str, object]], str]:
    tree_loader = _load_tree_loader_module()
    if load_cached_fn is None:
        return tree_loader.load_single_post_for_tree_from_env(post_uid)
    return tree_loader.load_single_post_for_tree_from_env(
        post_uid,
        load_cached_fn=load_cached_fn,
    )


def load_post_urls_from_env(
    post_uids: list[str],
    *,
    load_cached_fn=None,
) -> tuple[dict[str, str], str]:
    url_loader = _load_url_loader_module()
    if load_cached_fn is None:
        return url_loader.load_post_urls_from_env(post_uids)
    return url_loader.load_post_urls_from_env(
        post_uids,
        load_cached_fn=load_cached_fn,
    )


def __getattr__(name: str):
    if name in _SOURCE_LOADER_EXPORTS:
        return getattr(_load_source_loader_module(), name)
    if name in _STOCK_FAST_LOADER_EXPORTS:
        return getattr(_load_stock_fast_loader_module(), name)
    if name in _TRADE_BOARD_LOADER_EXPORTS:
        return getattr(_load_trade_board_loader_module(), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "HOMEWORK_DEFAULT_VIEW_KEY",
    "MISSING_POSTGRES_DSN_ERROR",
    "clear_reflex_source_caches",
    "load_homework_board_payload_from_env",
    "load_homework_trade_feed_from_env",
    "load_post_urls_from_env",
    "load_posts_for_tree_from_env",
    "load_source_engines_from_env",
    "load_single_post_for_tree_from_env",
    "load_stock_alias_candidates_from_env",
    "load_sources_from_env",
    "save_homework_trade_feed_from_env",
    "load_stock_alias_relations_from_env",
    "load_stock_sources_fast_from_env",
    "load_trade_assertions_from_env",
    "load_trade_board_assertions_from_env",
]
