from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from functools import lru_cache
from typing import Any

import pandas as pd

from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql.ui import (
    build_assertion_projection_expr,
    build_assertion_rollup_ctes,
    build_assertion_rollup_joins,
)
from alphavault.db.turso_db import ensure_turso_engine, turso_connect_autocommit
from alphavault.db.turso_env import load_configured_turso_sources_from_env
from alphavault.db.turso_pandas import turso_read_sql_df
from alphavault.env import load_dotenv_if_present
from alphavault.domains.stock.keys import stock_key_lookup_candidates
from alphavault.research_workbench import RESEARCH_RELATIONS_TABLE
from alphavault.research_workbench.service import (
    get_research_workbench_engine_from_env,
)
from alphavault_reflex.services.source_loader import (
    DEFAULT_FATAL_EXCEPTIONS,
    MISSING_TURSO_SOURCES_ERROR,
    WANTED_POST_COLUMNS_FOR_TREE,
    WANTED_TRADE_ASSERTION_COLUMNS,
    standardize_assertions,
    standardize_posts,
)
from alphavault_reflex.services.turso_read_utils import (
    ensure_platform_post_id,
    normalize_assertions_datetime,
    normalize_posts_datetime,
    normalize_stock_key_for_fast_query,
    stock_code_from_stock_key,
)

FAST_STOCK_ASSERTION_LIMIT_PER_SOURCE = 240
FAST_STOCK_TOTAL_TIMEOUT_SECONDS = 8.0

FAST_STOCK_ALIAS_KEY_LIMIT = 64
_STANDARD_TURSO_ERROR_PREFIX = "turso_connect_error:standard:"

FAST_STOCK_ALIAS_KEYS_SQL = """
SELECT right_key
FROM {relations_table}
WHERE relation_type = 'stock_alias'
  AND left_key = :stock_key
  AND relation_label = 'alias_of'
ORDER BY right_key ASC
LIMIT :limit
""".format(relations_table=RESEARCH_RELATIONS_TABLE)


def _dedupe_stock_keys(keys: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw_key in keys:
        key = str(raw_key or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _load_stock_alias_keys(conn: Any, *, stock_key: str) -> list[str]:
    key = str(stock_key or "").strip()
    if not key:
        return []
    rows = conn.execute(
        FAST_STOCK_ALIAS_KEYS_SQL,
        {"stock_key": key, "limit": int(FAST_STOCK_ALIAS_KEY_LIMIT)},
    ).fetchall()

    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if not row:
            continue
        alias_key = str(row[0] or "").strip()
        if not alias_key or alias_key in seen:
            continue
        if not alias_key.startswith("stock:"):
            continue
        seen.add(alias_key)
        out.append(alias_key)
    return out


def _standard_turso_error_text(err: BaseException) -> str:
    text = str(err or "").strip()
    if text.startswith(_STANDARD_TURSO_ERROR_PREFIX):
        return text
    return f"{_STANDARD_TURSO_ERROR_PREFIX}{type(err).__name__}"


@lru_cache(maxsize=64)
def load_stock_alias_keys_cached(stock_key: str) -> tuple[str, ...]:
    normalized_key = normalize_stock_key_for_fast_query(stock_key)
    if not normalized_key:
        return ()
    try:
        engine = get_research_workbench_engine_from_env()
        with turso_connect_autocommit(engine) as conn:
            return tuple(_load_stock_alias_keys(conn, stock_key=normalized_key))
    except BaseException as err:
        if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
            raise
        raise RuntimeError(_standard_turso_error_text(err)) from err


@lru_cache(maxsize=64)
def load_stock_trade_sources_fast_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    stock_key: str,
    stock_code: str,
    per_source_limit: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    del stock_code
    normalized_key = normalize_stock_key_for_fast_query(stock_key)
    if not normalized_key:
        return pd.DataFrame(), pd.DataFrame()
    limit = max(1, int(per_source_limit or FAST_STOCK_ASSERTION_LIMIT_PER_SOURCE))
    stock_keys = _dedupe_stock_keys(
        stock_key_lookup_candidates(normalized_key)
        + list(load_stock_alias_keys_cached(normalized_key))
    )
    if not stock_keys:
        return pd.DataFrame(), pd.DataFrame()

    engine = ensure_turso_engine(db_url, auth_token)
    with turso_connect_autocommit(engine) as conn:
        params: dict[str, object] = {"stock_key": stock_keys[0], "limit": limit}
        key_clause = "ae_filter.entity_key = :stock_key"
        if len(stock_keys) > 1:
            other_keys = stock_keys[1:]
            placeholders = make_in_placeholders(prefix="k", count=len(other_keys))
            params.update(make_in_params(prefix="k", values=other_keys))
            key_clause = (
                "(ae_filter.entity_key = :stock_key "
                f"OR ae_filter.entity_key IN ({placeholders}))"
            )
        select_expr = ", ".join(
            [
                build_assertion_projection_expr(WANTED_TRADE_ASSERTION_COLUMNS),
                "ae_filter.entity_key AS resolved_entity_key",
            ]
        )
        assertions_query = (
            f"{build_assertion_rollup_ctes()}\n"
            f"SELECT {select_expr}\n"
            "FROM assertions a\n"
            "JOIN assertion_entities ae_filter\n"
            "  ON ae_filter.assertion_id = a.assertion_id\n"
            f"{build_assertion_rollup_joins('a')}\n"
            "WHERE a.action LIKE 'trade.%'\n"
            "  AND ae_filter.entity_type = 'stock'\n"
            f"  AND {key_clause}\n"
            "ORDER BY a.created_at DESC\n"
            "LIMIT :limit"
        )
        assertions = turso_read_sql_df(conn, assertions_query, params=params)

        posts = pd.DataFrame()
        if not assertions.empty:
            post_uids = tuple(
                sorted(
                    {
                        str(uid or "").strip()
                        for uid in assertions.get(
                            "post_uid",
                            pd.Series(dtype=str),
                        ).tolist()
                        if str(uid or "").strip()
                    }
                )
            )
            if post_uids:
                placeholders = ", ".join(["?"] * len(post_uids))
                posts_query = f"""
SELECT {", ".join(WANTED_POST_COLUMNS_FOR_TREE)}
FROM posts
WHERE processed_at IS NOT NULL
  AND post_uid IN ({placeholders})
"""
                posts = turso_read_sql_df(conn, posts_query, params=list(post_uids))

    posts = standardize_posts(posts, source_name=source_name)
    posts = normalize_posts_datetime(posts)
    posts = ensure_platform_post_id(posts)
    assertions = standardize_assertions(assertions, posts, source_name=source_name)
    assertions = normalize_assertions_datetime(assertions)
    return posts, assertions


def load_stock_sources_fast_from_env(
    stock_key: str,
    *,
    per_source_limit: int = FAST_STOCK_ASSERTION_LIMIT_PER_SOURCE,
    load_cached_fn=load_stock_trade_sources_fast_cached,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    normalized_key = normalize_stock_key_for_fast_query(stock_key)
    if not normalized_key:
        return pd.DataFrame(), pd.DataFrame(), ""

    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return pd.DataFrame(), pd.DataFrame(), MISSING_TURSO_SOURCES_ERROR

    stock_code = stock_code_from_stock_key(normalized_key)
    limit = max(1, int(per_source_limit or FAST_STOCK_ASSERTION_LIMIT_PER_SOURCE))

    posts_frames: list[pd.DataFrame] = []
    assertions_frames: list[pd.DataFrame] = []
    errors: list[str] = []
    max_workers = max(1, min(4, int(len(sources))))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                load_cached_fn,
                source.url,
                source.token,
                source.name,
                normalized_key,
                stock_code,
                limit,
            ): source.name
            for source in sources
        }
        done, not_done = wait(
            futures.keys(),
            timeout=float(FAST_STOCK_TOTAL_TIMEOUT_SECONDS),
        )
        for fut in not_done:
            source_name = futures.get(fut, "")
            fut.cancel()
            errors.append(f"turso_timeout:{source_name}")
        for fut in as_completed(done):
            source_name = futures.get(fut, "")
            try:
                posts, assertions = fut.result()
            except BaseException as err:
                if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                    raise
                error_text = str(err or "").strip()
                if error_text.startswith(_STANDARD_TURSO_ERROR_PREFIX):
                    return pd.DataFrame(), pd.DataFrame(), error_text
                errors.append(f"turso_connect_error:{source_name}:{type(err).__name__}")
                continue
            posts_frames.append(posts)
            assertions_frames.append(assertions)

    if not posts_frames and not assertions_frames:
        if errors:
            return pd.DataFrame(), pd.DataFrame(), errors[0]
        return pd.DataFrame(), pd.DataFrame(), "turso_sources_empty"

    non_empty_posts = [frame for frame in posts_frames if not frame.empty]
    non_empty_assertions = [frame for frame in assertions_frames if not frame.empty]
    posts = (
        pd.concat(non_empty_posts, ignore_index=True)
        if non_empty_posts
        else pd.DataFrame()
    )
    assertions = (
        pd.concat(non_empty_assertions, ignore_index=True)
        if non_empty_assertions
        else pd.DataFrame()
    )
    if errors:
        return posts, assertions, f"partial_source_error:{errors[0]}"
    return posts, assertions, ""


__all__ = [
    "FAST_STOCK_ASSERTION_LIMIT_PER_SOURCE",
    "FAST_STOCK_TOTAL_TIMEOUT_SECONDS",
    "load_stock_alias_keys_cached",
    "load_stock_sources_fast_from_env",
    "load_stock_trade_sources_fast_cached",
]
