from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from functools import lru_cache

import pandas as pd

from alphavault.db.introspect import table_columns
from alphavault.db.sql.ui import build_assertions_query
from alphavault.db.turso_db import ensure_turso_engine, turso_connect_autocommit
from alphavault.db.turso_env import load_configured_turso_sources_from_env
from alphavault.db.turso_pandas import turso_read_sql_df
from alphavault.env import load_dotenv_if_present
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


@lru_cache(maxsize=64)
def load_stock_trade_sources_fast_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    stock_key: str,
    stock_code: str,
    per_source_limit: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    normalized_key = normalize_stock_key_for_fast_query(stock_key)
    if not normalized_key:
        return pd.DataFrame(), pd.DataFrame()
    limit = max(1, int(per_source_limit or FAST_STOCK_ASSERTION_LIMIT_PER_SOURCE))

    engine = ensure_turso_engine(db_url, auth_token)
    with turso_connect_autocommit(engine) as conn:
        assertion_cols = table_columns(conn, "assertions")
        selected_assertion_cols = [
            col for col in WANTED_TRADE_ASSERTION_COLUMNS if col in assertion_cols
        ]
        if not selected_assertion_cols:
            return pd.DataFrame(), pd.DataFrame()

        base_query = build_assertions_query(selected_assertion_cols)
        code_clause = ""
        params: dict[str, object] = {"stock_key": normalized_key, "limit": limit}
        if stock_code and "stock_codes_json" in assertion_cols:
            code_clause = " OR stock_codes_json LIKE :stock_code_like"
            params["stock_code_like"] = f"%{stock_code}%"
        order_clause = (
            " ORDER BY created_at DESC" if "created_at" in assertion_cols else ""
        )
        assertions_query = (
            f"{base_query}\n"
            "WHERE action LIKE 'trade.%'\n"
            f"  AND (topic_key = :stock_key{code_clause})"
            f"{order_clause}\n"
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
                post_cols = table_columns(conn, "posts")
                display_expr = (
                    "display_md" if "display_md" in post_cols else "'' AS display_md"
                )
                selected_post_cols = [
                    col for col in WANTED_POST_COLUMNS_FOR_TREE if col in post_cols
                ]
                post_select_expr = ", ".join(selected_post_cols + [display_expr])
                placeholders = ", ".join(["?"] * len(post_uids))
                posts_query = f"""
SELECT {post_select_expr}
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
    "load_stock_sources_fast_from_env",
    "load_stock_trade_sources_fast_cached",
]
