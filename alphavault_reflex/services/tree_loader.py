from __future__ import annotations

from functools import lru_cache

import pandas as pd

from alphavault.db.introspect import table_columns
from alphavault.db.turso_db import ensure_turso_engine, turso_connect_autocommit
from alphavault.db.turso_env import (
    infer_platform_from_post_uid,
    load_configured_turso_sources_from_env,
)
from alphavault.db.turso_pandas import turso_read_sql_df
from alphavault.env import load_dotenv_if_present
from alphavault_reflex.services.source_loader import (
    DEFAULT_FATAL_EXCEPTIONS,
    MISSING_TURSO_SOURCES_ERROR,
    WANTED_POST_COLUMNS_FOR_TREE,
    load_trade_sources_cached,
    standardize_posts,
)
from alphavault.domains.thread_tree.service import normalize_tree_lookup_post_uid
from alphavault_reflex.services.turso_read_utils import (
    ensure_platform_post_id,
    normalize_posts_datetime,
)


def load_posts_for_tree_cached(
    db_url: str, auth_token: str, source_name: str
) -> pd.DataFrame:
    posts, _ = load_trade_sources_cached(db_url, auth_token, source_name)
    return posts


@lru_cache(maxsize=64)
def load_single_post_for_tree_cached(
    db_url: str, auth_token: str, source_name: str, post_uid: str
) -> pd.DataFrame:
    uid = normalize_tree_lookup_post_uid(post_uid)
    if not uid:
        return pd.DataFrame()

    engine = ensure_turso_engine(db_url, auth_token)
    with turso_connect_autocommit(engine) as conn:
        post_cols = table_columns(conn, "posts")
        display_expr = "display_md" if "display_md" in post_cols else "'' AS display_md"
        selected_post_cols = [
            col for col in WANTED_POST_COLUMNS_FOR_TREE if col in post_cols
        ]
        post_select_expr = ", ".join(selected_post_cols + [display_expr])
        sql = f"""
SELECT {post_select_expr}
FROM posts
WHERE processed_at IS NOT NULL AND post_uid = ?
"""
        posts = turso_read_sql_df(conn, sql, params=[uid])

    posts = standardize_posts(posts, source_name=source_name)
    posts = normalize_posts_datetime(posts)
    posts = ensure_platform_post_id(posts)
    return posts


def load_single_post_for_tree_from_env(
    post_uid: str,
    *,
    load_cached_fn=load_single_post_for_tree_cached,
) -> tuple[pd.DataFrame, str]:
    uid = normalize_tree_lookup_post_uid(post_uid)
    if not uid:
        return pd.DataFrame(), ""

    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return pd.DataFrame(), MISSING_TURSO_SOURCES_ERROR

    sources_by_name = {source.name: source for source in sources}
    platform = infer_platform_from_post_uid(uid)
    if platform and platform in sources_by_name:
        sources = [sources_by_name[platform]]

    errors: list[str] = []
    for source in sources:
        try:
            posts = load_cached_fn(source.url, source.token, source.name, uid)
        except BaseException as err:
            if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                raise
            errors.append(f"turso_connect_error:{source.name}:{type(err).__name__}")
            continue
        if not posts.empty:
            return posts, ""

    if errors:
        return pd.DataFrame(), errors[0]
    return pd.DataFrame(), ""


def load_posts_for_tree_from_env(
    *,
    load_cached_fn=load_posts_for_tree_cached,
) -> tuple[pd.DataFrame, str]:
    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return pd.DataFrame(), MISSING_TURSO_SOURCES_ERROR

    frames: list[pd.DataFrame] = []
    for source in sources:
        try:
            frames.append(load_cached_fn(source.url, source.token, source.name))
        except BaseException as err:
            if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                raise
            return (
                pd.DataFrame(),
                f"turso_connect_error:{source.name}:{type(err).__name__}",
            )

    if not frames:
        return pd.DataFrame(), "turso_sources_empty"
    return pd.concat(frames, ignore_index=True), ""


__all__ = [
    "load_posts_for_tree_cached",
    "load_posts_for_tree_from_env",
    "load_single_post_for_tree_cached",
    "load_single_post_for_tree_from_env",
]
