from __future__ import annotations

from functools import lru_cache

import pandas as pd

from alphavault.constants import (
    ENV_WEIBO_TURSO_DATABASE_URL,
    ENV_XUEQIU_TURSO_DATABASE_URL,
)
from alphavault.db.introspect import table_columns
from alphavault.db.sql.ui import build_assertions_query
from alphavault.db.turso_db import ensure_turso_engine, turso_connect_autocommit
from alphavault.db.turso_env import load_configured_turso_sources_from_env
from alphavault.db.turso_pandas import turso_read_sql_df
from alphavault.env import load_dotenv_if_present
from alphavault_reflex.services.turso_read_utils import (
    ensure_platform_post_id,
    normalize_assertions_datetime,
    normalize_posts_datetime,
    parse_json_list,
)

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)

MISSING_TURSO_SOURCES_ERROR = (
    f"Missing {ENV_WEIBO_TURSO_DATABASE_URL} or {ENV_XUEQIU_TURSO_DATABASE_URL}"
)

WANTED_TRADE_ASSERTION_COLUMNS = [
    "post_uid",
    "idx",
    "topic_key",
    "action",
    "action_strength",
    "summary",
    "evidence",
    "confidence",
    "stock_codes_json",
    "stock_names_json",
    "industries_json",
    "commodities_json",
    "indices_json",
    "author",
    "created_at",
]

WANTED_POST_COLUMNS_FOR_TREE = [
    "post_uid",
    "platform_post_id",
    "author",
    "created_at",
    "url",
    "raw_text",
]


def standardize_posts(posts: pd.DataFrame, *, source_name: str) -> pd.DataFrame:
    posts = posts.copy()
    defaults: dict[str, object] = {
        "post_uid": "",
        "platform": source_name,
        "platform_post_id": "",
        "author": "",
        "created_at": "",
        "url": "",
        "raw_text": "",
        "display_md": "",
        "status": "",
        "invest_score": 0.0,
        "processed_at": "",
    }
    for col, default in defaults.items():
        if col not in posts.columns:
            posts[col] = default
    if "platform" in posts.columns:
        posts["platform"] = posts["platform"].replace("", pd.NA)
        posts["platform"] = posts["platform"].fillna(source_name)
    posts["source"] = source_name
    return posts


def standardize_assertions(
    assertions: pd.DataFrame,
    posts: pd.DataFrame,
    *,
    source_name: str,
) -> pd.DataFrame:
    assertions = assertions.copy()
    defaults: dict[str, object] = {
        "post_uid": "",
        "idx": 0,
        "topic_key": "",
        "action": "",
        "action_strength": 0,
        "summary": "",
        "evidence": "",
        "confidence": 0.0,
        "stock_codes_json": "[]",
        "stock_names_json": "[]",
        "industries_json": "[]",
        "commodities_json": "[]",
        "indices_json": "[]",
        "author": "",
        "created_at": "",
    }
    for col, default in defaults.items():
        if col not in assertions.columns:
            assertions[col] = default

    assertions["source"] = source_name

    assertions["url"] = ""
    assertions["raw_text"] = ""
    assertions["display_md"] = ""
    if not posts.empty and "post_uid" in posts.columns and "url" in posts.columns:
        url_map = posts.set_index("post_uid")["url"]
        assertions["url"] = assertions["post_uid"].map(url_map).fillna("")
    if not posts.empty and "post_uid" in posts.columns and "raw_text" in posts.columns:
        raw_map = posts.set_index("post_uid")["raw_text"]
        assertions["raw_text"] = assertions["post_uid"].map(raw_map).fillna("")
    if (
        not posts.empty
        and "post_uid" in posts.columns
        and "display_md" in posts.columns
    ):
        display_map = posts.set_index("post_uid")["display_md"]
        assertions["display_md"] = assertions["post_uid"].map(display_map).fillna("")

    if "author" in assertions.columns:
        missing_author = assertions["author"].eq("") | assertions["author"].isna()
    else:
        missing_author = pd.Series([True] * len(assertions))
    if "created_at" in assertions.columns:
        missing_created = (
            assertions["created_at"].eq("") | assertions["created_at"].isna()
        )
    else:
        missing_created = pd.Series([True] * len(assertions))

    if not posts.empty:
        author_map = posts.set_index("post_uid")["author"]
        created_map = posts.set_index("post_uid")["created_at"]
        assertions.loc[missing_author, "author"] = assertions.loc[
            missing_author, "post_uid"
        ].map(author_map)
        assertions.loc[missing_created, "created_at"] = assertions.loc[
            missing_created, "post_uid"
        ].map(created_map)

    assertions["stock_codes"] = assertions["stock_codes_json"].apply(parse_json_list)
    assertions["stock_names"] = assertions["stock_names_json"].apply(parse_json_list)
    assertions["industries"] = assertions["industries_json"].apply(parse_json_list)
    assertions["commodities"] = assertions["commodities_json"].apply(parse_json_list)
    assertions["indices"] = assertions["indices_json"].apply(parse_json_list)
    return assertions


@lru_cache(maxsize=2)
def load_trade_sources_cached(
    db_url: str, auth_token: str, source_name: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    engine = ensure_turso_engine(db_url, auth_token)
    with turso_connect_autocommit(engine) as conn:
        post_cols = table_columns(conn, "posts")
        display_expr = "display_md" if "display_md" in post_cols else "'' AS display_md"
        selected_post_cols = [
            col for col in WANTED_POST_COLUMNS_FOR_TREE if col in post_cols
        ]
        post_select_expr = ", ".join(selected_post_cols + [display_expr])
        posts_query = f"""
SELECT {post_select_expr}
FROM posts
WHERE processed_at IS NOT NULL
"""

        assertion_cols = table_columns(conn, "assertions")
        selected_assertion_cols = [
            col for col in WANTED_TRADE_ASSERTION_COLUMNS if col in assertion_cols
        ]
        base_query = build_assertions_query(selected_assertion_cols)
        trade_query = f"{base_query} WHERE action LIKE 'trade.%'"

        posts = turso_read_sql_df(conn, posts_query)
        assertions = turso_read_sql_df(conn, trade_query)

    posts = standardize_posts(posts, source_name=source_name)
    posts = normalize_posts_datetime(posts)
    posts = ensure_platform_post_id(posts)
    assertions = standardize_assertions(assertions, posts, source_name=source_name)
    assertions = normalize_assertions_datetime(assertions)
    return posts, assertions


def load_trade_assertions_cached(
    db_url: str, auth_token: str, source_name: str
) -> pd.DataFrame:
    _, assertions = load_trade_sources_cached(db_url, auth_token, source_name)
    return assertions


def load_trade_assertions_from_env(
    *,
    load_cached_fn=load_trade_assertions_cached,
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
            if isinstance(err, _FATAL_BASE_EXCEPTIONS):
                raise
            return (
                pd.DataFrame(),
                f"turso_connect_error:{source.name}:{type(err).__name__}",
            )

    if not frames:
        return pd.DataFrame(), "turso_sources_empty"
    return pd.concat(frames, ignore_index=True), ""


def load_sources_from_env(
    *,
    load_posts_for_tree_from_env_fn,
    load_trade_assertions_from_env_fn=load_trade_assertions_from_env,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    load_dotenv_if_present()
    posts, posts_err = load_posts_for_tree_from_env_fn()
    if posts_err:
        return pd.DataFrame(), pd.DataFrame(), posts_err
    assertions, assertions_err = load_trade_assertions_from_env_fn()
    if assertions_err:
        return pd.DataFrame(), pd.DataFrame(), assertions_err
    return posts, assertions, ""


__all__ = [
    "DEFAULT_FATAL_EXCEPTIONS",
    "MISSING_TURSO_SOURCES_ERROR",
    "WANTED_POST_COLUMNS_FOR_TREE",
    "WANTED_TRADE_ASSERTION_COLUMNS",
    "load_sources_from_env",
    "load_trade_assertions_cached",
    "load_trade_assertions_from_env",
    "load_trade_sources_cached",
    "standardize_assertions",
    "standardize_posts",
]

DEFAULT_FATAL_EXCEPTIONS = _FATAL_BASE_EXCEPTIONS
