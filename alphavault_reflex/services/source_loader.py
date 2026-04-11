from __future__ import annotations

from functools import lru_cache

import pandas as pd

from alphavault.constants import (
    ENV_POSTGRES_DSN,
    SCHEMA_WEIBO,
    SCHEMA_XUEQIU,
)
from alphavault.db.postgres_db import (
    ensure_postgres_engine,
    postgres_connect_autocommit,
    qualify_postgres_table,
)
from alphavault.db.postgres_env import (
    load_configured_postgres_sources_from_env,
    PostgresSource,
)
from alphavault.db.sql.ui import build_assertions_query
from alphavault.db.sql_df import read_sql_df
from alphavault.env import load_dotenv_if_present
from alphavault_reflex.services.source_read_utils import (
    ensure_platform_post_id,
    normalize_assertions_datetime,
    normalize_posts_datetime,
    parse_json_list,
)

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
_SOURCE_SCHEMA_NAMES = frozenset((SCHEMA_WEIBO, SCHEMA_XUEQIU))

MISSING_POSTGRES_DSN_ERROR = f"Missing {ENV_POSTGRES_DSN}"
SOURCE_SCHEMAS_EMPTY_ERROR = "source_schemas_empty"

WANTED_TRADE_ASSERTION_COLUMNS = [
    "post_uid",
    "idx",
    "entity_key",
    "action",
    "action_strength",
    "summary",
    "evidence",
    "confidence",
    "stock_codes",
    "stock_names",
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


def source_schema_name(source: object) -> str:
    if isinstance(source, str):
        return str(source or "").strip()
    schema_name = str(getattr(source, "schema", "") or "").strip()
    if schema_name:
        return schema_name
    return str(getattr(source, "name", "") or "").strip()


def source_table(source_name: str, table_name: str) -> str:
    return qualify_postgres_table(str(source_name or "").strip(), table_name)


def load_configured_source_schemas_from_env() -> list[PostgresSource]:
    return [
        source
        for source in load_configured_postgres_sources_from_env()
        if source_schema_name(source) in _SOURCE_SCHEMA_NAMES
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
        "entity_key": "",
        "action": "",
        "action_strength": 0,
        "summary": "",
        "evidence": "",
        "confidence": 0.0,
        "stock_codes": "[]",
        "stock_names": "[]",
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
    if not posts.empty and "post_uid" in posts.columns and "url" in posts.columns:
        url_map = posts.set_index("post_uid")["url"]
        assertions["url"] = assertions["post_uid"].map(url_map).fillna("")
    if not posts.empty and "post_uid" in posts.columns and "raw_text" in posts.columns:
        raw_map = posts.set_index("post_uid")["raw_text"]
        assertions["raw_text"] = assertions["post_uid"].map(raw_map).fillna("")

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

    assertions["stock_codes"] = assertions["stock_codes"].apply(parse_json_list)
    assertions["stock_names"] = assertions["stock_names"].apply(parse_json_list)
    assertions["industries"] = assertions["industries_json"].apply(parse_json_list)
    assertions["commodities"] = assertions["commodities_json"].apply(parse_json_list)
    assertions["indices"] = assertions["indices_json"].apply(parse_json_list)
    return assertions


@lru_cache(maxsize=2)
def load_trade_sources_cached(
    db_url: str, auth_token: str, source_name: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    del auth_token
    schema_name = source_schema_name(source_name)
    engine = ensure_postgres_engine(db_url, schema_name=schema_name)
    posts_table = source_table(schema_name, "posts")
    assertions_table = source_table(schema_name, "assertions")
    assertion_entities_table = source_table(schema_name, "assertion_entities")
    assertion_mentions_table = source_table(schema_name, "assertion_mentions")
    topic_cluster_topics_table = source_table(schema_name, "topic_cluster_topics")
    with postgres_connect_autocommit(engine) as conn:
        posts_query = f"""
SELECT {", ".join(WANTED_POST_COLUMNS_FOR_TREE)}
FROM {posts_table}
WHERE processed_at IS NOT NULL
"""
        base_query = build_assertions_query(
            WANTED_TRADE_ASSERTION_COLUMNS,
            posts_table=posts_table,
            assertions_table=assertions_table,
            assertion_entities_table=assertion_entities_table,
            assertion_mentions_table=assertion_mentions_table,
            topic_cluster_topics_table=topic_cluster_topics_table,
        )
        trade_query = f"{base_query} WHERE action LIKE 'trade.%'"

        posts = read_sql_df(conn, posts_query)
        assertions = read_sql_df(conn, trade_query)

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
    sources = load_configured_source_schemas_from_env()
    if not sources:
        return pd.DataFrame(), MISSING_POSTGRES_DSN_ERROR

    frames: list[pd.DataFrame] = []
    for source in sources:
        try:
            frames.append(load_cached_fn(source.url, source.token, source.name))
        except BaseException as err:
            if isinstance(err, _FATAL_BASE_EXCEPTIONS):
                raise
            return (
                pd.DataFrame(),
                f"postgres_connect_error:{source.name}:{type(err).__name__}",
            )

    if not frames:
        return pd.DataFrame(), SOURCE_SCHEMAS_EMPTY_ERROR
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
    "MISSING_POSTGRES_DSN_ERROR",
    "SOURCE_SCHEMAS_EMPTY_ERROR",
    "WANTED_POST_COLUMNS_FOR_TREE",
    "WANTED_TRADE_ASSERTION_COLUMNS",
    "load_configured_source_schemas_from_env",
    "load_sources_from_env",
    "load_trade_assertions_cached",
    "load_trade_assertions_from_env",
    "load_trade_sources_cached",
    "source_schema_name",
    "source_table",
    "standardize_assertions",
    "standardize_posts",
]

DEFAULT_FATAL_EXCEPTIONS = _FATAL_BASE_EXCEPTIONS
