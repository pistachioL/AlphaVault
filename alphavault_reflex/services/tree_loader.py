from __future__ import annotations

from functools import lru_cache

from alphavault.constants import SCHEMA_WEIBO, SCHEMA_XUEQIU
from alphavault.db.postgres_db import (
    ensure_postgres_engine,
    postgres_connect_autocommit,
)
from alphavault.db.postgres_env import (
    load_configured_postgres_sources_from_env,
    PostgresSource,
)
from alphavault.db.sql_rows import read_sql_rows
from alphavault.db.postgres_env import (
    infer_platform_from_post_uid,
)
from alphavault.env import load_dotenv_if_present
from alphavault_reflex.services.source_loader import (
    DEFAULT_FATAL_EXCEPTIONS,
    MISSING_POSTGRES_DSN_ERROR,
    SOURCE_SCHEMAS_EMPTY_ERROR,
    WANTED_POST_COLUMNS_FOR_TREE,
    load_trade_sources_cached,
    source_schema_name,
    source_table,
    standardize_posts_rows,
)
from alphavault.domains.thread_tree.service import normalize_tree_lookup_post_uid
from alphavault_reflex.services.source_read_utils import (
    ensure_platform_post_id_rows,
    normalize_posts_datetime_rows,
)

_SOURCE_SCHEMA_NAMES = frozenset((SCHEMA_WEIBO, SCHEMA_XUEQIU))


def _load_source_schemas_from_env() -> list[PostgresSource]:
    return [
        source
        for source in load_configured_postgres_sources_from_env()
        if source_schema_name(source) in _SOURCE_SCHEMA_NAMES
    ]


def load_posts_for_tree_cached(
    db_url: str, auth_token: str, source_name: str
) -> tuple[dict[str, object], ...]:
    posts, _ = load_trade_sources_cached(db_url, auth_token, source_name)
    return posts


@lru_cache(maxsize=64)
def load_single_post_for_tree_cached(
    db_url: str, auth_token: str, source_name: str, post_uid: str
) -> tuple[dict[str, object], ...]:
    uid = normalize_tree_lookup_post_uid(post_uid)
    if not uid:
        return ()

    del auth_token
    schema_name = source_schema_name(source_name)
    engine = ensure_postgres_engine(db_url, schema_name=schema_name)
    with postgres_connect_autocommit(engine) as conn:
        sql = f"""
SELECT {", ".join(WANTED_POST_COLUMNS_FOR_TREE)}
FROM {source_table(schema_name, "posts")}
WHERE processed_at IS NOT NULL AND post_uid = ?
"""
        rows = read_sql_rows(conn, sql, params=[uid])

    posts = standardize_posts_rows(rows, source_name=source_name)
    posts = normalize_posts_datetime_rows(posts)
    posts = ensure_platform_post_id_rows(posts)
    return tuple(dict(row) for row in posts)


def load_single_post_for_tree_from_env(
    post_uid: str,
    *,
    load_cached_fn=load_single_post_for_tree_cached,
) -> tuple[list[dict[str, object]], str]:
    uid = normalize_tree_lookup_post_uid(post_uid)
    if not uid:
        return [], ""

    load_dotenv_if_present()
    sources = _load_source_schemas_from_env()
    if not sources:
        return [], MISSING_POSTGRES_DSN_ERROR

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
            errors.append(f"postgres_connect_error:{source.name}:{type(err).__name__}")
            continue
        if posts:
            return [dict(row) for row in posts], ""

    if errors:
        return [], errors[0]
    return [], ""


def load_posts_for_tree_from_env(
    *,
    load_cached_fn=load_posts_for_tree_cached,
) -> tuple[list[dict[str, object]], str]:
    load_dotenv_if_present()
    sources = _load_source_schemas_from_env()
    if not sources:
        return [], MISSING_POSTGRES_DSN_ERROR

    rows: list[dict[str, object]] = []
    for source in sources:
        try:
            rows.extend(
                dict(row)
                for row in load_cached_fn(source.url, source.token, source.name)
            )
        except BaseException as err:
            if isinstance(err, DEFAULT_FATAL_EXCEPTIONS):
                raise
            return [], f"postgres_connect_error:{source.name}:{type(err).__name__}"

    if not rows:
        return [], SOURCE_SCHEMAS_EMPTY_ERROR
    return rows, ""


__all__ = [
    "load_posts_for_tree_cached",
    "load_posts_for_tree_from_env",
    "load_single_post_for_tree_cached",
    "load_single_post_for_tree_from_env",
]
