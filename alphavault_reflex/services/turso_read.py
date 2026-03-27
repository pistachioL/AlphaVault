from __future__ import annotations

from functools import lru_cache
import json

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

from alphavault.constants import (
    ENV_WEIBO_TURSO_DATABASE_URL,
    ENV_XUEQIU_TURSO_DATABASE_URL,
)
from alphavault.db.introspect import table_columns
from alphavault.db.sql.ui import build_assertions_query
from alphavault.db.turso_db import ensure_turso_engine, turso_connect_autocommit
from alphavault.db.turso_env import (
    infer_platform_from_post_uid,
    load_configured_turso_sources_from_env,
)
from alphavault.db.turso_pandas import turso_read_sql_df
from alphavault.env import load_dotenv_if_present
from alphavault.ui.thread_tree import extract_platform_post_id

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)

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


def _normalize_posts_datetime(posts: pd.DataFrame) -> pd.DataFrame:
    posts = posts.copy()
    for col in [
        "created_at",
        "ingested_at",
        "processed_at",
        "next_retry_at",
        "synced_at",
    ]:
        if col not in posts.columns:
            continue
        if is_datetime64_any_dtype(posts[col]):
            continue
        posts[col] = pd.to_datetime(posts[col], errors="coerce", utc=True)
        posts[col] = posts[col].dt.tz_convert(None)
    return posts


def _normalize_assertions_datetime(assertions: pd.DataFrame) -> pd.DataFrame:
    assertions = assertions.copy()
    if "created_at" not in assertions.columns:
        return assertions
    if is_datetime64_any_dtype(assertions["created_at"]):
        return assertions
    assertions["created_at"] = pd.to_datetime(
        assertions["created_at"], errors="coerce", utc=True
    )
    assertions["created_at"] = assertions["created_at"].dt.tz_convert(None)
    return assertions


def _standardize_posts(posts: pd.DataFrame, *, source_name: str) -> pd.DataFrame:
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


def _standardize_assertions(
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

    assertions["stock_codes"] = assertions["stock_codes_json"].apply(_parse_json_list)
    assertions["stock_names"] = assertions["stock_names_json"].apply(_parse_json_list)
    assertions["industries"] = assertions["industries_json"].apply(_parse_json_list)
    assertions["commodities"] = assertions["commodities_json"].apply(_parse_json_list)
    assertions["indices"] = assertions["indices_json"].apply(_parse_json_list)

    return assertions


def _parse_json_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    return [str(item).strip() for item in data if str(item).strip()]


@lru_cache(maxsize=2)
def _load_trade_sources_cached(
    db_url: str, auth_token: str, source_name: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    engine = ensure_turso_engine(db_url, auth_token)
    with turso_connect_autocommit(engine) as conn:
        post_cols = table_columns(conn, "posts")
        display_expr = "display_md" if "display_md" in post_cols else "'' AS display_md"
        selected_post_cols = [
            col
            for col in [
                "post_uid",
                "platform_post_id",
                "author",
                "created_at",
                "url",
                "raw_text",
            ]
            if col in post_cols
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

    posts = _standardize_posts(posts, source_name=source_name)
    posts = _normalize_posts_datetime(posts)
    posts = _ensure_platform_post_id(posts)
    assertions = _standardize_assertions(assertions, posts, source_name=source_name)
    assertions = _normalize_assertions_datetime(assertions)
    return posts, assertions


def _load_trade_assertions_cached(
    db_url: str, auth_token: str, source_name: str
) -> pd.DataFrame:
    _, assertions = _load_trade_sources_cached(db_url, auth_token, source_name)
    return assertions


@lru_cache(maxsize=2)
def _load_stock_alias_relations_cached(
    db_url: str, auth_token: str, source_name: str
) -> pd.DataFrame:
    engine = ensure_turso_engine(db_url, auth_token)
    sql = """
SELECT relation_type, left_key, right_key, relation_label, source, updated_at
FROM research_relations
WHERE relation_type = 'stock_alias' OR relation_label = 'alias_of'
"""
    with turso_connect_autocommit(engine) as conn:
        df = turso_read_sql_df(conn, sql)
    if df.empty:
        return df
    df = df.copy()
    df["db_source"] = source_name
    return df


def _ensure_platform_post_id(posts: pd.DataFrame) -> pd.DataFrame:
    if posts.empty:
        return posts
    if "post_uid" not in posts.columns:
        return posts
    posts = posts.copy()
    if "platform_post_id" not in posts.columns:
        posts["platform_post_id"] = posts["post_uid"].apply(extract_platform_post_id)
        return posts
    mask = posts["platform_post_id"].astype(str).str.strip().eq("")
    if not mask.any():
        return posts
    posts.loc[mask, "platform_post_id"] = posts.loc[mask, "post_uid"].apply(
        extract_platform_post_id
    )
    return posts


def _load_posts_for_tree_cached(
    db_url: str, auth_token: str, source_name: str
) -> pd.DataFrame:
    posts, _ = _load_trade_sources_cached(db_url, auth_token, source_name)
    return posts


def load_trade_assertions_from_env() -> tuple[pd.DataFrame, str]:
    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return (
            pd.DataFrame(),
            f"Missing {ENV_WEIBO_TURSO_DATABASE_URL} or {ENV_XUEQIU_TURSO_DATABASE_URL}",
        )

    frames: list[pd.DataFrame] = []
    for source in sources:
        try:
            frames.append(
                _load_trade_assertions_cached(source.url, source.token, source.name)
            )
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
            return (
                pd.DataFrame(),
                f"turso_connect_error:{source.name}:{type(e).__name__}",
            )

    if not frames:
        return pd.DataFrame(), "turso_sources_empty"
    return pd.concat(frames, ignore_index=True), ""


def load_posts_for_tree_from_env() -> tuple[pd.DataFrame, str]:
    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return (
            pd.DataFrame(),
            f"Missing {ENV_WEIBO_TURSO_DATABASE_URL} or {ENV_XUEQIU_TURSO_DATABASE_URL}",
        )

    frames: list[pd.DataFrame] = []
    for source in sources:
        try:
            frames.append(
                _load_posts_for_tree_cached(source.url, source.token, source.name)
            )
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
            return (
                pd.DataFrame(),
                f"turso_connect_error:{source.name}:{type(e).__name__}",
            )

    if not frames:
        return pd.DataFrame(), "turso_sources_empty"
    return pd.concat(frames, ignore_index=True), ""


def load_post_urls_from_env(post_uids: list[str]) -> tuple[dict[str, str], str]:
    cleaned = [str(uid or "").strip() for uid in (post_uids or [])]
    uids = tuple(sorted({uid for uid in cleaned if uid}))
    if not uids:
        return {}, ""

    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return (
            {},
            f"Missing {ENV_WEIBO_TURSO_DATABASE_URL} or {ENV_XUEQIU_TURSO_DATABASE_URL}",
        )

    sources_by_name = {s.name: s for s in sources}
    groups: dict[str, list[str]] = {}
    unknown: list[str] = []
    for uid in uids:
        platform = infer_platform_from_post_uid(uid)
        if platform and platform in sources_by_name:
            groups.setdefault(platform, []).append(uid)
        else:
            unknown.append(uid)

    out: dict[str, str] = {}
    try:
        for platform, group_uids in groups.items():
            source = sources_by_name[platform]
            out.update(
                _load_post_urls_cached(source.url, source.token, tuple(group_uids))
            )
        if unknown:
            for source in sources:
                out.update(
                    _load_post_urls_cached(source.url, source.token, tuple(unknown))
                )
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        return {}, f"turso_connect_error:{type(e).__name__}"

    return out, ""


@lru_cache(maxsize=32)
def _load_post_urls_cached(
    db_url: str, auth_token: str, post_uids: tuple[str, ...]
) -> dict[str, str]:
    if not post_uids:
        return {}
    engine = ensure_turso_engine(db_url, auth_token)
    placeholders = ", ".join(["?"] * len(post_uids))
    sql = f"SELECT post_uid, url FROM posts WHERE post_uid IN ({placeholders})"
    with turso_connect_autocommit(engine) as conn:
        df = turso_read_sql_df(conn, sql, params=list(post_uids))
    if df.empty or "post_uid" not in df.columns or "url" not in df.columns:
        return {}
    out: dict[str, str] = {}
    for _, row in df.iterrows():
        uid = str(row.get("post_uid") or "").strip()
        url = str(row.get("url") or "").strip()
        if uid and url:
            out[uid] = url
    return out


def load_sources_from_env() -> tuple[pd.DataFrame, pd.DataFrame, str]:
    load_dotenv_if_present()
    posts, posts_err = load_posts_for_tree_from_env()
    if posts_err:
        return pd.DataFrame(), pd.DataFrame(), posts_err
    assertions, assertions_err = load_trade_assertions_from_env()
    if assertions_err:
        return pd.DataFrame(), pd.DataFrame(), assertions_err
    return posts, assertions, ""


def load_stock_alias_relations_from_env() -> tuple[pd.DataFrame, str]:
    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return (
            pd.DataFrame(),
            f"Missing {ENV_WEIBO_TURSO_DATABASE_URL} or {ENV_XUEQIU_TURSO_DATABASE_URL}",
        )
    frames: list[pd.DataFrame] = []
    for source in sources:
        try:
            frames.append(
                _load_stock_alias_relations_cached(
                    source.url, source.token, source.name
                )
            )
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
            return (
                pd.DataFrame(),
                f"turso_connect_error:{source.name}:{type(e).__name__}",
            )

    if not frames:
        return pd.DataFrame(), "turso_sources_empty"
    return pd.concat(frames, ignore_index=True), ""


def clear_reflex_source_caches() -> None:
    _load_trade_sources_cached.cache_clear()
    _load_post_urls_cached.cache_clear()
    _load_stock_alias_relations_cached.cache_clear()
