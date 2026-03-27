from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from datetime import datetime, timedelta
from functools import lru_cache
import json
import logging
import os
import time

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
from alphavault.ui.follow_pages_key_match import (
    is_stock_code_value,
    normalize_stock_code,
)
from alphavault.ui.thread_tree import extract_platform_post_id
from alphavault_reflex.services.homework_constants import TRADE_BOARD_MAX_WINDOW_DAYS
from alphavault_reflex.services.thread_tree import normalize_tree_lookup_post_uid

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
_logger = logging.getLogger(__name__)
ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS = "REFLEX_HOMEWORK_SOURCE_MAX_WORKERS"
DEFAULT_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS = 2

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

TRADE_BOARD_ASSERTION_COLUMNS = [
    "post_uid",
    "idx",
    "topic_key",
    "action",
    "action_strength",
    "summary",
    "stock_codes_json",
    "stock_names_json",
]

WANTED_POST_COLUMNS_FOR_TREE = [
    "post_uid",
    "platform_post_id",
    "author",
    "created_at",
    "url",
    "raw_text",
]
FAST_STOCK_ASSERTION_LIMIT_PER_SOURCE = 240
FAST_STOCK_TOTAL_TIMEOUT_SECONDS = 8.0

STOCK_ALIAS_RELATIONS_SQL = """
SELECT relation_type, left_key, right_key, relation_label, source, updated_at
FROM research_relations
WHERE relation_type = 'stock_alias' OR relation_label = 'alias_of'
"""


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


def _trade_board_cutoff_from_utc_now(*, lookback_days: int) -> str:
    days = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))
    now = datetime.utcnow()
    cutoff_day = now.date() - timedelta(days=max(0, int(days) - 1))
    cutoff = datetime.combine(cutoff_day, datetime.min.time())
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


def _trade_board_select_expr() -> str:
    return ", ".join(
        [f"a.{col} AS {col}" for col in TRADE_BOARD_ASSERTION_COLUMNS]
        + [
            "p.author AS author",
            "p.created_at AS created_at",
            "p.url AS url",
        ]
    )


def _query_trade_board_assertions(
    *, conn: object, cutoff: str, source_name: str
) -> pd.DataFrame:
    sql = f"""
SELECT {_trade_board_select_expr()}
FROM posts p
JOIN assertions a ON a.post_uid = p.post_uid
WHERE p.processed_at IS NOT NULL
  AND p.created_at >= :cutoff
  AND a.action LIKE 'trade.%'
"""
    df = turso_read_sql_df(conn, sql, params={"cutoff": cutoff})
    if df.empty:
        return df
    df = df.copy()
    df["source"] = str(source_name or "").strip()
    for col in ["post_uid", "topic_key", "action", "summary", "author", "url"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    df = _normalize_assertions_datetime(df)
    return df


def _query_stock_alias_relations(*, conn: object, source_name: str) -> pd.DataFrame:
    df = turso_read_sql_df(conn, STOCK_ALIAS_RELATIONS_SQL)
    if df.empty:
        return df
    df = df.copy()
    df["db_source"] = source_name
    return df


@lru_cache(maxsize=8)
def _load_trade_board_assertions_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    lookback_days: int,
) -> pd.DataFrame:
    lookback = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))
    cutoff = _trade_board_cutoff_from_utc_now(lookback_days=lookback)
    engine = ensure_turso_engine(db_url, auth_token)
    with turso_connect_autocommit(engine) as conn:
        return _query_trade_board_assertions(
            conn=conn,
            cutoff=cutoff,
            source_name=source_name,
        )


def load_trade_board_assertions_from_env(
    lookback_days: int,
) -> tuple[pd.DataFrame, str]:
    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return pd.DataFrame(), MISSING_TURSO_SOURCES_ERROR

    lookback = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))

    frames: list[pd.DataFrame] = []
    max_workers = max(1, min(4, int(len(sources))))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _load_trade_board_assertions_cached,
                source.url,
                source.token,
                source.name,
                lookback,
            ): source.name
            for source in sources
        }
        for fut in as_completed(futures):
            name = futures.get(fut, "")
            try:
                frames.append(fut.result())
            except BaseException as e:
                if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                    raise
                return (
                    pd.DataFrame(),
                    f"turso_connect_error:{name}:{type(e).__name__}",
                )

    if not frames:
        return pd.DataFrame(), "turso_sources_empty"
    return pd.concat(frames, ignore_index=True), ""


@lru_cache(maxsize=2)
def _load_stock_alias_relations_cached(
    db_url: str, auth_token: str, source_name: str
) -> pd.DataFrame:
    engine = ensure_turso_engine(db_url, auth_token)
    with turso_connect_autocommit(engine) as conn:
        return _query_stock_alias_relations(
            conn=conn,
            source_name=source_name,
        )


def _resolve_homework_source_workers(*, source_count: int) -> int:
    total = max(1, int(source_count or 1))
    raw = os.getenv(ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "").strip()
    if not raw:
        wanted = int(DEFAULT_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS)
    else:
        try:
            wanted = int(raw)
        except ValueError:
            wanted = int(DEFAULT_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS)
    return max(1, min(int(wanted), total))


@lru_cache(maxsize=8)
def _load_homework_board_payload_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    lookback_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    start = time.perf_counter()
    lookback = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))
    cutoff = _trade_board_cutoff_from_utc_now(lookback_days=lookback)
    engine = ensure_turso_engine(db_url, auth_token)
    assertion_count = 0
    relation_count = 0
    with turso_connect_autocommit(engine) as conn:
        assertions = _query_trade_board_assertions(
            conn=conn,
            cutoff=cutoff,
            source_name=source_name,
        )
        assertion_count = int(len(assertions))
        try:
            relations = _query_stock_alias_relations(
                conn=conn,
                source_name=source_name,
            )
            relation_count = int(len(relations))
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
            _logger.debug(
                "homework_payload relation_query_failed source=%s err=%s",
                source_name,
                type(e).__name__,
            )
            relations = pd.DataFrame()
    _logger.debug(
        "homework_payload source_query source=%s lookback_days=%d assertions=%d relations=%d elapsed=%.3fs",
        source_name,
        int(lookback),
        assertion_count,
        relation_count,
        time.perf_counter() - start,
    )
    return assertions, relations


def load_homework_board_payload_from_env(
    lookback_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    start = time.perf_counter()
    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return pd.DataFrame(), pd.DataFrame(), MISSING_TURSO_SOURCES_ERROR

    lookback = max(1, min(int(lookback_days or 1), TRADE_BOARD_MAX_WINDOW_DAYS))
    assertions_frames: list[pd.DataFrame] = []
    relation_frames: list[pd.DataFrame] = []

    max_workers = _resolve_homework_source_workers(source_count=len(sources))
    if max_workers == 1:
        for source in sources:
            source_start = time.perf_counter()
            try:
                assertions, relations = _load_homework_board_payload_cached(
                    source.url,
                    source.token,
                    source.name,
                    lookback,
                )
            except BaseException as e:
                if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                    raise
                return (
                    pd.DataFrame(),
                    pd.DataFrame(),
                    f"turso_connect_error:{source.name}:{type(e).__name__}",
                )
            assertions_frames.append(assertions)
            relation_frames.append(relations)
            _logger.debug(
                "homework_payload source_done source=%s mode=serial assertions=%d relations=%d elapsed=%.3fs",
                source.name,
                int(len(assertions)),
                int(len(relations)),
                time.perf_counter() - source_start,
            )
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {}
            for source in sources:
                futures[
                    pool.submit(
                        _load_homework_board_payload_cached,
                        source.url,
                        source.token,
                        source.name,
                        lookback,
                    )
                ] = (source.name, time.perf_counter())
            for fut in as_completed(futures):
                name, source_start = futures.get(fut, ("", time.perf_counter()))
                try:
                    assertions, relations = fut.result()
                except BaseException as e:
                    if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                        raise
                    return (
                        pd.DataFrame(),
                        pd.DataFrame(),
                        f"turso_connect_error:{name}:{type(e).__name__}",
                    )
                assertions_frames.append(assertions)
                relation_frames.append(relations)
                _logger.debug(
                    "homework_payload source_done source=%s mode=parallel assertions=%d relations=%d elapsed=%.3fs",
                    name,
                    int(len(assertions)),
                    int(len(relations)),
                    time.perf_counter() - source_start,
                )

    if not assertions_frames:
        return pd.DataFrame(), pd.DataFrame(), "turso_sources_empty"
    assertions_all = pd.concat(assertions_frames, ignore_index=True)
    relations_all = pd.concat(relation_frames, ignore_index=True)
    _logger.debug(
        "homework_payload done sources=%d workers=%d assertions=%d relations=%d elapsed=%.3fs",
        int(len(sources)),
        int(max_workers),
        int(len(assertions_all)),
        int(len(relations_all)),
        time.perf_counter() - start,
    )
    return assertions_all, relations_all, ""


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


@lru_cache(maxsize=64)
def _load_single_post_for_tree_cached(
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

    posts = _standardize_posts(posts, source_name=source_name)
    posts = _normalize_posts_datetime(posts)
    posts = _ensure_platform_post_id(posts)
    return posts


def load_single_post_for_tree_from_env(post_uid: str) -> tuple[pd.DataFrame, str]:
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
            posts = _load_single_post_for_tree_cached(
                source.url, source.token, source.name, uid
            )
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
            errors.append(f"turso_connect_error:{source.name}:{type(e).__name__}")
            continue
        if not posts.empty:
            return posts, ""

    if errors:
        return pd.DataFrame(), errors[0]
    return pd.DataFrame(), ""


def _normalize_stock_key_for_fast_query(stock_key: str) -> str:
    raw = str(stock_key or "").strip()
    if not raw:
        return ""
    if raw.startswith("stock:"):
        return raw
    return f"stock:{raw}"


def _stock_code_from_stock_key(stock_key: str) -> str:
    normalized = _normalize_stock_key_for_fast_query(stock_key)
    if not normalized.startswith("stock:"):
        return ""
    value = normalized[len("stock:") :].strip()
    code = normalize_stock_code(value)
    if not is_stock_code_value(code):
        return ""
    return code


@lru_cache(maxsize=64)
def _load_stock_trade_sources_fast_cached(
    db_url: str,
    auth_token: str,
    source_name: str,
    stock_key: str,
    stock_code: str,
    per_source_limit: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    normalized_key = _normalize_stock_key_for_fast_query(stock_key)
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

    posts = _standardize_posts(posts, source_name=source_name)
    posts = _normalize_posts_datetime(posts)
    posts = _ensure_platform_post_id(posts)
    assertions = _standardize_assertions(assertions, posts, source_name=source_name)
    assertions = _normalize_assertions_datetime(assertions)
    return posts, assertions


def load_stock_sources_fast_from_env(
    stock_key: str,
    *,
    per_source_limit: int = FAST_STOCK_ASSERTION_LIMIT_PER_SOURCE,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    normalized_key = _normalize_stock_key_for_fast_query(stock_key)
    if not normalized_key:
        return pd.DataFrame(), pd.DataFrame(), ""

    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return pd.DataFrame(), pd.DataFrame(), MISSING_TURSO_SOURCES_ERROR

    stock_code = _stock_code_from_stock_key(normalized_key)
    limit = max(1, int(per_source_limit or FAST_STOCK_ASSERTION_LIMIT_PER_SOURCE))

    posts_frames: list[pd.DataFrame] = []
    assertions_frames: list[pd.DataFrame] = []
    errors: list[str] = []
    max_workers = max(1, min(4, int(len(sources))))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _load_stock_trade_sources_fast_cached,
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
            except BaseException as e:
                if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                    raise
                errors.append(f"turso_connect_error:{source_name}:{type(e).__name__}")
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


def load_trade_assertions_from_env() -> tuple[pd.DataFrame, str]:
    load_dotenv_if_present()
    sources = load_configured_turso_sources_from_env()
    if not sources:
        return pd.DataFrame(), MISSING_TURSO_SOURCES_ERROR

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
        return pd.DataFrame(), MISSING_TURSO_SOURCES_ERROR

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
        return {}, MISSING_TURSO_SOURCES_ERROR

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
        return pd.DataFrame(), MISSING_TURSO_SOURCES_ERROR
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
    _load_trade_board_assertions_cached.cache_clear()
    _load_stock_trade_sources_fast_cached.cache_clear()
    _load_homework_board_payload_cached.cache_clear()
    _load_single_post_for_tree_cached.cache_clear()
    _load_post_urls_cached.cache_clear()
    _load_stock_alias_relations_cached.cache_clear()
    try:
        from alphavault_reflex.services.stock_hot_read import (
            clear_stock_hot_read_caches,
        )

        clear_stock_hot_read_caches()
    except Exception:
        pass
