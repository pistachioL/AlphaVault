from __future__ import annotations

# Shared helpers for source-schema reads in Reflex services.

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

from alphavault.domains.common.json_list import parse_json_list
from alphavault.domains.stock.key_match import is_stock_code_value, normalize_stock_code
from alphavault.domains.stock.keys import normalize_stock_key as _normalize_stock_key
from alphavault.domains.thread_tree.api import extract_platform_post_id


def normalize_posts_datetime(posts: pd.DataFrame) -> pd.DataFrame:
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


def normalize_assertions_datetime(assertions: pd.DataFrame) -> pd.DataFrame:
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


def ensure_platform_post_id(posts: pd.DataFrame) -> pd.DataFrame:
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


def normalize_stock_key_for_fast_query(stock_key: str) -> str:
    return _normalize_stock_key(stock_key)


def stock_code_from_stock_key(stock_key: str) -> str:
    normalized = normalize_stock_key_for_fast_query(stock_key)
    if not normalized.startswith("stock:"):
        return ""
    value = normalized[len("stock:") :].strip()
    code = normalize_stock_code(value)
    if not is_stock_code_value(code):
        return ""
    return code


__all__ = [
    "ensure_platform_post_id",
    "normalize_assertions_datetime",
    "normalize_posts_datetime",
    "normalize_stock_key_for_fast_query",
    "parse_json_list",
    "stock_code_from_stock_key",
]
