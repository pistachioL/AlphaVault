from __future__ import annotations

# Shared helpers for source-schema reads in Reflex services.

from datetime import datetime, timezone

from alphavault.domains.common.json_list import parse_json_list
from alphavault.domains.stock.key_match import is_stock_code_value, normalize_stock_code
from alphavault.domains.stock.keys import normalize_stock_key as _normalize_stock_key
from alphavault.domains.thread_tree.api import extract_platform_post_id


def normalize_datetime_value(value: object) -> datetime | str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    text = str(value or "").strip()
    if not text:
        return ""
    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return text
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def normalize_posts_datetime_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    target_columns = {
        "created_at",
        "ingested_at",
        "processed_at",
        "next_retry_at",
        "synced_at",
    }
    for raw_row in rows:
        row = dict(raw_row)
        for col in target_columns:
            if col in row:
                row[col] = normalize_datetime_value(row.get(col))
        normalized.append(row)
    return normalized


def normalize_assertions_datetime_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for raw_row in rows:
        row = dict(raw_row)
        if "created_at" in row:
            row["created_at"] = normalize_datetime_value(row.get("created_at"))
        normalized.append(row)
    return normalized


def ensure_platform_post_id_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for raw_row in rows:
        row = dict(raw_row)
        post_uid = str(row.get("post_uid") or "").strip()
        platform_post_id = str(row.get("platform_post_id") or "").strip()
        if post_uid and not platform_post_id:
            row["platform_post_id"] = extract_platform_post_id(post_uid)
        normalized.append(row)
    return normalized


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
    "ensure_platform_post_id_rows",
    "normalize_assertions_datetime_rows",
    "normalize_datetime_value",
    "normalize_posts_datetime_rows",
    "normalize_stock_key_for_fast_query",
    "parse_json_list",
    "stock_code_from_stock_key",
]
