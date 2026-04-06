from __future__ import annotations


FOLLOW_KEYS_JSON_COLUMN = "follow_keys_json"


def select_follow_pages(table: str) -> str:
    return f"""
SELECT page_key, follow_type, follow_key, {FOLLOW_KEYS_JSON_COLUMN}, page_name, keywords_text, created_at, updated_at
FROM {table}
"""


def upsert_follow_page(table: str) -> str:
    return f"""
INSERT INTO {table}(
    page_key,
    follow_type,
    follow_key,
    {FOLLOW_KEYS_JSON_COLUMN},
    page_name,
    keywords_text,
    created_at,
    updated_at
)
VALUES (:page_key, :follow_type, :follow_key, :follow_keys_json, :page_name, :keywords_text, :now, :now)
ON CONFLICT(page_key) DO UPDATE SET
    {FOLLOW_KEYS_JSON_COLUMN} = excluded.{FOLLOW_KEYS_JSON_COLUMN},
    page_name = excluded.page_name,
    keywords_text = excluded.keywords_text,
    updated_at = excluded.updated_at
"""


def delete_follow_page(table: str) -> str:
    return f"""
DELETE FROM {table}
WHERE page_key = :page_key
"""


__all__ = [
    "FOLLOW_KEYS_JSON_COLUMN",
    "delete_follow_page",
    "select_follow_pages",
    "upsert_follow_page",
]
