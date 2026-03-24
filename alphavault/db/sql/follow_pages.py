from __future__ import annotations


FOLLOW_KEYS_JSON_COLUMN = "follow_keys_json"


def create_follow_pages_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    page_key TEXT PRIMARY KEY,
    follow_type TEXT NOT NULL CHECK (follow_type IN ('topic', 'cluster')),
    follow_key TEXT NOT NULL,
    {FOLLOW_KEYS_JSON_COLUMN} TEXT NOT NULL DEFAULT '[]',
    page_name TEXT NOT NULL DEFAULT '',
    keywords_text TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def create_follow_pages_index(table: str) -> str:
    return f"""
CREATE INDEX IF NOT EXISTS idx_{table}_follow_type_key
    ON {table}(follow_type, follow_key);
"""


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


def add_follow_keys_json_column(table: str) -> str:
    return f"""
ALTER TABLE {table}
ADD COLUMN {FOLLOW_KEYS_JSON_COLUMN} TEXT NOT NULL DEFAULT '[]'
"""


__all__ = [
    "FOLLOW_KEYS_JSON_COLUMN",
    "add_follow_keys_json_column",
    "create_follow_pages_index",
    "create_follow_pages_table",
    "delete_follow_page",
    "select_follow_pages",
    "upsert_follow_page",
]
