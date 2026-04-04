from __future__ import annotations


def create_homework_trade_feed_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    view_key TEXT PRIMARY KEY,
    header_json TEXT NOT NULL DEFAULT '{{}}',
    items_json TEXT NOT NULL DEFAULT '[]',
    counters_json TEXT NOT NULL DEFAULT '{{}}',
    content_hash TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
)
"""


def create_homework_trade_feed_index(table: str) -> str:
    return f"""
CREATE INDEX IF NOT EXISTS idx_{table}_updated
ON {table}(updated_at)
"""


def upsert_homework_trade_feed(table: str) -> str:
    return f"""
INSERT INTO {table}(
    view_key,
    header_json,
    items_json,
    counters_json,
    content_hash,
    updated_at
)
VALUES(
    :view_key,
    :header_json,
    :items_json,
    :counters_json,
    :content_hash,
    :updated_at
)
ON CONFLICT(view_key) DO UPDATE SET
    header_json = excluded.header_json,
    items_json = excluded.items_json,
    counters_json = excluded.counters_json,
    content_hash = excluded.content_hash,
    updated_at = excluded.updated_at
"""


def select_homework_trade_feed(table: str) -> str:
    return f"""
SELECT view_key,
       header_json,
       items_json,
       counters_json,
       content_hash,
       updated_at
FROM {table}
WHERE view_key = :view_key
LIMIT 1
"""
