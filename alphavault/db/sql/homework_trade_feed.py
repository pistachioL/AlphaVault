from __future__ import annotations


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
