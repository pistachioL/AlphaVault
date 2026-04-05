from __future__ import annotations


def upsert_stock_backfill_meta(table: str) -> str:
    return f"""
INSERT INTO {table}(stock_key, signature, row_count, updated_at)
VALUES(:stock_key, :signature, :row_count, :updated_at)
ON CONFLICT(stock_key) DO UPDATE SET
    signature = excluded.signature,
    row_count = excluded.row_count,
    updated_at = excluded.updated_at
"""


def select_stock_backfill_meta(table: str) -> str:
    return f"""
SELECT stock_key,
       signature,
       row_count,
       updated_at
FROM {table}
WHERE stock_key = :stock_key
LIMIT 1
"""


def upsert_stock_backfill_dirty_key(table: str) -> str:
    return f"""
INSERT INTO {table}(stock_key, reason, updated_at)
VALUES(:stock_key, :reason, :updated_at)
ON CONFLICT(stock_key) DO UPDATE SET
    reason = excluded.reason,
    updated_at = excluded.updated_at
"""


def select_stock_backfill_dirty_keys(table: str) -> str:
    return f"""
SELECT stock_key,
       reason,
       updated_at
FROM {table}
ORDER BY updated_at ASC, stock_key ASC
LIMIT :limit
"""


def delete_stock_backfill_posts(table: str) -> str:
    return f"DELETE FROM {table} WHERE stock_key = :stock_key"


def insert_stock_backfill_posts(table: str) -> str:
    return f"""
INSERT INTO {table}(
    stock_key,
    post_uid,
    author,
    created_at,
    url,
    matched_terms,
    preview,
    tree_text,
    updated_at
)
VALUES (
    :stock_key,
    :post_uid,
    :author,
    :created_at,
    :url,
    :matched_terms,
    :preview,
    :tree_text,
    :updated_at
)
"""


def select_stock_backfill_posts(table: str) -> str:
    return f"""
SELECT post_uid, author, created_at, url, matched_terms, preview, tree_text, updated_at
FROM {table}
WHERE stock_key = :stock_key
ORDER BY created_at DESC, post_uid DESC
LIMIT :limit
"""
