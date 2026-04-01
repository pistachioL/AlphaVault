from __future__ import annotations


def create_research_stock_backfill_posts_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    stock_key TEXT NOT NULL,
    post_uid TEXT NOT NULL,
    author TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    matched_terms TEXT NOT NULL DEFAULT '',
    preview TEXT NOT NULL DEFAULT '',
    tree_text TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    PRIMARY KEY(stock_key, post_uid)
)
"""


def create_research_stock_backfill_posts_index(table: str) -> str:
    return f"""
CREATE INDEX IF NOT EXISTS idx_{table}_stock_created
ON {table}(stock_key, created_at, post_uid)
"""


def create_research_stock_backfill_meta_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    stock_key TEXT PRIMARY KEY,
    signature TEXT NOT NULL DEFAULT '',
    row_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
)
"""


def create_research_stock_backfill_meta_index(table: str) -> str:
    return f"""
CREATE INDEX IF NOT EXISTS idx_{table}_updated
ON {table}(updated_at)
"""


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


def create_research_stock_backfill_dirty_keys_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    stock_key TEXT PRIMARY KEY,
    reason TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
)
"""


def create_research_stock_backfill_dirty_keys_index(table: str) -> str:
    return f"""
CREATE INDEX IF NOT EXISTS idx_{table}_updated
ON {table}(updated_at, stock_key)
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
