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
    updated_at TEXT NOT NULL,
    PRIMARY KEY(stock_key, post_uid)
)
"""


def create_research_stock_backfill_posts_index(table: str) -> str:
    return f"""
CREATE INDEX IF NOT EXISTS idx_{table}_stock_created
ON {table}(stock_key, created_at, post_uid)
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
    :updated_at
)
"""


def select_stock_backfill_posts(table: str) -> str:
    return f"""
SELECT post_uid, author, created_at, url, matched_terms, preview, updated_at
FROM {table}
WHERE stock_key = :stock_key
ORDER BY created_at DESC, post_uid DESC
LIMIT :limit
"""
