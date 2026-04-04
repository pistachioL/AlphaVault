from __future__ import annotations


def create_research_stock_hot_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    stock_key TEXT PRIMARY KEY,
    entity_key TEXT NOT NULL DEFAULT '',
    header_title TEXT NOT NULL DEFAULT '',
    signal_total INTEGER NOT NULL DEFAULT 0,
    signals_json TEXT NOT NULL DEFAULT '[]',
    related_sectors_json TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL
)
"""


def create_research_stock_hot_index(table: str) -> str:
    return f"""
CREATE INDEX IF NOT EXISTS idx_{table}_updated
ON {table}(updated_at)
"""


def upsert_research_stock_hot(table: str) -> str:
    return f"""
INSERT INTO {table}(
    stock_key,
    entity_key,
    header_title,
    signal_total,
    signals_json,
    related_sectors_json,
    updated_at
)
VALUES(
    :stock_key,
    :entity_key,
    :header_title,
    :signal_total,
    :signals_json,
    :related_sectors_json,
    :updated_at
)
ON CONFLICT(stock_key) DO UPDATE SET
    entity_key = excluded.entity_key,
    header_title = excluded.header_title,
    signal_total = excluded.signal_total,
    signals_json = excluded.signals_json,
    related_sectors_json = excluded.related_sectors_json,
    updated_at = excluded.updated_at
"""


def select_research_stock_hot(table: str) -> str:
    return f"""
SELECT stock_key,
       entity_key,
       header_title,
       signal_total,
       signals_json,
       related_sectors_json,
       updated_at
FROM {table}
WHERE stock_key = :stock_key
LIMIT 1
"""


def create_research_stock_extras_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    stock_key TEXT PRIMARY KEY,
    backfill_posts_json TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL
)
"""


def create_research_stock_extras_index(table: str) -> str:
    return f"""
CREATE INDEX IF NOT EXISTS idx_{table}_updated
ON {table}(updated_at)
"""


def upsert_research_stock_extras(table: str) -> str:
    return f"""
INSERT INTO {table}(
    stock_key,
    backfill_posts_json,
    updated_at
)
VALUES(
    :stock_key,
    :backfill_posts_json,
    :updated_at
)
ON CONFLICT(stock_key) DO UPDATE SET
    backfill_posts_json = excluded.backfill_posts_json,
    updated_at = excluded.updated_at
"""


def select_research_stock_extras(table: str) -> str:
    return f"""
SELECT stock_key,
       backfill_posts_json,
       updated_at
FROM {table}
WHERE stock_key = :stock_key
LIMIT 1
"""


def create_research_stock_dirty_keys_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    stock_key TEXT PRIMARY KEY,
    reason TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
)
"""


def create_research_stock_dirty_keys_index(table: str) -> str:
    return f"""
CREATE INDEX IF NOT EXISTS idx_{table}_updated
ON {table}(updated_at, stock_key)
"""


def upsert_research_stock_dirty_key(table: str) -> str:
    return f"""
INSERT INTO {table}(stock_key, reason, updated_at)
VALUES(:stock_key, :reason, :updated_at)
ON CONFLICT(stock_key) DO UPDATE SET
    reason = excluded.reason,
    updated_at = excluded.updated_at
"""


def select_research_stock_dirty_keys(table: str) -> str:
    return f"""
SELECT stock_key,
       reason,
       updated_at
FROM {table}
ORDER BY updated_at ASC, stock_key ASC
LIMIT :limit
"""
