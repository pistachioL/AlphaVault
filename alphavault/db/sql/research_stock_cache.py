from __future__ import annotations


def create_entity_page_snapshot_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    entity_key TEXT PRIMARY KEY,
    header_title TEXT NOT NULL DEFAULT '',
    signal_total INTEGER NOT NULL DEFAULT 0,
    signals_json TEXT NOT NULL DEFAULT '[]',
    related_sectors_json TEXT NOT NULL DEFAULT '[]',
    related_stocks_json TEXT NOT NULL DEFAULT '[]',
    backfill_posts_json TEXT NOT NULL DEFAULT '[]',
    content_hash TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
)
"""


def create_entity_page_snapshot_index(table: str) -> str:
    return f"""
CREATE INDEX IF NOT EXISTS idx_{table}_updated
ON {table}(updated_at)
"""


def upsert_entity_page_snapshot_hot(table: str) -> str:
    return f"""
INSERT INTO {table}(
    entity_key,
    header_title,
    signal_total,
    signals_json,
    related_sectors_json,
    related_stocks_json,
    content_hash,
    updated_at
)
VALUES(
    :entity_key,
    :header_title,
    :signal_total,
    :signals_json,
    :related_sectors_json,
    :related_stocks_json,
    :content_hash,
    :updated_at
)
ON CONFLICT(entity_key) DO UPDATE SET
    entity_key = excluded.entity_key,
    header_title = excluded.header_title,
    signal_total = excluded.signal_total,
    signals_json = excluded.signals_json,
    related_sectors_json = excluded.related_sectors_json,
    related_stocks_json = excluded.related_stocks_json,
    content_hash = excluded.content_hash,
    updated_at = excluded.updated_at
"""


def upsert_entity_page_snapshot_extras(table: str) -> str:
    return f"""
INSERT INTO {table}(
    entity_key,
    backfill_posts_json,
    content_hash,
    updated_at
)
VALUES(
    :entity_key,
    :backfill_posts_json,
    :content_hash,
    :updated_at
)
ON CONFLICT(entity_key) DO UPDATE SET
    backfill_posts_json = excluded.backfill_posts_json,
    content_hash = excluded.content_hash,
    updated_at = excluded.updated_at
"""


def select_entity_page_snapshot(table: str) -> str:
    return f"""
SELECT entity_key,
       header_title,
       signal_total,
       signals_json,
       related_sectors_json,
       related_stocks_json,
       backfill_posts_json,
       content_hash,
       updated_at
FROM {table}
WHERE entity_key = :entity_key
LIMIT 1
"""


def create_research_stock_dirty_keys_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    job_type TEXT NOT NULL,
    target_key TEXT NOT NULL,
    reason_mask INTEGER NOT NULL DEFAULT 0,
    dirty_since TEXT NOT NULL DEFAULT '',
    last_dirty_at TEXT NOT NULL DEFAULT '',
    claim_until TEXT NOT NULL DEFAULT '',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(job_type, target_key)
)
"""


def create_research_stock_dirty_keys_index(table: str) -> str:
    return f"""
CREATE INDEX IF NOT EXISTS idx_{table}_claimable
ON {table}(job_type, claim_until, dirty_since, last_dirty_at, target_key)
"""


def upsert_research_stock_dirty_key(table: str) -> str:
    return f"""
INSERT INTO {table}(
    job_type,
    target_key,
    reason_mask,
    dirty_since,
    last_dirty_at,
    claim_until,
    attempt_count,
    updated_at
)
VALUES(
    :job_type,
    :target_key,
    :reason_mask,
    :dirty_since,
    :last_dirty_at,
    '',
    0,
    :updated_at
)
ON CONFLICT(job_type, target_key) DO UPDATE SET
    reason_mask = {table}.reason_mask | excluded.reason_mask,
    dirty_since = CASE
        WHEN {table}.dirty_since = '' THEN excluded.dirty_since
        ELSE {table}.dirty_since
    END,
    last_dirty_at = excluded.last_dirty_at,
    claim_until = '',
    updated_at = excluded.updated_at
"""


def select_research_stock_dirty_keys(table: str) -> str:
    return f"""
SELECT job_type,
       target_key,
       reason_mask,
       dirty_since,
       last_dirty_at,
       claim_until,
       attempt_count,
       updated_at
FROM {table}
WHERE job_type = :job_type
ORDER BY dirty_since ASC, last_dirty_at ASC, target_key ASC
LIMIT :limit
"""


def select_claimable_research_stock_dirty_keys(table: str) -> str:
    return f"""
SELECT job_type,
       target_key,
       reason_mask,
       dirty_since,
       last_dirty_at,
       claim_until,
       attempt_count,
       updated_at
FROM {table}
WHERE job_type = :job_type
  AND (claim_until = '' OR claim_until <= :now)
ORDER BY dirty_since ASC, last_dirty_at ASC, target_key ASC
LIMIT :limit
"""
