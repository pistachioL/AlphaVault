from __future__ import annotations


SELECT_ONE = "SELECT 1"
SELECT_TOTAL_POSTS = "SELECT COUNT(*) FROM posts"
SELECT_TOTAL_ASSERTIONS = "SELECT COUNT(*) FROM assertions"
WEIBO_RAW_TEXT_MIGRATION_TABLE = "weibo_raw_text_migration"


def select_post_uids_in(placeholders: str) -> str:
    return f"SELECT post_uid FROM posts WHERE post_uid IN ({placeholders})"


CREATE_WEIBO_RAW_TEXT_MIGRATION_TABLE = f"""
CREATE TABLE IF NOT EXISTS {WEIBO_RAW_TEXT_MIGRATION_TABLE} (
    post_uid TEXT PRIMARY KEY,
    old_raw_text TEXT NOT NULL,
    new_raw_text TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'applied', 'conflict', 'skipped')),
    reason TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""

CREATE_WEIBO_RAW_TEXT_MIGRATION_STATUS_INDEX = f"""
CREATE INDEX IF NOT EXISTS idx_weibo_raw_text_migration_status_post_uid
    ON {WEIBO_RAW_TEXT_MIGRATION_TABLE}(status, post_uid)
"""

SELECT_PENDING_WEIBO_RAW_TEXT_MIGRATION_BATCH = f"""
SELECT post_uid, old_raw_text, new_raw_text, status, reason
FROM {WEIBO_RAW_TEXT_MIGRATION_TABLE}
WHERE status = 'pending'
ORDER BY post_uid ASC
LIMIT :limit
"""

UPSERT_WEIBO_RAW_TEXT_MIGRATION_ROW = f"""
INSERT INTO {WEIBO_RAW_TEXT_MIGRATION_TABLE}(
    post_uid,
    old_raw_text,
    new_raw_text,
    status,
    reason,
    updated_at
)
VALUES(
    :post_uid,
    :old_raw_text,
    :new_raw_text,
    :status,
    :reason,
    :updated_at
)
ON CONFLICT(post_uid) DO UPDATE SET
    old_raw_text = excluded.old_raw_text,
    new_raw_text = excluded.new_raw_text,
    status = excluded.status,
    reason = excluded.reason,
    updated_at = excluded.updated_at
"""

UPDATE_POST_RAW_TEXT_IF_UNCHANGED = """
UPDATE posts
SET raw_text = :new_raw_text
WHERE post_uid = :post_uid
  AND raw_text = :old_raw_text
"""

UPDATE_WEIBO_RAW_TEXT_MIGRATION_STATUS = f"""
UPDATE {WEIBO_RAW_TEXT_MIGRATION_TABLE}
SET status = :status,
    reason = :reason,
    updated_at = :updated_at
WHERE post_uid = :post_uid
"""


def select_legacy_weibo_raw_text_batch() -> str:
    return """
SELECT post_uid, platform, author, raw_text
FROM posts
WHERE LOWER(COALESCE(platform, '')) = 'weibo'
  AND TRIM(raw_text) <> ''
  AND post_uid > :last_post_uid
  AND post_uid <= :stop_post_uid
ORDER BY post_uid ASC
LIMIT :limit
"""


def count_legacy_weibo_raw_text_source_posts() -> str:
    return """
SELECT COUNT(*)
FROM posts
WHERE LOWER(COALESCE(platform, '')) = 'weibo'
  AND TRIM(raw_text) <> ''
"""


def max_legacy_weibo_raw_text_post_uid() -> str:
    return """
SELECT MAX(post_uid)
FROM posts
WHERE LOWER(COALESCE(platform, '')) = 'weibo'
  AND TRIM(raw_text) <> ''
"""


def select_legacy_weibo_raw_text_rows_by_post_uids(placeholders: str) -> str:
    return f"""
SELECT post_uid, platform, author, raw_text
FROM posts
WHERE post_uid IN ({placeholders})
  AND LOWER(COALESCE(platform, '')) = 'weibo'
  AND TRIM(raw_text) <> ''
ORDER BY post_uid ASC
"""


def select_pending_weibo_raw_text_migration_rows_by_post_uids(placeholders: str) -> str:
    return f"""
SELECT post_uid, old_raw_text, new_raw_text, status, reason
FROM {WEIBO_RAW_TEXT_MIGRATION_TABLE}
WHERE status = 'pending'
  AND post_uid IN ({placeholders})
ORDER BY post_uid ASC
"""


def scan_invalid_assertion_rows(*, filter_prompt_version: bool) -> str:
    where_clause = "WHERE p.processed_at IS NOT NULL"
    if filter_prompt_version:
        where_clause += " AND COALESCE(p.prompt_version, '') = :prompt_version"
    return f"""
SELECT
    p.post_uid AS post_uid,
    COALESCE(p.prompt_version, '') AS prompt_version,
    a.idx AS idx,
    a.topic_key AS topic_key,
    a.action AS action,
    a.action_strength AS action_strength,
    a.confidence AS confidence,
    a.stock_codes_json AS stock_codes_json,
    a.stock_names_json AS stock_names_json,
    a.industries_json AS industries_json,
    a.commodities_json AS commodities_json,
    a.indices_json AS indices_json,
    COALESCE(a.keywords_json, '[]') AS keywords_json
FROM posts p
JOIN assertions a
  ON a.post_uid = p.post_uid
{where_clause}
ORDER BY p.post_uid ASC, a.idx ASC
"""
