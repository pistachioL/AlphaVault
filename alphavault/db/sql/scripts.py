from __future__ import annotations


SELECT_ONE = "SELECT 1"
SELECT_LAST_INSERT_ROWID = "SELECT last_insert_rowid()"
SELECT_TOTAL_POSTS = "SELECT COUNT(*) FROM posts"
SELECT_TOTAL_ASSERTIONS = "SELECT COUNT(*) FROM assertions"

SELECT_POST_PROCESSED_AND_INGESTED = (
    "SELECT processed_at, ingested_at FROM posts WHERE post_uid = :post_uid LIMIT 1"
)


def create_healthcheck_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at INTEGER NOT NULL,
    note TEXT NOT NULL
)
"""


def insert_healthcheck_row(table: str) -> str:
    return f"INSERT INTO {table}(created_at, note) VALUES (:ts, :note)"


def delete_healthcheck_row(table: str) -> str:
    return f"DELETE FROM {table} WHERE id = :id"


def select_post_uids_in(placeholders: str) -> str:
    return f"SELECT post_uid FROM posts WHERE post_uid IN ({placeholders})"


def select_backfill_batch(*, overwrite: bool) -> str:
    if overwrite:
        return """
SELECT post_uid, author, raw_text
FROM posts
WHERE TRIM(raw_text) <> ''
  AND post_uid > :last_post_uid
  AND post_uid <= :stop_post_uid
ORDER BY post_uid ASC
LIMIT :limit
"""
    return """
SELECT post_uid, author, raw_text
FROM posts
WHERE (display_md IS NULL OR TRIM(display_md) = '')
  AND TRIM(raw_text) <> ''
  AND post_uid > :last_post_uid
  AND post_uid <= :stop_post_uid
ORDER BY post_uid ASC
LIMIT :limit
"""


def count_backfill_targets(*, overwrite: bool) -> str:
    if overwrite:
        return "SELECT COUNT(*) FROM posts WHERE TRIM(raw_text) <> ''"
    return """
SELECT COUNT(*)
FROM posts
WHERE (display_md IS NULL OR TRIM(display_md) = '')
  AND TRIM(raw_text) <> ''
"""


def max_backfill_target_post_uid(*, overwrite: bool) -> str:
    if overwrite:
        return "SELECT MAX(post_uid) FROM posts WHERE TRIM(raw_text) <> ''"
    return """
SELECT MAX(post_uid)
FROM posts
WHERE (display_md IS NULL OR TRIM(display_md) = '')
  AND TRIM(raw_text) <> ''
"""


def select_backfill_rows_by_post_uids(placeholders: str) -> str:
    return f"""
SELECT post_uid, author, raw_text
FROM posts
WHERE post_uid IN ({placeholders})
  AND TRIM(raw_text) <> ''
ORDER BY post_uid ASC
"""


def update_display_md(*, overwrite: bool) -> str:
    base = """
UPDATE posts
SET display_md = :display_md
WHERE post_uid = :post_uid
"""
    if overwrite:
        return base
    return (
        base
        + """
  AND (display_md IS NULL OR TRIM(display_md) = '')
"""
    )


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
    a.indices_json AS indices_json
FROM posts p
JOIN assertions a
  ON a.post_uid = p.post_uid
{where_clause}
ORDER BY p.post_uid ASC, a.idx ASC
"""
