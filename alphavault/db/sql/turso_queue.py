from __future__ import annotations

_POSTS_TABLE = "posts"
_ASSERTIONS_TABLE = "assertions"
_ASSERTION_MENTIONS_TABLE = "assertion_mentions"
_ASSERTION_ENTITIES_TABLE = "assertion_entities"


def select_post_count_all_sql(posts_table: str) -> str:
    return f"SELECT COUNT(*) FROM {posts_table}"


def select_assertion_count_all_sql(assertions_table: str) -> str:
    return f"SELECT COUNT(*) FROM {assertions_table}"


def select_post_count_by_post_uids_sql(posts_table: str, placeholders: str) -> str:
    return f"SELECT COUNT(*) FROM {posts_table} WHERE post_uid IN ({placeholders})"


def select_assertion_count_by_post_uids_sql(
    assertions_table: str,
    placeholders: str,
) -> str:
    return f"SELECT COUNT(*) FROM {assertions_table} WHERE post_uid IN ({placeholders})"


def upsert_pending_post_sql(posts_table: str) -> str:
    return f"""
INSERT INTO {posts_table} (
    post_uid, platform, platform_post_id, author, created_at, url, raw_text,
    final_status, invest_score, processed_at, model, prompt_version, archived_at,
    ingested_at
) VALUES (
    :post_uid, :platform, :platform_post_id, :author, :created_at, :url, :raw_text,
    :final_status, NULL, NULL, NULL, NULL, :archived_at,
    :ingested_at
)
ON CONFLICT(post_uid) DO UPDATE SET
    platform=CASE
        WHEN {posts_table}.processed_at IS NULL THEN excluded.platform
        ELSE {posts_table}.platform
    END,
    platform_post_id=CASE
        WHEN {posts_table}.processed_at IS NULL THEN excluded.platform_post_id
        ELSE {posts_table}.platform_post_id
    END,
    author=CASE
        WHEN TRIM(COALESCE(excluded.author, '')) <> '' THEN excluded.author
        ELSE {posts_table}.author
    END,
    created_at=CASE
        WHEN {posts_table}.processed_at IS NULL
             OR LOWER(COALESCE(excluded.platform, {posts_table}.platform, '')) = 'xueqiu'
        THEN excluded.created_at
        ELSE {posts_table}.created_at
    END,
    url=CASE
        WHEN {posts_table}.processed_at IS NULL THEN excluded.url
        ELSE {posts_table}.url
    END,
    raw_text=CASE
        WHEN {posts_table}.processed_at IS NULL
             OR LOWER(COALESCE(excluded.platform, {posts_table}.platform, '')) = 'xueqiu'
        THEN excluded.raw_text
        ELSE {posts_table}.raw_text
    END,
    archived_at=CASE
        WHEN {posts_table}.processed_at IS NULL
             OR LOWER(COALESCE(excluded.platform, {posts_table}.platform, '')) = 'xueqiu'
        THEN excluded.archived_at
        ELSE {posts_table}.archived_at
    END,
    ingested_at=CASE
        WHEN {posts_table}.processed_at IS NULL THEN excluded.ingested_at
        ELSE {posts_table}.ingested_at
    END
"""


def select_cloud_post_sql(posts_table: str) -> str:
    return f"""
SELECT post_uid, platform, platform_post_id, author, created_at, url, raw_text,
       0 AS ai_retry_count
FROM {posts_table}
WHERE post_uid = :post_uid
LIMIT 1
"""


def select_post_processed_at_sql(posts_table: str) -> str:
    return f"""
SELECT processed_at
FROM {posts_table}
WHERE post_uid = :post_uid
LIMIT 1
"""


def delete_assertions_by_post_uid_sql(assertions_table: str) -> str:
    return f"DELETE FROM {assertions_table} WHERE post_uid = :post_uid"


def delete_assertion_mentions_by_post_uid_sql(
    assertion_mentions_table: str,
    assertions_table: str,
) -> str:
    return f"""
DELETE FROM {assertion_mentions_table}
WHERE assertion_id IN (
    SELECT assertion_id
    FROM {assertions_table}
    WHERE post_uid = :post_uid
)
""".strip()


def delete_assertion_entities_by_post_uid_sql(
    assertion_entities_table: str,
    assertions_table: str,
) -> str:
    return f"""
DELETE FROM {assertion_entities_table}
WHERE assertion_id IN (
    SELECT assertion_id
    FROM {assertions_table}
    WHERE post_uid = :post_uid
)
""".strip()


def delete_assertions_all_sql(assertions_table: str) -> str:
    return f"DELETE FROM {assertions_table}"


def delete_assertion_mentions_all_sql(assertion_mentions_table: str) -> str:
    return f"DELETE FROM {assertion_mentions_table}"


def delete_assertion_entities_all_sql(assertion_entities_table: str) -> str:
    return f"DELETE FROM {assertion_entities_table}"


def delete_assertions_by_post_uids_sql(
    assertions_table: str,
    placeholders: str,
) -> str:
    return f"DELETE FROM {assertions_table} WHERE post_uid IN ({placeholders})"


def delete_assertion_mentions_by_post_uids_sql(
    assertion_mentions_table: str,
    assertions_table: str,
    placeholders: str,
) -> str:
    return f"""
DELETE FROM {assertion_mentions_table}
WHERE assertion_id IN (
    SELECT assertion_id
    FROM {assertions_table}
    WHERE post_uid IN ({placeholders})
)
"""


def delete_assertion_entities_by_post_uids_sql(
    assertion_entities_table: str,
    assertions_table: str,
    placeholders: str,
) -> str:
    return f"""
DELETE FROM {assertion_entities_table}
WHERE assertion_id IN (
    SELECT assertion_id
    FROM {assertions_table}
    WHERE post_uid IN ({placeholders})
)
"""


def insert_assertion_sql(assertions_table: str) -> str:
    return f"""
INSERT INTO {assertions_table} (
    assertion_id, post_uid, idx, action, action_strength, summary, evidence, created_at
) VALUES (
    :assertion_id, :post_uid, :idx, :action, :action_strength, :summary, :evidence,
    :created_at
)
"""


def insert_assertion_mention_sql(assertion_mentions_table: str) -> str:
    return f"""
INSERT INTO {assertion_mentions_table} (
    assertion_id, mention_seq, mention_text, mention_norm, mention_type, evidence,
    confidence
) VALUES (
    :assertion_id, :mention_seq, :mention_text, :mention_norm, :mention_type,
    :evidence, :confidence
)
"""


def insert_assertion_entity_sql(assertion_entities_table: str) -> str:
    return f"""
INSERT INTO {assertion_entities_table} (
    assertion_id, entity_key, entity_type, match_source, is_primary
) VALUES (
    :assertion_id, :entity_key, :entity_type, :match_source, :is_primary
)
"""


def update_post_done_sql(posts_table: str) -> str:
    return f"""
UPDATE {posts_table}
SET final_status=:final_status,
    invest_score=:invest_score,
    processed_at=:processed_at,
    model=:model,
    prompt_version=:prompt_version,
    archived_at=:archived_at
WHERE post_uid=:post_uid
"""


def reset_all_posts_to_pending_sql(posts_table: str) -> str:
    return f"""
UPDATE {posts_table}
SET final_status='irrelevant',
    invest_score=NULL,
    processed_at=NULL,
    model=NULL,
    prompt_version=NULL,
    archived_at=:archived_at
"""


def reset_posts_to_pending_by_post_uids_sql(
    posts_table: str,
    placeholders: str,
) -> str:
    return f"""
UPDATE {posts_table}
SET final_status='irrelevant',
    invest_score=NULL,
    processed_at=NULL,
    model=NULL,
    prompt_version=NULL,
    archived_at=:archived_at
WHERE post_uid IN ({placeholders})
"""


SELECT_POST_PROCESSED_AT = select_post_processed_at_sql(_POSTS_TABLE)
UPDATE_POST_DONE = update_post_done_sql(_POSTS_TABLE)
