from __future__ import annotations

INSERT_ASSERTION_OUTBOX = """
INSERT INTO research_assertion_outbox(source, post_uid, author, event_json, created_at)
VALUES (:source, :post_uid, :author, :event_json, :created_at)
"""

SELECT_ASSERTION_OUTBOX_AFTER_ID = """
SELECT id, source, post_uid, author, event_json, created_at
FROM research_assertion_outbox
WHERE id > :after_id
ORDER BY id ASC
LIMIT :limit
"""

SELECT_POST_COUNT_ALL = "SELECT COUNT(*) FROM posts"
SELECT_ASSERTION_COUNT_ALL = "SELECT COUNT(*) FROM assertions"


def select_post_count_by_post_uids(placeholders: str) -> str:
    return f"SELECT COUNT(*) FROM posts WHERE post_uid IN ({placeholders})"


def select_assertion_count_by_post_uids(placeholders: str) -> str:
    return f"SELECT COUNT(*) FROM assertions WHERE post_uid IN ({placeholders})"


UPSERT_PENDING_POST = """
INSERT INTO posts (
    post_uid, platform, platform_post_id, author, created_at, url, raw_text, display_md,
    final_status, invest_score, processed_at, model, prompt_version, archived_at,
    ingested_at
) VALUES (
    :post_uid, :platform, :platform_post_id, :author, :created_at, :url, :raw_text, :display_md,
    :final_status, NULL, NULL, NULL, NULL, :archived_at,
    :ingested_at
)
ON CONFLICT(post_uid) DO UPDATE SET
    platform=CASE
        WHEN posts.processed_at IS NULL THEN excluded.platform
        ELSE posts.platform
    END,
    platform_post_id=CASE
        WHEN posts.processed_at IS NULL THEN excluded.platform_post_id
        ELSE posts.platform_post_id
    END,
    author=CASE
        WHEN TRIM(COALESCE(excluded.author, '')) <> '' THEN excluded.author
        ELSE posts.author
    END,
    created_at=CASE
        WHEN posts.processed_at IS NULL OR LOWER(COALESCE(excluded.platform, posts.platform, '')) = 'xueqiu' THEN excluded.created_at
        ELSE posts.created_at
    END,
    url=CASE
        WHEN posts.processed_at IS NULL THEN excluded.url
        ELSE posts.url
    END,
    raw_text=CASE
        WHEN posts.processed_at IS NULL OR LOWER(COALESCE(excluded.platform, posts.platform, '')) = 'xueqiu' THEN excluded.raw_text
        ELSE posts.raw_text
    END,
    display_md=CASE
        WHEN posts.processed_at IS NULL OR LOWER(COALESCE(excluded.platform, posts.platform, '')) = 'xueqiu' THEN excluded.display_md
        ELSE posts.display_md
    END,
    archived_at=CASE
        WHEN posts.processed_at IS NULL OR LOWER(COALESCE(excluded.platform, posts.platform, '')) = 'xueqiu' THEN excluded.archived_at
        ELSE posts.archived_at
    END,
    ingested_at=CASE
        WHEN posts.processed_at IS NULL THEN excluded.ingested_at
        ELSE posts.ingested_at
    END
"""

SELECT_CLOUD_POST = """
SELECT post_uid, platform, platform_post_id, author, created_at, url, raw_text,
       COALESCE(display_md, '') AS display_md,
       0 AS ai_retry_count
FROM posts
WHERE post_uid = :post_uid
LIMIT 1
"""

SELECT_POST_PROCESSED_AT = """
SELECT processed_at
FROM posts
WHERE post_uid = :post_uid
LIMIT 1
"""

SELECT_RECENT_POSTS_BY_AUTHOR = """
SELECT post_uid, platform_post_id, author, created_at, url, raw_text,
       COALESCE(display_md, '') AS display_md,
       processed_at
FROM posts
WHERE author = :author
ORDER BY created_at DESC
LIMIT :limit
"""

SELECT_UNPROCESSED_POST_QUEUE_ROWS = """
SELECT post_uid, platform, platform_post_id, author, created_at, url, raw_text,
       COALESCE(display_md, '') AS display_md
FROM posts
WHERE processed_at IS NULL OR TRIM(processed_at) = ''
ORDER BY ingested_at DESC, post_uid DESC
LIMIT :limit
"""

SELECT_UNPROCESSED_POST_QUEUE_ROWS_BY_PLATFORM = """
SELECT post_uid, platform, platform_post_id, author, created_at, url, raw_text,
       COALESCE(display_md, '') AS display_md
FROM posts
WHERE platform = :platform
  AND (processed_at IS NULL OR TRIM(processed_at) = '')
ORDER BY ingested_at DESC, post_uid DESC
LIMIT :limit
"""


DELETE_ASSERTIONS_BY_POST_UID = "DELETE FROM assertions WHERE post_uid = :post_uid"
DELETE_ASSERTION_MENTIONS_BY_POST_UID = (
    "DELETE FROM assertion_mentions WHERE post_uid = :post_uid"
)
DELETE_ASSERTION_ENTITIES_BY_POST_UID = (
    "DELETE FROM assertion_entities WHERE post_uid = :post_uid"
)
DELETE_ASSERTIONS_ALL = "DELETE FROM assertions"
DELETE_ASSERTION_MENTIONS_ALL = "DELETE FROM assertion_mentions"
DELETE_ASSERTION_ENTITIES_ALL = "DELETE FROM assertion_entities"


def delete_assertions_by_post_uids(placeholders: str) -> str:
    return f"DELETE FROM assertions WHERE post_uid IN ({placeholders})"


def delete_assertion_mentions_by_post_uids(placeholders: str) -> str:
    return f"DELETE FROM assertion_mentions WHERE post_uid IN ({placeholders})"


def delete_assertion_entities_by_post_uids(placeholders: str) -> str:
    return f"DELETE FROM assertion_entities WHERE post_uid IN ({placeholders})"


INSERT_ASSERTION = """
INSERT INTO assertions (
    post_uid, idx, speaker, relation_to_topic, topic_key, action, action_strength,
    summary, evidence, evidence_refs_json, confidence, stock_codes_json,
    stock_names_json, industries_json, commodities_json, indices_json, keywords_json,
    cluster_keys_json, author, created_at
) VALUES (
    :post_uid, :idx, :speaker, :relation_to_topic, :topic_key, :action,
    :action_strength, :summary, :evidence, :evidence_refs_json, :confidence,
    :stock_codes_json, :stock_names_json, :industries_json, :commodities_json,
    :indices_json, :keywords_json, :cluster_keys_json, :author, :created_at
)
"""

INSERT_ASSERTION_MENTION = """
INSERT INTO assertion_mentions (
    post_uid, assertion_idx, mention_idx, mention_text, mention_type, evidence, confidence
) VALUES (
    :post_uid, :assertion_idx, :mention_idx, :mention_text, :mention_type, :evidence, :confidence
)
"""

INSERT_ASSERTION_ENTITY = """
INSERT INTO assertion_entities (
    post_uid, assertion_idx, entity_idx, entity_key, entity_type,
    source_mention_text, source_mention_type, confidence
) VALUES (
    :post_uid, :assertion_idx, :entity_idx, :entity_key, :entity_type,
    :source_mention_text, :source_mention_type, :confidence
)
"""

UPDATE_POST_DONE = """
UPDATE posts
SET final_status=:final_status,
    invest_score=:invest_score,
    processed_at=:processed_at,
    model=:model,
    prompt_version=:prompt_version,
    archived_at=:archived_at
WHERE post_uid=:post_uid
"""

RESET_ALL_POSTS_TO_PENDING = """
UPDATE posts
SET final_status='irrelevant',
    invest_score=NULL,
    processed_at=NULL,
    model=NULL,
    prompt_version=NULL,
    archived_at=:archived_at
"""


def reset_posts_to_pending_by_post_uids(placeholders: str) -> str:
    return f"""
UPDATE posts
SET final_status='irrelevant',
    invest_score=NULL,
    processed_at=NULL,
    model=NULL,
    prompt_version=NULL,
    archived_at=:archived_at
WHERE post_uid IN ({placeholders})
"""
