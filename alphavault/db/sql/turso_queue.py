from __future__ import annotations

QUEUE_EXTRA_COLUMNS: list[tuple[str, str]] = [
    ("display_md", "display_md TEXT"),
    ("ai_status", "ai_status TEXT NOT NULL DEFAULT 'done'"),
    ("ai_retry_count", "ai_retry_count INTEGER NOT NULL DEFAULT 0"),
    ("ai_next_retry_at", "ai_next_retry_at INTEGER"),
    ("ai_running_at", "ai_running_at INTEGER"),
    ("ai_last_error", "ai_last_error TEXT"),
    ("ai_result_json", "ai_result_json TEXT"),
    ("ingested_at", "ingested_at INTEGER NOT NULL DEFAULT 0"),
]

CREATE_IDX_POSTS_AI_STATUS_NEXT_RETRY_AT = """
CREATE INDEX IF NOT EXISTS idx_posts_ai_status_next_retry_at
    ON posts(ai_status, ai_next_retry_at);
"""

CREATE_ASSERTION_OUTBOX_TABLE = """
CREATE TABLE IF NOT EXISTS research_assertion_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL DEFAULT '',
    post_uid TEXT NOT NULL,
    author TEXT NOT NULL DEFAULT '',
    event_json TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""

CREATE_IDX_ASSERTION_OUTBOX_CREATED_AT = """
CREATE INDEX IF NOT EXISTS idx_research_assertion_outbox_created_at
    ON research_assertion_outbox(created_at)
"""

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

UPSERT_PENDING_POST = """
INSERT INTO posts (
    post_uid, platform, platform_post_id, author, created_at, url, raw_text, display_md,
    final_status, invest_score, processed_at, model, prompt_version, archived_at,
    ai_status, ai_retry_count, ai_next_retry_at, ai_running_at, ai_last_error, ai_result_json,
    ingested_at
) VALUES (
    :post_uid, :platform, :platform_post_id, :author, :created_at, :url, :raw_text, :display_md,
    :final_status, NULL, NULL, NULL, NULL, :archived_at,
    :ai_status, 0, NULL, NULL, NULL, NULL,
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

SELECT_DUE_POST_UIDS = """
SELECT post_uid
FROM posts
WHERE ai_status IN ('pending', 'error')
  AND (ai_next_retry_at IS NULL OR ai_next_retry_at <= :now)
ORDER BY
    CASE
        WHEN processed_at IS NULL OR TRIM(processed_at) = '' THEN 0
        ELSE 1
    END ASC,
    COALESCE(ai_next_retry_at, 0) ASC,
    ingested_at DESC
LIMIT :limit
"""

SELECT_DUE_POST_UIDS_BY_PLATFORM = """
SELECT post_uid
FROM posts
WHERE platform = :platform
  AND ai_status IN ('pending', 'error')
  AND (ai_next_retry_at IS NULL OR ai_next_retry_at <= :now)
ORDER BY
    CASE
        WHEN processed_at IS NULL OR TRIM(processed_at) = '' THEN 0
        ELSE 1
    END ASC,
    COALESCE(ai_next_retry_at, 0) ASC,
    ingested_at DESC
LIMIT :limit
"""

TRY_MARK_AI_RUNNING = """
UPDATE posts
SET ai_status='running',
    ai_running_at=:now,
    ai_retry_count=COALESCE(ai_retry_count, 0) + 1,
    ai_last_error=NULL,
    ai_next_retry_at=NULL
WHERE post_uid=:post_uid
  AND ai_status IN ('pending', 'error')
  AND (ai_next_retry_at IS NULL OR ai_next_retry_at <= :now)
"""

SELECT_CLOUD_POST = """
SELECT post_uid, platform, platform_post_id, author, created_at, url, raw_text,
       COALESCE(display_md, '') AS display_md,
       ai_retry_count
FROM posts
WHERE post_uid = :post_uid
LIMIT 1
"""

SELECT_RECENT_POSTS_BY_AUTHOR = """
SELECT post_uid, platform_post_id, author, created_at, url, raw_text,
       COALESCE(display_md, '') AS display_md,
       processed_at, ai_status, COALESCE(ai_retry_count, 0) AS ai_retry_count
FROM posts
WHERE author = :author
ORDER BY created_at DESC
LIMIT :limit
"""

RESET_AI_RESULTS_ALL = """
UPDATE posts
SET ai_status=:ai_status,
    ai_retry_count=0,
    ai_next_retry_at=NULL,
    ai_running_at=NULL,
    ai_last_error=NULL,
    ai_result_json=NULL,
    archived_at=:archived_at
WHERE ai_status != 'running'
"""


def build_reset_ai_results_for_post_uids(placeholders: str) -> str:
    return f"""
UPDATE posts
SET ai_status=:ai_status,
    ai_retry_count=0,
    ai_next_retry_at=NULL,
    ai_running_at=NULL,
    ai_last_error=NULL,
    ai_result_json=NULL,
    archived_at=:archived_at
WHERE post_uid IN ({placeholders})
  AND ai_status != 'running'
"""


DELETE_ASSERTIONS_BY_POST_UID = "DELETE FROM assertions WHERE post_uid = :post_uid"

INSERT_ASSERTION = """
INSERT INTO assertions (
    post_uid, idx, topic_key, action, action_strength, summary, evidence, confidence,
    stock_codes_json, stock_names_json, industries_json, commodities_json, indices_json
) VALUES (
    :post_uid, :idx, :topic_key, :action, :action_strength, :summary, :evidence, :confidence,
    :stock_codes_json, :stock_names_json, :industries_json, :commodities_json, :indices_json
)
"""

UPDATE_POST_DONE = """
UPDATE posts
SET final_status=:final_status,
    invest_score=:invest_score,
    processed_at=:processed_at,
    model=:model,
    prompt_version=:prompt_version,
    archived_at=:archived_at,
    ai_status='done',
    ai_running_at=NULL,
    ai_next_retry_at=NULL,
    ai_last_error=NULL,
    ai_result_json=:ai_result_json
WHERE post_uid=:post_uid
"""

MARK_AI_ERROR = """
UPDATE posts
SET ai_status='error',
    ai_running_at=NULL,
    ai_last_error=:error,
    ai_next_retry_at=:next_retry_at,
    archived_at=:archived_at
WHERE post_uid=:post_uid
"""

RECOVER_STUCK_AI_TASKS = """
UPDATE posts
SET ai_status='error',
    ai_running_at=NULL,
    ai_last_error='ai::recovered_after_restart',
    ai_next_retry_at=:next_retry_at
WHERE ai_status='running'
  AND ai_running_at IS NOT NULL
  AND ai_running_at <= :threshold
"""

RECOVER_STUCK_AI_TASKS_BY_PLATFORM = """
UPDATE posts
SET ai_status='error',
    ai_running_at=NULL,
    ai_last_error='ai::recovered_after_restart',
    ai_next_retry_at=:next_retry_at
WHERE platform = :platform
  AND ai_status='running'
  AND ai_running_at IS NOT NULL
  AND ai_running_at <= :threshold
"""

RECOVER_DONE_WITHOUT_PROCESSED_AT = """
UPDATE posts
SET ai_status='pending',
    ai_running_at=NULL,
    ai_next_retry_at=NULL,
    ai_last_error='ai::recovered_done_without_processed_at'
WHERE ai_status='done'
  AND (processed_at IS NULL OR TRIM(processed_at) = '')
"""

RECOVER_DONE_WITHOUT_PROCESSED_AT_BY_PLATFORM = """
UPDATE posts
SET ai_status='pending',
    ai_running_at=NULL,
    ai_next_retry_at=NULL,
    ai_last_error='ai::recovered_done_without_processed_at'
WHERE platform = :platform
  AND ai_status='done'
  AND (processed_at IS NULL OR TRIM(processed_at) = '')
"""


def alter_posts_add_column(column_def: str) -> str:
    return f"ALTER TABLE posts ADD COLUMN {column_def}"
