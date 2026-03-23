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
    platform=excluded.platform,
    platform_post_id=excluded.platform_post_id,
    author=excluded.author,
    created_at=excluded.created_at,
    url=excluded.url,
    raw_text=excluded.raw_text,
    display_md=excluded.display_md,
    archived_at=excluded.archived_at,
    ingested_at=excluded.ingested_at
WHERE posts.processed_at IS NULL
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

RECOVER_DONE_WITHOUT_PROCESSED_AT = """
UPDATE posts
SET ai_status='pending',
    ai_running_at=NULL,
    ai_next_retry_at=NULL,
    ai_last_error='ai:recovered_done_without_processed_at'
WHERE ai_status='done'
  AND (processed_at IS NULL OR TRIM(processed_at) = '')
"""


def alter_posts_add_column(column_def: str) -> str:
    return f"ALTER TABLE posts ADD COLUMN {column_def}"
