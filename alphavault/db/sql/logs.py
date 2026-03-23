from __future__ import annotations

SELECT_AI_STATUS_COUNTS = """
SELECT ai_status AS ai_status, COUNT(1) AS n
FROM posts
GROUP BY ai_status
ORDER BY n DESC
"""


def select_ai_queue_rows(status_placeholders: str, *, only_with_error: bool) -> str:
    where = f"WHERE ai_status IN ({status_placeholders})"
    if only_with_error:
        where += " AND ai_last_error IS NOT NULL AND TRIM(ai_last_error) <> ''"
    return f"""
SELECT
    post_uid,
    author,
    created_at,
    ai_status,
    COALESCE(ai_retry_count, 0) AS ai_retry_count,
    ai_next_retry_at,
    ai_running_at,
    COALESCE(ai_last_error, '') AS ai_last_error,
    ingested_at,
    processed_at
FROM posts
{where}
ORDER BY
    COALESCE(ai_running_at, ai_next_retry_at, ingested_at, 0) DESC
LIMIT :limit
"""
