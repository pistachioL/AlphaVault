from __future__ import annotations


SELECT_ONE = "SELECT 1"
SELECT_TOTAL_POSTS = "SELECT COUNT(*) FROM posts"
SELECT_TOTAL_ASSERTIONS = "SELECT COUNT(*) FROM assertions"


def select_post_uids_in(placeholders: str) -> str:
    return f"SELECT post_uid FROM posts WHERE post_uid IN ({placeholders})"


def scan_invalid_assertion_rows(*, filter_prompt_version: bool) -> str:
    where_clause = "WHERE p.processed_at IS NOT NULL"
    if filter_prompt_version:
        where_clause += " AND COALESCE(p.prompt_version, '') = :prompt_version"
    return f"""
SELECT
    a.assertion_id AS assertion_id,
    a.post_uid AS post_uid,
    COALESCE(p.prompt_version, '') AS prompt_version,
    a.idx AS idx,
    a.action AS action,
    a.action_strength AS action_strength,
    a.summary AS summary,
    a.evidence AS evidence,
    COALESCE(p.created_at, '') AS created_at
FROM posts p
JOIN assertions a
  ON a.post_uid = p.post_uid
{where_clause}
ORDER BY a.post_uid ASC, a.idx ASC
"""
