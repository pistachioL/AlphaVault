from __future__ import annotations


def upsert_entity_page_snapshot_hot(table: str) -> str:
    return f"""
INSERT INTO {table}(
    entity_key,
    entity_type,
    header_json,
    signal_top_json,
    related_json,
    counters_json,
    content_hash,
    updated_at
)
VALUES(
    :entity_key,
    :entity_type,
    :header_json,
    :signal_top_json,
    :related_json,
    :counters_json,
    :content_hash,
    :updated_at
)
ON CONFLICT(entity_key) DO UPDATE SET
    entity_key = excluded.entity_key,
    entity_type = excluded.entity_type,
    header_json = excluded.header_json,
    signal_top_json = excluded.signal_top_json,
    related_json = excluded.related_json,
    counters_json = excluded.counters_json,
    content_hash = excluded.content_hash,
    updated_at = excluded.updated_at
"""


def select_entity_page_snapshot(table: str) -> str:
    return f"""
SELECT entity_key,
       entity_type,
       header_json,
       signal_top_json,
       related_json,
       counters_json,
       content_hash,
       updated_at
FROM {table}
WHERE entity_key = :entity_key
LIMIT 1
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
