from __future__ import annotations


def upsert_security_master_stock(table: str) -> str:
    return f"""
INSERT INTO {table}(
    stock_key,
    market,
    code,
    official_name,
    official_name_norm,
    created_at,
    updated_at
)
VALUES (
    :stock_key,
    :market,
    :code,
    :official_name,
    :official_name_norm,
    :now,
    :now
)
ON CONFLICT(stock_key) DO UPDATE SET
    market = excluded.market,
    code = excluded.code,
    official_name = excluded.official_name,
    official_name_norm = excluded.official_name_norm,
    updated_at = excluded.updated_at
"""


def upsert_research_relation(table: str) -> str:
    return f"""
INSERT INTO {table}(
    relation_id,
    relation_type,
    left_key,
    right_key,
    relation_label,
    source,
    created_at,
    updated_at
)
VALUES (
    :relation_id,
    :relation_type,
    :left_key,
    :right_key,
    :relation_label,
    :source,
    :now,
    :now
)
ON CONFLICT(relation_id) DO UPDATE SET
    source = excluded.source,
    updated_at = excluded.updated_at
"""


def upsert_relation_candidate(table: str) -> str:
    return f"""
INSERT INTO {table} AS target(
    candidate_id,
    relation_type,
    left_key,
    right_key,
    relation_label,
    suggestion_reason,
    evidence_summary,
    score,
    ai_status,
    status,
    created_at,
    updated_at
)
VALUES (
    :candidate_id,
    :relation_type,
    :left_key,
    :right_key,
    :relation_label,
    :suggestion_reason,
    :evidence_summary,
    :score,
    :ai_status,
    :status,
    :now,
    :now
)
ON CONFLICT(candidate_id) DO UPDATE SET
    relation_type = excluded.relation_type,
    left_key = excluded.left_key,
    right_key = excluded.right_key,
    relation_label = excluded.relation_label,
    suggestion_reason = excluded.suggestion_reason,
    evidence_summary = excluded.evidence_summary,
    score = excluded.score,
    ai_status = excluded.ai_status,
    status = CASE
        WHEN target.status IN ('accepted', 'ignored', 'blocked') THEN target.status
        ELSE excluded.status
    END,
    updated_at = excluded.updated_at
"""


def select_security_master_by_official_names(table: str, *, name_count: int) -> str:
    count = max(1, int(name_count or 0))
    placeholders = ", ".join(["?"] * count)
    return f"""
SELECT stock_key, official_name, official_name_norm
FROM {table}
WHERE official_name_norm IN ({placeholders})
"""


def select_security_master_by_stock_key(table: str) -> str:
    return f"""
SELECT official_name
FROM {table}
WHERE stock_key = :stock_key
LIMIT 1
"""


def select_all_security_master(table: str) -> str:
    return f"""
SELECT stock_key, official_name, official_name_norm
FROM {table}
"""


def select_all_stock_alias_relations(table: str) -> str:
    return f"""
SELECT left_key, right_key
FROM {table}
WHERE relation_type = 'stock_alias'
  AND relation_label = 'alias_of'
"""


def select_pending_candidates(table: str) -> str:
    return f"""
SELECT candidate_id,
       relation_type,
       left_key,
       right_key,
       CASE
           WHEN relation_type IN ('stock_sector', 'sector_sector') AND right_key LIKE 'cluster:%'
               THEN substr(right_key, 9)
           ELSE right_key
       END AS candidate_key,
       relation_label,
       suggestion_reason, evidence_summary, score, ai_status, status,
       created_at, updated_at
FROM {table}
WHERE status = 'pending'
ORDER BY score DESC, updated_at DESC, candidate_id ASC
"""


def select_pending_candidates_for_left_key(table: str) -> str:
    return f"""
SELECT candidate_id,
       relation_type,
       left_key,
       right_key,
       CASE
           WHEN relation_type IN ('stock_sector', 'sector_sector') AND right_key LIKE 'cluster:%'
               THEN substr(right_key, 9)
           ELSE right_key
       END AS candidate_key,
       relation_label,
       suggestion_reason, evidence_summary, score, ai_status, status,
       created_at, updated_at
FROM {table}
WHERE status = 'pending'
  AND left_key = :left_key
ORDER BY score DESC, updated_at DESC, candidate_id ASC
LIMIT :limit
"""


def select_candidate_by_id(table: str) -> str:
    return f"""
SELECT candidate_id, relation_type, left_key, right_key, relation_label,
       suggestion_reason, evidence_summary, score, ai_status, status,
       created_at, updated_at
FROM {table}
WHERE candidate_id = :candidate_id
"""


def select_candidate_status_by_ids(table: str, *, id_count: int) -> str:
    count = max(1, int(id_count or 0))
    placeholders = ", ".join(["?"] * count)
    return f"""
SELECT candidate_id, status
FROM {table}
WHERE candidate_id IN ({placeholders})
"""


def update_candidate_status(table: str) -> str:
    return f"""
UPDATE {table}
SET status = :status, updated_at = :now
WHERE candidate_id = :candidate_id
"""


def upsert_alias_resolve_task_attempt(table: str) -> str:
    return f"""
INSERT INTO {table} AS target(
    alias_key,
    status,
    attempt_count,
    created_at,
    updated_at
)
VALUES (:alias_key, :status, 1, :now, :now)
ON CONFLICT(alias_key) DO UPDATE SET
    attempt_count = target.attempt_count + 1,
    updated_at = excluded.updated_at
"""


def upsert_alias_resolve_task_status(table: str) -> str:
    return f"""
INSERT INTO {table} AS target(
    alias_key,
    status,
    attempt_count,
    sample_post_uid,
    sample_evidence,
    sample_raw_text_excerpt,
    created_at,
    updated_at
)
VALUES (
    :alias_key,
    :status,
    :attempt_count,
    :sample_post_uid,
    :sample_evidence,
    :sample_raw_text_excerpt,
    :now,
    :now
)
ON CONFLICT(alias_key) DO UPDATE SET
    status = excluded.status,
    sample_post_uid = CASE
        WHEN COALESCE(target.sample_post_uid, '') <> '' THEN target.sample_post_uid
        ELSE excluded.sample_post_uid
    END,
    sample_evidence = CASE
        WHEN COALESCE(target.sample_evidence, '') <> '' THEN target.sample_evidence
        ELSE excluded.sample_evidence
    END,
    sample_raw_text_excerpt = CASE
        WHEN COALESCE(target.sample_raw_text_excerpt, '') <> '' THEN target.sample_raw_text_excerpt
        ELSE excluded.sample_raw_text_excerpt
    END,
    updated_at = excluded.updated_at
"""


def select_alias_resolve_tasks_by_keys(table: str, *, key_count: int) -> str:
    count = max(1, int(key_count or 0))
    placeholders = ", ".join(["?"] * count)
    return f"""
SELECT alias_key, status, attempt_count,
       sample_post_uid, sample_evidence, sample_raw_text_excerpt
FROM {table}
WHERE alias_key IN ({placeholders})
"""


def select_alias_resolve_tasks_by_status(
    table: str,
    *,
    limit_count: int | None = None,
) -> str:
    limit_clause = ""
    if limit_count is not None:
        limit_clause = "\nLIMIT :limit"
    return f"""
SELECT alias_key, status, attempt_count,
       sample_post_uid, sample_evidence, sample_raw_text_excerpt,
       created_at, updated_at
FROM {table}
WHERE status = :status
ORDER BY updated_at DESC, alias_key ASC{limit_clause}
"""
