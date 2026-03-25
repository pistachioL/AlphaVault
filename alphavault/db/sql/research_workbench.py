from __future__ import annotations


def create_research_objects_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    object_key TEXT PRIMARY KEY,
    object_type TEXT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""


def create_research_relations_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    relation_id TEXT PRIMARY KEY,
    relation_type TEXT NOT NULL,
    left_key TEXT NOT NULL,
    right_key TEXT NOT NULL,
    relation_label TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(relation_type, left_key, right_key, relation_label)
)
"""


def create_research_relation_candidates_table(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    candidate_id TEXT PRIMARY KEY,
    relation_type TEXT NOT NULL,
    left_key TEXT NOT NULL,
    right_key TEXT NOT NULL,
    relation_label TEXT NOT NULL,
    suggestion_reason TEXT NOT NULL DEFAULT '',
    evidence_summary TEXT NOT NULL DEFAULT '',
    score REAL NOT NULL DEFAULT 0,
    ai_status TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""


def create_research_object_index(table: str) -> str:
    return f"CREATE INDEX IF NOT EXISTS idx_{table}_type ON {table}(object_type)"


def create_research_relation_index(table: str) -> str:
    return f"""
CREATE INDEX IF NOT EXISTS idx_{table}_lookup
ON {table}(relation_type, left_key, relation_label, right_key)
"""


def create_research_relation_candidate_index(table: str) -> str:
    return f"""
CREATE INDEX IF NOT EXISTS idx_{table}_pending
ON {table}(status, relation_type, score, updated_at)
"""


def upsert_research_object(table: str) -> str:
    return f"""
INSERT INTO {table}(object_key, object_type, display_name, created_at, updated_at)
VALUES (:object_key, :object_type, :display_name, :now, :now)
ON CONFLICT(object_key) DO UPDATE SET
    object_type = excluded.object_type,
    display_name = excluded.display_name,
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
INSERT INTO {table}(
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
    status = excluded.status,
    updated_at = excluded.updated_at
"""


def select_pending_candidates(table: str) -> str:
    return f"""
SELECT candidate_id, relation_type, left_key, right_key, relation_label,
       suggestion_reason, evidence_summary, score, ai_status, status,
       created_at, updated_at
FROM {table}
WHERE status = 'pending'
ORDER BY score DESC, updated_at DESC, candidate_id ASC
"""


def select_candidate_by_id(table: str) -> str:
    return f"""
SELECT candidate_id, relation_type, left_key, right_key, relation_label,
       suggestion_reason, evidence_summary, score, ai_status, status,
       created_at, updated_at
FROM {table}
WHERE candidate_id = :candidate_id
"""


def update_candidate_status(table: str) -> str:
    return f"""
UPDATE {table}
SET status = :status, updated_at = :now
WHERE candidate_id = :candidate_id
"""
