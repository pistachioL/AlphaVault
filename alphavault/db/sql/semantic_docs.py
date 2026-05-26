from __future__ import annotations

from alphavault.constants import DEFAULT_SEMANTIC_DOC_EMBEDDING_DIMENSIONS


def select_assertions_for_post_sql(assertions_table: str) -> str:
    return f"""
SELECT assertion_id, post_uid, idx, action, action_strength, summary, evidence
FROM {assertions_table}
WHERE post_uid = :post_uid
ORDER BY idx ASC, assertion_id ASC
""".strip()


def select_assertions_for_posts_sql(
    assertions_table: str,
    *,
    post_uid_placeholders: str,
) -> str:
    return f"""
SELECT assertion_id, post_uid, idx, action, action_strength, summary, evidence
FROM {assertions_table}
WHERE post_uid IN ({post_uid_placeholders})
ORDER BY post_uid ASC, idx ASC, assertion_id ASC
""".strip()


def select_assertion_mentions_for_post_sql(
    assertion_mentions_table: str,
    assertions_table: str,
) -> str:
    return f"""
SELECT am.assertion_id, am.mention_seq, am.mention_text
FROM {assertion_mentions_table} am
JOIN {assertions_table} a
  ON a.assertion_id = am.assertion_id
WHERE a.post_uid = :post_uid
ORDER BY am.assertion_id ASC, am.mention_seq ASC
""".strip()


def select_assertion_mentions_for_posts_sql(
    assertion_mentions_table: str,
    assertions_table: str,
    *,
    post_uid_placeholders: str,
) -> str:
    return f"""
SELECT a.post_uid, am.assertion_id, am.mention_seq, am.mention_text
FROM {assertion_mentions_table} am
JOIN {assertions_table} a
  ON a.assertion_id = am.assertion_id
WHERE a.post_uid IN ({post_uid_placeholders})
ORDER BY a.post_uid ASC, am.assertion_id ASC, am.mention_seq ASC
""".strip()


def select_assertion_entities_for_post_sql(
    assertion_entities_table: str,
    assertions_table: str,
) -> str:
    return f"""
SELECT ae.assertion_id, ae.entity_key
FROM {assertion_entities_table} ae
JOIN {assertions_table} a
  ON a.assertion_id = ae.assertion_id
WHERE a.post_uid = :post_uid
ORDER BY ae.assertion_id ASC, ae.entity_key ASC
""".strip()


def select_assertion_entities_for_posts_sql(
    assertion_entities_table: str,
    assertions_table: str,
    *,
    post_uid_placeholders: str,
) -> str:
    return f"""
SELECT a.post_uid, ae.assertion_id, ae.entity_key
FROM {assertion_entities_table} ae
JOIN {assertions_table} a
  ON a.assertion_id = ae.assertion_id
WHERE a.post_uid IN ({post_uid_placeholders})
ORDER BY a.post_uid ASC, ae.assertion_id ASC, ae.entity_key ASC
""".strip()


def select_stored_semantic_docs_for_post_sql(semantic_docs_table: str) -> str:
    return f"""
SELECT doc_id, content_hash, embedding_model, CAST(embedding AS text) AS embedding_text
FROM {semantic_docs_table}
WHERE post_uid = :post_uid
ORDER BY doc_id ASC
""".strip()


def select_stored_semantic_docs_for_posts_sql(
    semantic_docs_table: str,
    *,
    post_uid_placeholders: str,
) -> str:
    return f"""
SELECT post_uid, doc_id, content_hash, embedding_model, CAST(embedding AS text) AS embedding_text
FROM {semantic_docs_table}
WHERE post_uid IN ({post_uid_placeholders})
ORDER BY post_uid ASC, doc_id ASC
""".strip()


def delete_semantic_docs_by_post_uid_sql(semantic_docs_table: str) -> str:
    return f"DELETE FROM {semantic_docs_table} WHERE post_uid = :post_uid"


def insert_semantic_doc_sql(semantic_docs_table: str) -> str:
    return f"""
INSERT INTO {semantic_docs_table} (
    doc_id,
    post_uid,
    assertion_id,
    doc_kind,
    chunk_seq,
    platform,
    author,
    created_at,
    created_at_ts,
    action,
    action_strength,
    mention_texts,
    entity_keys,
    doc_text,
    content_hash,
    embedding_model,
    embedding,
    updated_at
) VALUES (
    :doc_id,
    :post_uid,
    :assertion_id,
    :doc_kind,
    :chunk_seq,
    :platform,
    :author,
    :created_at,
    :created_at_ts,
    :action,
    :action_strength,
    :mention_texts,
    :entity_keys,
    :doc_text,
    :content_hash,
    :embedding_model,
    CAST(:embedding AS halfvec({DEFAULT_SEMANTIC_DOC_EMBEDDING_DIMENSIONS})),
    :updated_at
)
""".strip()


def select_relevant_post_uids_sql(posts_table: str) -> str:
    return f"""
SELECT post_uid
FROM {posts_table}
WHERE final_status = 'relevant'
  AND processed_at IS NOT NULL
  AND TRIM(processed_at) <> ''
  AND post_uid > :after_post_uid
ORDER BY post_uid ASC
LIMIT :limit
""".strip()
