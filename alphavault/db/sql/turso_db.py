from __future__ import annotations

SQL_BEGIN = "BEGIN"
SQL_COMMIT = "COMMIT"
SQL_ROLLBACK = "ROLLBACK"


SELECT_ASSERTIONS_FOR_POST_UID = """
SELECT idx, speaker, relation_to_topic, topic_key, action, action_strength, summary,
       evidence, evidence_refs_json, confidence, stock_codes_json, stock_names_json,
       industries_json, commodities_json, indices_json, keywords_json,
       cluster_keys_json, author, created_at
FROM assertions
WHERE post_uid = :post_uid
ORDER BY idx ASC
"""

SELECT_ASSERTION_MENTIONS_FOR_POST_UID = """
SELECT assertion_idx, mention_idx, mention_text, mention_type, evidence, confidence
FROM assertion_mentions
WHERE post_uid = :post_uid
ORDER BY assertion_idx ASC, mention_idx ASC
"""

SELECT_ASSERTION_ENTITIES_FOR_POST_UID = """
SELECT assertion_idx, entity_idx, entity_key, entity_type,
       source_mention_text, source_mention_type, confidence
FROM assertion_entities
WHERE post_uid = :post_uid
ORDER BY assertion_idx ASC, entity_idx ASC
"""


__all__ = [
    "SELECT_ASSERTIONS_FOR_POST_UID",
    "SELECT_ASSERTION_MENTIONS_FOR_POST_UID",
    "SELECT_ASSERTION_ENTITIES_FOR_POST_UID",
    "SQL_BEGIN",
    "SQL_COMMIT",
    "SQL_ROLLBACK",
]
