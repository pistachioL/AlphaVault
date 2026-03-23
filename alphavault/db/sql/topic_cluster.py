from __future__ import annotations


def select_clusters(table: str) -> str:
    return f"SELECT cluster_key, cluster_name, description, created_at, updated_at FROM {table}"


def select_topic_map(table: str) -> str:
    return f"SELECT topic_key, cluster_key, source, confidence, created_at FROM {table}"


def select_post_overrides(table: str) -> str:
    return f"SELECT post_uid, cluster_key, reason, confidence, created_at FROM {table}"


def upsert_cluster(table: str) -> str:
    return f"""
INSERT INTO {table}(
    cluster_key, cluster_name, description, created_at, updated_at
)
VALUES (:cluster_key, :cluster_name, :description, :now, :now)
ON CONFLICT(cluster_key) DO UPDATE SET
    cluster_name = excluded.cluster_name,
    description = excluded.description,
    updated_at = excluded.updated_at
"""


def upsert_cluster_topic_map(table: str) -> str:
    return f"""
INSERT INTO {table}(
    topic_key, cluster_key, source, confidence, created_at
)
VALUES (:topic_key, :cluster_key, :source, :confidence, :now)
ON CONFLICT(topic_key, cluster_key) DO UPDATE SET
    source = excluded.source,
    confidence = excluded.confidence,
    created_at = excluded.created_at
"""


def delete_cluster_topic_map(table: str) -> str:
    return f"""
DELETE FROM {table}
WHERE topic_key = :topic_key AND cluster_key = :cluster_key
"""


def delete_cluster_key_rows(table: str) -> str:
    return f"DELETE FROM {table} WHERE cluster_key = :cluster_key"
