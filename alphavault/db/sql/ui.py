from __future__ import annotations

_ASSERTION_ROLLUP_CTES = """
WITH entity_rollup AS (
    SELECT
        ae.assertion_id AS assertion_id,
        COALESCE(
            MAX(CASE WHEN ae.is_primary = 1 THEN ae.entity_key END),
            MAX(CASE WHEN ae.entity_type = 'stock' THEN ae.entity_key END),
            MAX(ae.entity_key),
            ''
        ) AS entity_key,
        COALESCE(
            (
                SELECT CAST(json_agg(value ORDER BY value) AS TEXT)
                FROM (
                    SELECT DISTINCT SUBSTR(e2.entity_key, 7) AS value
                    FROM {assertion_entities_table} e2
                    WHERE e2.assertion_id = ae.assertion_id
                      AND e2.entity_type = 'stock'
                      AND e2.entity_key LIKE 'stock:%'
                    ORDER BY value
                )
            ),
            '[]'
        ) AS stock_codes,
        COALESCE(
            (
                SELECT CAST(json_agg(value ORDER BY value) AS TEXT)
                FROM (
                    SELECT DISTINCT SUBSTR(e2.entity_key, 10) AS value
                    FROM {assertion_entities_table} e2
                    WHERE e2.assertion_id = ae.assertion_id
                      AND e2.entity_type = 'industry'
                      AND e2.entity_key LIKE 'industry:%'
                    ORDER BY value
                )
            ),
            '[]'
        ) AS industries_json,
        COALESCE(
            (
                SELECT CAST(json_agg(value ORDER BY value) AS TEXT)
                FROM (
                    SELECT DISTINCT SUBSTR(e2.entity_key, 11) AS value
                    FROM {assertion_entities_table} e2
                    WHERE e2.assertion_id = ae.assertion_id
                      AND e2.entity_type = 'commodity'
                      AND e2.entity_key LIKE 'commodity:%'
                    ORDER BY value
                )
            ),
            '[]'
        ) AS commodities_json,
        COALESCE(
            (
                SELECT CAST(json_agg(value ORDER BY value) AS TEXT)
                FROM (
                    SELECT DISTINCT SUBSTR(e2.entity_key, 7) AS value
                    FROM {assertion_entities_table} e2
                    WHERE e2.assertion_id = ae.assertion_id
                      AND e2.entity_type = 'index'
                      AND e2.entity_key LIKE 'index:%'
                    ORDER BY value
                )
            ),
            '[]'
        ) AS indices_json
    FROM {assertion_entities_table} ae
    GROUP BY ae.assertion_id
),
mention_rollup AS (
    SELECT
        am.assertion_id AS assertion_id,
        COALESCE(MAX(am.confidence), 0.5) AS confidence,
        COALESCE(
            (
                SELECT CAST(json_agg(value ORDER BY value) AS TEXT)
                FROM (
                    SELECT DISTINCT m2.mention_text AS value
                    FROM {assertion_mentions_table} m2
                    WHERE m2.assertion_id = am.assertion_id
                      AND m2.mention_type = 'stock_name'
                      AND TRIM(COALESCE(m2.mention_text, '')) <> ''
                    ORDER BY value
                )
            ),
            '[]'
        ) AS stock_names,
        COALESCE(
            (
                SELECT CAST(json_agg(value ORDER BY value) AS TEXT)
                FROM (
                    SELECT DISTINCT
                        COALESCE(NULLIF(TRIM(m2.mention_norm), ''), TRIM(m2.mention_text)) AS value
                    FROM {assertion_mentions_table} m2
                    WHERE m2.assertion_id = am.assertion_id
                      AND m2.mention_type = 'keyword'
                      AND TRIM(COALESCE(m2.mention_norm, m2.mention_text, '')) <> ''
                    ORDER BY value
                )
            ),
            '[]'
        ) AS keywords_json
    FROM {assertion_mentions_table} am
    GROUP BY am.assertion_id
),
cluster_rollup AS (
    SELECT
        ae.assertion_id AS assertion_id,
        COALESCE(
            (
                SELECT CAST(json_agg(value ORDER BY value) AS TEXT)
                FROM (
                    SELECT DISTINCT tct.cluster_key AS value
                    FROM {assertion_entities_table} e2
                    JOIN {topic_cluster_topics_table} tct
                      ON tct.topic_key = e2.entity_key
                    WHERE e2.assertion_id = ae.assertion_id
                      AND e2.entity_type IN ('industry', 'commodity', 'index', 'keyword')
                      AND TRIM(COALESCE(tct.cluster_key, '')) <> ''
                    ORDER BY value
                )
            ),
            '[]'
        ) AS cluster_keys_json
    FROM {assertion_entities_table} ae
    GROUP BY ae.assertion_id
)
"""

_ASSERTION_PROJECTION_BY_COLUMN = {
    "assertion_id": "a.assertion_id AS assertion_id",
    "post_uid": "a.post_uid AS post_uid",
    "idx": "a.idx AS idx",
    "entity_key": "COALESCE(er.entity_key, '') AS entity_key",
    "action": "a.action AS action",
    "action_strength": "a.action_strength AS action_strength",
    "summary": "a.summary AS summary",
    "evidence": "a.evidence AS evidence",
    "confidence": "COALESCE(mr.confidence, 0.5) AS confidence",
    "stock_codes": "COALESCE(er.stock_codes, '[]') AS stock_codes",
    "stock_names": "COALESCE(mr.stock_names, '[]') AS stock_names",
    "industries_json": "COALESCE(er.industries_json, '[]') AS industries_json",
    "commodities_json": "COALESCE(er.commodities_json, '[]') AS commodities_json",
    "indices_json": "COALESCE(er.indices_json, '[]') AS indices_json",
    "keywords_json": "COALESCE(mr.keywords_json, '[]') AS keywords_json",
    "cluster_keys_json": "COALESCE(cr.cluster_keys_json, '[]') AS cluster_keys_json",
    "author": "'' AS author",
    "created_at": "p.created_at AS created_at",
}


def build_assertion_rollup_ctes(
    *,
    assertion_entities_table: str = "assertion_entities",
    assertion_mentions_table: str = "assertion_mentions",
    topic_cluster_topics_table: str = "topic_cluster_topics",
) -> str:
    return _ASSERTION_ROLLUP_CTES.format(
        assertion_entities_table=assertion_entities_table,
        assertion_mentions_table=assertion_mentions_table,
        topic_cluster_topics_table=topic_cluster_topics_table,
    )


def build_assertion_rollup_joins(assertion_alias: str = "a") -> str:
    return f"""
LEFT JOIN entity_rollup er
  ON er.assertion_id = {assertion_alias}.assertion_id
LEFT JOIN mention_rollup mr
  ON mr.assertion_id = {assertion_alias}.assertion_id
LEFT JOIN cluster_rollup cr
  ON cr.assertion_id = {assertion_alias}.assertion_id
""".strip()


def build_assertion_projection_expr(
    selected_columns: list[str],
    *,
    assertion_alias: str = "a",
    post_alias: str = "p",
) -> str:
    columns = selected_columns or list(_ASSERTION_PROJECTION_BY_COLUMN.keys())
    out: list[str] = []
    for col in columns:
        projection = _ASSERTION_PROJECTION_BY_COLUMN.get(col)
        if projection is None:
            out.append(f"{assertion_alias}.{col} AS {col}")
            continue
        out.append(
            projection.replace("a.", f"{assertion_alias}.").replace(
                "p.", f"{post_alias}."
            )
        )
    return ", ".join(out)


def build_assertions_query(
    selected_columns: list[str],
    *,
    posts_table: str = "posts",
    assertions_table: str = "assertions",
    assertion_entities_table: str = "assertion_entities",
    assertion_mentions_table: str = "assertion_mentions",
    topic_cluster_topics_table: str = "topic_cluster_topics",
) -> str:
    return (
        f"{build_assertion_rollup_ctes(assertion_entities_table=assertion_entities_table, assertion_mentions_table=assertion_mentions_table, topic_cluster_topics_table=topic_cluster_topics_table)}\n"
        f"SELECT {build_assertion_projection_expr(selected_columns)}\n"
        f"FROM {assertions_table} a\n"
        f"JOIN {posts_table} p ON p.post_uid = a.post_uid\n"
        f"{build_assertion_rollup_joins('a')}"
    )


__all__ = [
    "build_assertion_projection_expr",
    "build_assertion_rollup_ctes",
    "build_assertion_rollup_joins",
    "build_assertions_query",
]
