from __future__ import annotations

import libsql

from alphavault.db.turso_db import TursoConnection


_SOURCE_TABLES = {
    "posts",
    "assertions",
    "assertion_mentions",
    "assertion_entities",
    "topic_clusters",
    "topic_cluster_topics",
    "topic_cluster_post_overrides",
    "entity_page_snapshot",
    "projection_dirty",
    "worker_cursor",
    "worker_locks",
}

_STANDARD_TABLES = {
    "security_master",
    "relations",
    "relation_candidates",
    "alias_resolve_tasks",
    "homework_trade_feed",
    "follow_pages",
}


def _table_names(conn: TursoConnection) -> set[str]:
    return {
        str(row["name"])
        for row in conn.execute(
            """
SELECT name
FROM sqlite_schema
WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
"""
        )
        .mappings()
        .all()
    }


def test_apply_cloud_schema_all_creates_final_cloud_tables() -> None:
    from alphavault.db.cloud_schema import apply_cloud_schema

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        apply_cloud_schema(conn, target="all")

        table_names = _table_names(conn)
        assert _SOURCE_TABLES.union(_STANDARD_TABLES).issubset(table_names)
        assert "research_assertion_outbox" not in table_names
        assert "research_stock_backfill_posts" not in table_names
        assert "research_stock_backfill_meta" not in table_names
        assert "research_stock_backfill_dirty_keys" not in table_names

        projection_dirty_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(projection_dirty)")
            .mappings()
            .all()
        }
        assert {
            "job_type",
            "target_key",
            "reason_mask",
            "dirty_since",
            "last_dirty_at",
            "claim_until",
            "attempt_count",
            "updated_at",
        } == projection_dirty_columns

        assertion_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(assertions)").mappings().all()
        }
        assert {
            "assertion_id",
            "post_uid",
            "idx",
            "action",
            "action_strength",
            "summary",
            "evidence",
            "created_at",
        } == assertion_columns

        mention_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(assertion_mentions)")
            .mappings()
            .all()
        }
        assert {
            "assertion_id",
            "mention_seq",
            "mention_text",
            "mention_norm",
            "mention_type",
            "evidence",
            "confidence",
        } == mention_columns

        entity_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(assertion_entities)")
            .mappings()
            .all()
        }
        assert {
            "assertion_id",
            "entity_key",
            "entity_type",
            "match_source",
            "is_primary",
        } == entity_columns

        snapshot_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(entity_page_snapshot)")
            .mappings()
            .all()
        }
        assert {
            "entity_key",
            "entity_type",
            "header_json",
            "signal_top_json",
            "related_json",
            "counters_json",
            "content_hash",
            "updated_at",
        } == snapshot_columns

        alias_task_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(alias_resolve_tasks)")
            .mappings()
            .all()
        }
        assert {
            "alias_key",
            "status",
            "attempt_count",
            "sample_post_uid",
            "sample_evidence",
            "sample_raw_text_excerpt",
            "created_at",
            "updated_at",
        } == alias_task_columns

        follow_page_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(follow_pages)").mappings().all()
        }
        assert "follow_keys_json" in follow_page_columns
    finally:
        conn.close()


def test_apply_cloud_schema_supports_source_and_standard_targets() -> None:
    from alphavault.db.cloud_schema import apply_cloud_schema

    source_conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    standard_conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        apply_cloud_schema(source_conn, target="source")
        apply_cloud_schema(standard_conn, target="standard")

        source_tables = _table_names(source_conn)
        standard_tables = _table_names(standard_conn)

        assert _SOURCE_TABLES == source_tables
        assert _STANDARD_TABLES == standard_tables
    finally:
        source_conn.close()
        standard_conn.close()


def test_load_cloud_schema_sql_supports_targets() -> None:
    from alphavault.db.cloud_schema import load_cloud_schema_sql

    source_sql = load_cloud_schema_sql(target="source")
    standard_sql = load_cloud_schema_sql(target="standard")
    all_sql = load_cloud_schema_sql(target="all")

    assert "CREATE TABLE IF NOT EXISTS posts" in source_sql
    assert "CREATE TABLE IF NOT EXISTS relations" not in source_sql
    assert "CREATE TABLE IF NOT EXISTS relations" in standard_sql
    assert "CREATE TABLE IF NOT EXISTS posts" not in standard_sql
    assert source_sql in all_sql
    assert standard_sql in all_sql


def test_cloud_schema_sql_has_no_alter_table() -> None:
    from alphavault.db.cloud_schema import load_cloud_schema_sql

    sql_text = load_cloud_schema_sql(target="all").upper()

    assert "ALTER TABLE" not in sql_text


def test_apply_cloud_schema_posts_table_has_no_ai_runtime_columns() -> None:
    from alphavault.db.cloud_schema import apply_cloud_schema

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        apply_cloud_schema(conn, target="source")
        post_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(posts)").mappings().all()
        }
        assert {
            "post_uid",
            "platform",
            "platform_post_id",
            "author",
            "created_at",
            "url",
            "raw_text",
            "final_status",
            "invest_score",
            "processed_at",
            "model",
            "prompt_version",
            "archived_at",
            "ingested_at",
        } == post_columns

        index_names = {
            str(row["name"])
            for row in conn.execute(
                """
SELECT name
FROM sqlite_schema
WHERE type = 'index'
"""
            )
            .mappings()
            .all()
        }
        assert "idx_posts_ai_status_next_retry_at" not in index_names
    finally:
        conn.close()
