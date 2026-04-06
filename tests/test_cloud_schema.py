from __future__ import annotations

import libsql

from alphavault.db.turso_db import TursoConnection


def test_apply_cloud_schema_creates_final_cloud_tables() -> None:
    from alphavault.db.cloud_schema import apply_cloud_schema

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        apply_cloud_schema(conn)

        table_names = {
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
        assert {
            "posts",
            "assertions",
            "assertion_mentions",
            "assertion_entities",
            "topic_clusters",
            "topic_cluster_topics",
            "topic_cluster_post_overrides",
            "security_master",
            "relations",
            "relation_candidates",
            "alias_resolve_tasks",
            "entity_page_snapshot",
            "projection_dirty",
            "homework_trade_feed",
            "follow_pages",
            "worker_cursor",
            "worker_locks",
        }.issubset(table_names)
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

        snapshot_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(entity_page_snapshot)")
            .mappings()
            .all()
        }
        assert {
            "entity_key",
            "header_title",
            "signal_total",
            "signals_json",
            "related_sectors_json",
            "related_stocks_json",
            "content_hash",
            "updated_at",
        } == snapshot_columns

        follow_page_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(follow_pages)").mappings().all()
        }
        assert "follow_keys_json" in follow_page_columns
    finally:
        conn.close()


def test_cloud_schema_sql_has_no_alter_table() -> None:
    from alphavault.db.cloud_schema import load_cloud_schema_sql

    sql_text = load_cloud_schema_sql().upper()

    assert "ALTER TABLE" not in sql_text


def test_apply_cloud_schema_posts_table_has_no_ai_runtime_columns() -> None:
    from alphavault.db.cloud_schema import apply_cloud_schema

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        apply_cloud_schema(conn)
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
