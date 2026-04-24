from __future__ import annotations

from alphavault.constants import SCHEMA_STANDARD, SCHEMA_WEIBO, SCHEMA_XUEQIU


_SOURCE_TABLES = {
    "posts",
    "assertions",
    "assertion_mentions",
    "assertion_entities",
    "post_context_runs",
    "post_context_mentions",
    "post_context_entities",
    "post_analysis_feedback",
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


def _table_names(pg_conn, schema_name: str) -> set[str]:
    rows = pg_conn.execute(
        """
SELECT tablename
FROM pg_tables
WHERE schemaname = %(schema_name)s
""",
        {"schema_name": schema_name},
    ).fetchall()
    return {str(row[0]) for row in rows}


def _column_names(pg_conn, schema_name: str, table_name: str) -> set[str]:
    rows = pg_conn.execute(
        """
SELECT column_name
FROM information_schema.columns
WHERE table_schema = %(schema_name)s AND table_name = %(table_name)s
ORDER BY ordinal_position
""",
        {"schema_name": schema_name, "table_name": table_name},
    ).fetchall()
    return {str(row[0]) for row in rows}


def _index_names(pg_conn, schema_name: str, table_name: str) -> set[str]:
    rows = pg_conn.execute(
        """
SELECT indexname
FROM pg_indexes
WHERE schemaname = %(schema_name)s AND tablename = %(table_name)s
""",
        {"schema_name": schema_name, "table_name": table_name},
    ).fetchall()
    return {str(row[0]) for row in rows}


def test_apply_cloud_schema_source_installs_into_target_schema(pg_conn) -> None:
    from alphavault.db.cloud_schema import apply_cloud_schema

    apply_cloud_schema(pg_conn, target="source", schema_name=SCHEMA_WEIBO)

    assert _table_names(pg_conn, SCHEMA_WEIBO) == _SOURCE_TABLES
    assert _table_names(pg_conn, SCHEMA_STANDARD) == set()

    row = pg_conn.execute("SELECT to_regclass('weibo.posts')").fetchone()
    assert row[0] == "weibo.posts"


def test_apply_cloud_schema_standard_installs_into_standard_schema(pg_conn) -> None:
    from alphavault.db.cloud_schema import apply_cloud_schema

    apply_cloud_schema(pg_conn, target="standard", schema_name=SCHEMA_STANDARD)

    assert _table_names(pg_conn, SCHEMA_STANDARD) == _STANDARD_TABLES
    assert _table_names(pg_conn, SCHEMA_WEIBO) == set()

    row = pg_conn.execute("SELECT to_regclass('standard.relations')").fetchone()
    assert row[0] == "standard.relations"


def test_apply_cloud_schema_all_installs_all_known_schemas(pg_conn) -> None:
    from alphavault.db.cloud_schema import apply_cloud_schema

    apply_cloud_schema(pg_conn, target="all")

    assert _table_names(pg_conn, SCHEMA_WEIBO) == _SOURCE_TABLES
    assert _table_names(pg_conn, SCHEMA_XUEQIU) == _SOURCE_TABLES
    assert _table_names(pg_conn, SCHEMA_STANDARD) == _STANDARD_TABLES


def test_load_cloud_schema_sql_supports_targets() -> None:
    from alphavault.db.cloud_schema import load_cloud_schema_sql

    source_sql = load_cloud_schema_sql(target="source")
    standard_sql = load_cloud_schema_sql(target="standard")
    all_sql = load_cloud_schema_sql(target="all")

    assert "CREATE SCHEMA IF NOT EXISTS {{schema_name}}" in source_sql
    assert "CREATE TABLE IF NOT EXISTS {{schema_name}}.posts" in source_sql
    assert "CREATE TABLE IF NOT EXISTS {{schema_name}}.relations" not in source_sql
    assert "CREATE SCHEMA IF NOT EXISTS {{schema_name}}" in standard_sql
    assert "CREATE TABLE IF NOT EXISTS {{schema_name}}.relations" in standard_sql
    assert "CREATE TABLE IF NOT EXISTS {{schema_name}}.posts" not in standard_sql
    assert source_sql in all_sql
    assert standard_sql in all_sql


def test_cloud_schema_sql_has_no_alter_table() -> None:
    from alphavault.db.cloud_schema import load_cloud_schema_sql

    sql_text = load_cloud_schema_sql(target="all").upper()

    assert "ALTER TABLE" not in sql_text


def test_apply_cloud_schema_posts_table_has_no_ai_runtime_columns(pg_conn) -> None:
    from alphavault.db.cloud_schema import apply_cloud_schema

    apply_cloud_schema(pg_conn, target="source", schema_name=SCHEMA_WEIBO)
    post_columns = _column_names(pg_conn, SCHEMA_WEIBO, "posts")
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

    index_names = _index_names(pg_conn, SCHEMA_WEIBO, "posts")
    assert "idx_posts_ai_status_next_retry_at" not in index_names


def test_apply_cloud_schema_feedback_table_has_expected_columns(pg_conn) -> None:
    from alphavault.db.cloud_schema import apply_cloud_schema

    apply_cloud_schema(pg_conn, target="source", schema_name=SCHEMA_WEIBO)

    feedback_columns = _column_names(pg_conn, SCHEMA_WEIBO, "post_analysis_feedback")
    assert feedback_columns == {
        "feedback_id",
        "post_uid",
        "feedback_tag",
        "feedback_note",
        "feedback_status",
        "entrypoint",
        "submitted_at",
        "applied_at",
    }

    index_names = _index_names(pg_conn, SCHEMA_WEIBO, "post_analysis_feedback")
    assert "idx_post_analysis_feedback_post_uid_submitted_at" in index_names
    assert "idx_post_analysis_feedback_status_submitted_at" in index_names


def test_apply_cloud_schema_assertions_table_has_no_created_at(pg_conn) -> None:
    from alphavault.db.cloud_schema import apply_cloud_schema

    apply_cloud_schema(pg_conn, target="source", schema_name=SCHEMA_WEIBO)
    assertion_columns = _column_names(pg_conn, SCHEMA_WEIBO, "assertions")
    assert {
        "assertion_id",
        "post_uid",
        "idx",
        "action",
        "action_strength",
        "summary",
        "evidence",
    } == assertion_columns

    index_names = _index_names(pg_conn, SCHEMA_WEIBO, "assertions")
    assert "idx_assertions_created_at" not in index_names
