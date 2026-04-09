from __future__ import annotations

from alphavault.constants import SCHEMA_STANDARD
from alphavault.db.cloud_schema import (
    apply_cloud_schema as ensure_research_workbench_schema,
)
from alphavault.db.postgres_db import PostgresConnection
from alphavault.research_workbench import (
    ALIAS_TASK_STATUS_MANUAL,
    ALIAS_TASK_STATUS_PENDING,
    RESEARCH_ALIAS_RESOLVE_TASKS_TABLE,
    get_alias_resolve_tasks_map,
    increment_alias_resolve_attempts,
    list_manual_alias_resolve_tasks,
    list_pending_alias_resolve_tasks,
    set_alias_resolve_task_status,
)


def _workbench_conn(pg_conn) -> PostgresConnection:
    ensure_research_workbench_schema(
        pg_conn,
        target="standard",
        schema_name=SCHEMA_STANDARD,
    )
    return PostgresConnection(pg_conn, schema_name=SCHEMA_STANDARD)


def test_ensure_schema_creates_alias_resolve_tasks_table(pg_conn) -> None:
    conn = _workbench_conn(pg_conn)
    try:
        conn.execute(
            f"""
SELECT alias_key, status, attempt_count,
       sample_post_uid, sample_evidence, sample_raw_text_excerpt
FROM {RESEARCH_ALIAS_RESOLVE_TASKS_TABLE}
"""
        ).fetchall()
    finally:
        conn.close()


def test_increment_attempts_creates_row_and_increments(pg_conn) -> None:
    conn = _workbench_conn(pg_conn)
    try:
        assert get_alias_resolve_tasks_map(conn, ["stock:紫金"]) == {}

        first = increment_alias_resolve_attempts(conn, ["stock:紫金"])
        assert first == {"stock:紫金": 1}

        second = increment_alias_resolve_attempts(conn, ["stock:紫金"])
        assert second == {"stock:紫金": 2}

        assert get_alias_resolve_tasks_map(conn, ["stock:紫金"]) == {
            "stock:紫金": {
                "status": "pending",
                "attempt_count": 2,
                "sample_post_uid": "",
                "sample_evidence": "",
                "sample_raw_text_excerpt": "",
            }
        }
    finally:
        conn.close()


def test_set_status_and_list_manual(pg_conn) -> None:
    conn = _workbench_conn(pg_conn)
    try:
        increment_alias_resolve_attempts(conn, ["stock:紫金"])
        set_alias_resolve_task_status(conn, alias_key="stock:紫金", status="manual")

        manual = list_manual_alias_resolve_tasks(conn)
        assert manual
        assert manual[0]["alias_key"] == "stock:紫金"
        assert str(manual[0]["attempt_count"]) == "1"
        assert manual[0]["status"] == ALIAS_TASK_STATUS_MANUAL
    finally:
        conn.close()


def test_set_status_keeps_first_sample_context(pg_conn) -> None:
    conn = _workbench_conn(pg_conn)
    try:
        set_alias_resolve_task_status(
            conn,
            alias_key="stock:茅台",
            status=ALIAS_TASK_STATUS_PENDING,
            sample_post_uid="weibo:1",
            sample_evidence="第一条证据",
            sample_raw_text_excerpt="第一条原文",
        )
        set_alias_resolve_task_status(
            conn,
            alias_key="stock:茅台",
            status=ALIAS_TASK_STATUS_PENDING,
            sample_post_uid="weibo:2",
            sample_evidence="第二条证据",
            sample_raw_text_excerpt="第二条原文",
        )

        assert get_alias_resolve_tasks_map(conn, ["stock:茅台"]) == {
            "stock:茅台": {
                "status": "pending",
                "attempt_count": 0,
                "sample_post_uid": "weibo:1",
                "sample_evidence": "第一条证据",
                "sample_raw_text_excerpt": "第一条原文",
            }
        }
    finally:
        conn.close()


def test_list_pending_alias_resolve_tasks_includes_sample_context(pg_conn) -> None:
    conn = _workbench_conn(pg_conn)
    try:
        set_alias_resolve_task_status(
            conn,
            alias_key="stock:长电",
            status=ALIAS_TASK_STATUS_PENDING,
            sample_post_uid="weibo:9",
            sample_evidence="提到长电和封测",
            sample_raw_text_excerpt="今天继续看好长电科技。",
        )

        pending = list_pending_alias_resolve_tasks(conn)
        assert pending == [
            {
                "alias_key": "stock:长电",
                "status": "pending",
                "attempt_count": 0,
                "sample_post_uid": "weibo:9",
                "sample_evidence": "提到长电和封测",
                "sample_raw_text_excerpt": "今天继续看好长电科技。",
                "created_at": pending[0]["created_at"],
                "updated_at": pending[0]["updated_at"],
            }
        ]
    finally:
        conn.close()


def test_list_pending_alias_resolve_tasks_respects_limit(pg_conn) -> None:
    conn = _workbench_conn(pg_conn)
    try:
        set_alias_resolve_task_status(
            conn,
            alias_key="stock:长电",
            status=ALIAS_TASK_STATUS_PENDING,
            sample_post_uid="weibo:1",
        )
        set_alias_resolve_task_status(
            conn,
            alias_key="stock:茅台",
            status=ALIAS_TASK_STATUS_PENDING,
            sample_post_uid="weibo:2",
        )

        pending = list_pending_alias_resolve_tasks(conn, limit=1)
        assert len(pending) == 1
    finally:
        conn.close()


def test_parse_manual_alias_target_stock_key_only_accepts_code() -> None:
    from alphavault_reflex.organizer_state import _parse_manual_alias_target_stock_key

    assert _parse_manual_alias_target_stock_key("601899.SH") == "stock:601899.SH"
    assert _parse_manual_alias_target_stock_key("stock:601899.sh") == "stock:601899.SH"
    assert _parse_manual_alias_target_stock_key("紫金") == ""
