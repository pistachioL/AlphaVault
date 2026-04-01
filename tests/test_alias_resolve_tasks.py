from __future__ import annotations

import libsql

from alphavault.db.turso_db import TursoConnection
from alphavault.research_workbench import (
    ALIAS_TASK_STATUS_MANUAL,
    ensure_research_workbench_schema,
    get_alias_resolve_tasks_map,
    increment_alias_resolve_attempts,
    list_manual_alias_resolve_tasks,
    set_alias_resolve_task_status,
)
from alphavault.worker.local_cache import (
    ENV_LOCAL_CACHE_DB_PATH,
    apply_outbox_event_payload,
    open_local_cache,
    resolve_local_cache_db_path,
)


def test_default_max_retries_is_6(monkeypatch) -> None:
    monkeypatch.delenv("WORKER_STOCK_ALIAS_MAX_RETRIES", raising=False)

    from alphavault.worker.stock_alias_sync import _resolve_alias_max_retries

    assert _resolve_alias_max_retries() == 6


def test_ensure_schema_creates_alias_resolve_tasks_table() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        conn.execute(
            "SELECT alias_key, status, attempt_count FROM research_alias_resolve_tasks"
        ).fetchall()
    finally:
        conn.close()


def test_increment_attempts_creates_row_and_increments() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)

        assert get_alias_resolve_tasks_map(conn, ["stock:紫金"]) == {}

        first = increment_alias_resolve_attempts(conn, ["stock:紫金"])
        assert first == {"stock:紫金": 1}

        second = increment_alias_resolve_attempts(conn, ["stock:紫金"])
        assert second == {"stock:紫金": 2}

        assert get_alias_resolve_tasks_map(conn, ["stock:紫金"]) == {
            "stock:紫金": {"status": "pending", "attempt_count": 2}
        }
    finally:
        conn.close()


def test_set_status_and_list_manual() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)

        increment_alias_resolve_attempts(conn, ["stock:紫金"])
        set_alias_resolve_task_status(conn, alias_key="stock:紫金", status="manual")

        manual = list_manual_alias_resolve_tasks(conn)
        assert manual
        assert manual[0]["alias_key"] == "stock:紫金"
        assert str(manual[0]["attempt_count"]) == "1"
        assert manual[0]["status"] == ALIAS_TASK_STATUS_MANUAL
    finally:
        conn.close()


def test_worker_marks_manual_after_max_retries(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("WORKER_STOCK_ALIAS_MAX_RETRIES", "2")

    from alphavault.worker.stock_alias_sync import sync_stock_alias_relations

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        monkeypatch.setenv(ENV_LOCAL_CACHE_DB_PATH, str(tmp_path / "cache.sqlite3"))
        db_path = resolve_local_cache_db_path(source_name="")
        with open_local_cache(db_path=db_path) as cache_conn:
            apply_outbox_event_payload(
                cache_conn,
                payload={
                    "event_type": "ai_done",
                    "post_uid": "p1",
                    "author": "alice",
                    "created_at": "2026-03-26 10:00:00",
                    "final_status": "relevant",
                    "assertions": [
                        {
                            "topic_key": "stock:紫金",
                            "action": "trade.buy",
                            "action_strength": 1,
                            "confidence": 0.9,
                            "stock_codes": [],
                            "stock_names": ["紫金"],
                        }
                    ],
                },
            )

        ensure_research_workbench_schema(conn)

        sync_stock_alias_relations(conn)
        sync_stock_alias_relations(conn)

        manual = list_manual_alias_resolve_tasks(conn)
        assert manual
        assert manual[0]["alias_key"] == "stock:紫金"
        assert str(manual[0]["attempt_count"]) == "2"
        assert manual[0]["status"] == ALIAS_TASK_STATUS_MANUAL

        sync_stock_alias_relations(conn)
    finally:
        conn.close()


def test_parse_manual_alias_target_stock_key_only_accepts_code() -> None:
    from alphavault_reflex.organizer_state import _parse_manual_alias_target_stock_key

    assert _parse_manual_alias_target_stock_key("601899.SH") == "stock:601899.SH"
    assert _parse_manual_alias_target_stock_key("stock:601899.sh") == "stock:601899.SH"
    assert _parse_manual_alias_target_stock_key("紫金") == ""
