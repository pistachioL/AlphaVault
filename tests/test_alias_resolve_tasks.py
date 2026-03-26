from __future__ import annotations

import libsql

from alphavault.db.turso_db import TursoConnection
from alphavault.db.sql.turso_db import CREATE_ASSERTIONS_TABLE
from alphavault.research_workbench import (
    ALIAS_TASK_STATUS_MANUAL,
    ensure_research_workbench_schema,
    get_alias_resolve_tasks_map,
    increment_alias_resolve_attempts,
    list_manual_alias_resolve_tasks,
    set_alias_resolve_task_status,
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


def test_worker_marks_manual_after_max_retries(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_STOCK_ALIAS_MAX_RETRIES", "2")
    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects.ai_is_configured",
        lambda: (True, ""),
    )
    ai_calls: list[str] = []

    def _fake_call_ai(**kwargs):
        ai_calls.append(str(kwargs.get("trace_label") or ""))
        return {"target_object_key": "", "ai_reason": "没把握"}

    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects._call_ai_with_litellm",
        _fake_call_ai,
    )

    from alphavault.worker.stock_alias_sync import sync_stock_alias_relations

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        conn.execute(CREATE_ASSERTIONS_TABLE)
        conn.execute(
            "ALTER TABLE assertions ADD COLUMN author TEXT NOT NULL DEFAULT ''"
        )
        conn.execute(
            "ALTER TABLE assertions ADD COLUMN created_at TEXT NOT NULL DEFAULT ''"
        )
        conn.execute(
            "ALTER TABLE assertions ADD COLUMN cluster_keys_json TEXT NOT NULL DEFAULT '[]'"
        )
        conn.execute(
            """
INSERT INTO assertions(
    post_uid, idx, topic_key, action, action_strength, summary, evidence, confidence,
    stock_codes_json, stock_names_json, industries_json, commodities_json, indices_json,
    author, created_at, cluster_keys_json
) VALUES (
    'p1', 1, 'stock:601899.SH', 'trade.buy', 1, '建仓', 'e', 0.9,
    '["601899.SH"]', '["紫金矿业"]', '[]', '[]', '[]',
    'alice', '2026-03-25 10:00:00', '["gold"]'
)
"""
        )
        conn.execute(
            """
INSERT INTO assertions(
    post_uid, idx, topic_key, action, action_strength, summary, evidence, confidence,
    stock_codes_json, stock_names_json, industries_json, commodities_json, indices_json,
    author, created_at, cluster_keys_json
) VALUES (
    'p2', 1, 'stock:紫金', 'trade.buy', 1, '继续拿着', 'e', 0.9,
    '[]', '["紫金"]', '[]', '[]', '[]',
    'alice', '2026-03-26 10:00:00', '["gold"]'
)
"""
        )

        ensure_research_workbench_schema(conn)

        sync_stock_alias_relations(conn)
        sync_stock_alias_relations(conn)

        manual = list_manual_alias_resolve_tasks(conn)
        assert manual
        assert manual[0]["alias_key"] == "stock:紫金"
        assert str(manual[0]["attempt_count"]) == "2"
        assert manual[0]["status"] == ALIAS_TASK_STATUS_MANUAL
        assert len(ai_calls) == 2

        sync_stock_alias_relations(conn)
        assert len(ai_calls) == 2
    finally:
        conn.close()


def test_parse_manual_alias_target_stock_key_only_accepts_code() -> None:
    from alphavault_reflex.organizer_state import _parse_manual_alias_target_stock_key

    assert _parse_manual_alias_target_stock_key("601899.SH") == "stock:601899.SH"
    assert _parse_manual_alias_target_stock_key("stock:601899.sh") == "stock:601899.SH"
    assert _parse_manual_alias_target_stock_key("紫金") == ""
