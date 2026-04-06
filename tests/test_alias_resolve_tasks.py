from __future__ import annotations

import libsql

from alphavault.db.cloud_schema import (
    apply_cloud_schema as ensure_research_workbench_schema,
)
from alphavault.db.turso_db import TursoConnection
from alphavault.research_workbench import (
    ALIAS_TASK_STATUS_MANUAL,
    RESEARCH_ALIAS_RESOLVE_TASKS_TABLE,
    get_alias_resolve_tasks_map,
    increment_alias_resolve_attempts,
    list_manual_alias_resolve_tasks,
    set_alias_resolve_task_status,
)


def test_ensure_schema_creates_alias_resolve_tasks_table() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        conn.execute(
            f"SELECT alias_key, status, attempt_count FROM {RESEARCH_ALIAS_RESOLVE_TASKS_TABLE}"
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


def test_parse_manual_alias_target_stock_key_only_accepts_code() -> None:
    from alphavault_reflex.organizer_state import _parse_manual_alias_target_stock_key

    assert _parse_manual_alias_target_stock_key("601899.SH") == "stock:601899.SH"
    assert _parse_manual_alias_target_stock_key("stock:601899.sh") == "stock:601899.SH"
    assert _parse_manual_alias_target_stock_key("紫金") == ""
