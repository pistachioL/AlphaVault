from __future__ import annotations

from contextlib import contextmanager
import importlib.util
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

from alphavault.constants import SCHEMA_XUEQIU
from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.postgres_db import PostgresConnection


def _load_script_module():
    script_path = (
        Path(__file__).resolve().parents[1] / "scripts" / "fix_bad_cn_stock_us_keys.py"
    )
    spec = importlib.util.spec_from_file_location(
        "fix_bad_cn_stock_us_keys",
        script_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("failed_to_load_script_module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_script_can_run_directly_with_help() -> None:
    script_path = (
        Path(__file__).resolve().parents[1] / "scripts" / "fix_bad_cn_stock_us_keys.py"
    )

    result = subprocess.run(
        [sys.executable, str(script_path), "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--apply" in result.stdout


def test_main_passes_log_level_as_keyword(monkeypatch) -> None:
    script = _load_script_module()
    seen_levels: list[object] = []

    def _fake_configure_logging(*, level: object = "") -> None:
        seen_levels.append(level)

    def _fake_load_dotenv_if_present() -> None:
        return None

    def _fake_load_configured_postgres_sources_from_env() -> list[SimpleNamespace]:
        return [SimpleNamespace(schema=SCHEMA_XUEQIU, dsn="postgresql://unused")]

    class _DummyEngine:
        def dispose(self) -> None:
            return None

    def _fake_ensure_postgres_engine(_dsn: str, *, schema_name: str) -> _DummyEngine:
        assert schema_name == SCHEMA_XUEQIU
        return _DummyEngine()

    @contextmanager
    def _fake_postgres_connect_autocommit(_engine: _DummyEngine):
        yield object()

    def _fake_build_schema_fix_plan(
        _conn: object,
        *,
        schema_name: str,
        sample_limit: int = 0,
    ) -> dict[str, object]:
        assert schema_name == SCHEMA_XUEQIU
        assert sample_limit == script.DEFAULT_SAMPLE_LIMIT
        return {}

    def _fake_print_plan(_plan: object) -> None:
        return None

    monkeypatch.setattr(script, "configure_logging", _fake_configure_logging)
    monkeypatch.setattr(script, "load_dotenv_if_present", _fake_load_dotenv_if_present)
    monkeypatch.setattr(
        script,
        "load_configured_postgres_sources_from_env",
        _fake_load_configured_postgres_sources_from_env,
    )
    monkeypatch.setattr(script, "ensure_postgres_engine", _fake_ensure_postgres_engine)
    monkeypatch.setattr(
        script,
        "postgres_connect_autocommit",
        _fake_postgres_connect_autocommit,
    )
    monkeypatch.setattr(script, "build_schema_fix_plan", _fake_build_schema_fix_plan)
    monkeypatch.setattr(script, "_print_plan", _fake_print_plan)

    exit_code = script.main(["--schema", SCHEMA_XUEQIU, "--log-level", "debug"])

    assert exit_code == 0
    assert seen_levels == ["debug"]


def test_build_schema_fix_plan_reports_counts_and_samples(pg_conn) -> None:
    script = _load_script_module()
    apply_cloud_schema(pg_conn, target="source", schema_name=SCHEMA_XUEQIU)
    conn = PostgresConnection(pg_conn, schema_name=SCHEMA_XUEQIU)
    conn.execute(
        """
INSERT INTO xueqiu.assertion_entities(
  assertion_id, entity_key, entity_type, match_source, is_primary
)
VALUES
  ('xueqiu:bad:1#1', 'stock:SH600941.US', 'stock', 'stock_code', 1),
  ('xueqiu:bad:2#1', 'stock:SH601899.US', 'stock', 'stock_code', 1),
  ('xueqiu:bad:2#1', 'stock:601899.SH', 'stock', 'stock_code', 0)
"""
    )

    plan = script.build_schema_fix_plan(conn, schema_name=SCHEMA_XUEQIU, sample_limit=5)

    assert plan["schema_name"] == SCHEMA_XUEQIU
    assert plan["bad_key_count"] == 2
    assert plan["bad_row_count"] == 2
    assert plan["conflict_delete_count"] == 1
    assert plan["update_row_count"] == 1
    assert plan["group_rows"] == [
        {
            "bad_key": "stock:SH600941.US",
            "good_key": "stock:600941.SH",
            "row_count": 1,
        },
        {
            "bad_key": "stock:SH601899.US",
            "good_key": "stock:601899.SH",
            "row_count": 1,
        },
    ]
    assert plan["sample_rows"] == [
        {
            "assertion_id": "xueqiu:bad:1#1",
            "bad_key": "stock:SH600941.US",
            "good_key": "stock:600941.SH",
            "has_conflict": False,
        },
        {
            "assertion_id": "xueqiu:bad:2#1",
            "bad_key": "stock:SH601899.US",
            "good_key": "stock:601899.SH",
            "has_conflict": True,
        },
    ]


def test_apply_schema_fix_plan_deletes_conflicts_and_updates_bad_keys(pg_conn) -> None:
    script = _load_script_module()
    apply_cloud_schema(pg_conn, target="source", schema_name=SCHEMA_XUEQIU)
    conn = PostgresConnection(pg_conn, schema_name=SCHEMA_XUEQIU)
    conn.execute(
        """
INSERT INTO xueqiu.assertion_entities(
  assertion_id, entity_key, entity_type, match_source, is_primary
)
VALUES
  ('xueqiu:bad:1#1', 'stock:SH600941.US', 'stock', 'stock_code', 1),
  ('xueqiu:bad:2#1', 'stock:SH601899.US', 'stock', 'stock_code', 1),
  ('xueqiu:bad:2#1', 'stock:601899.SH', 'stock', 'stock_code', 0)
"""
    )

    stats = script.apply_schema_fix_plan(conn, schema_name=SCHEMA_XUEQIU)

    assert stats == {
        "schema_name": SCHEMA_XUEQIU,
        "deleted_conflict_rows": 1,
        "updated_rows": 1,
        "remaining_bad_rows": 0,
    }
    rows = (
        conn.execute(
            """
SELECT assertion_id, entity_key
FROM xueqiu.assertion_entities
ORDER BY assertion_id ASC, entity_key ASC
"""
        )
        .mappings()
        .all()
    )
    assert rows == [
        {
            "assertion_id": "xueqiu:bad:1#1",
            "entity_key": "stock:600941.SH",
        },
        {
            "assertion_id": "xueqiu:bad:2#1",
            "entity_key": "stock:601899.SH",
        },
    ]
