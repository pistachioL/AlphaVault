from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace


def _load_script_module():
    script_path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "backfill_entity_match_followups.py"
    )
    spec = importlib.util.spec_from_file_location(
        "backfill_entity_match_followups",
        script_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("failed_to_load_script_module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeEngine:
    def __init__(self, name: str) -> None:
        self.name = name
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


class _FakeConn:
    pass


class _FakeConnContext:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        return None


def test_main_disposes_source_and_standard_engines(monkeypatch) -> None:
    script = _load_script_module()
    source_engine = _FakeEngine("source")
    standard_engine = _FakeEngine("standard")
    persisted_results: list[object] = []

    monkeypatch.setattr(script, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(script, "configure_logging", lambda **_kwargs: "INFO")
    monkeypatch.setattr(
        script,
        "parse_args",
        lambda: SimpleNamespace(
            source="weibo",
            days=30,
            post_uids=None,
            log_level="info",
        ),
    )
    monkeypatch.setattr(
        script,
        "require_postgres_source_from_env",
        lambda _name: SimpleNamespace(
            name="weibo", dsn="postgres://test", schema="weibo"
        ),
    )
    monkeypatch.setattr(
        script,
        "ensure_postgres_engine",
        lambda _dsn, *, schema_name="": source_engine,
    )
    monkeypatch.setattr(
        script,
        "get_research_workbench_engine_from_env",
        lambda: standard_engine,
    )
    monkeypatch.setattr(
        script,
        "postgres_connect_autocommit",
        lambda _engine: _FakeConnContext(_FakeConn()),
    )
    monkeypatch.setattr(
        script,
        "_load_assertion_rows",
        lambda _conn, *, days=None, post_uids=None: [
            {
                "assertion_id": "weibo:1#1",
                "post_uid": "weibo:1",
                "evidence": "原文",
                "raw_text": "原文",
                "created_at": "2026-04-15 10:00:00",
            }
        ],
    )
    monkeypatch.setattr(
        script,
        "_load_mentions_by_assertion_id",
        lambda _conn, _assertion_ids: {
            "weibo:1#1": [
                {
                    "mention_text": "长江电力股份有限公司",
                    "mention_type": "stock_name",
                    "confidence": 0.88,
                }
            ]
        },
    )
    monkeypatch.setattr(
        script,
        "resolve_rows_entity_matches",
        lambda _engine, _rows_by_post_uid: {
            "weibo:1": [
                SimpleNamespace(
                    relation_candidates=[{"candidate_id": "c1"}],
                    alias_task_keys=[],
                )
            ]
        },
    )
    monkeypatch.setattr(
        script,
        "persist_entity_match_followups_batch",
        lambda _engine, results: persisted_results.extend(results),
    )

    assert script.main() == 0
    assert persisted_results
    assert source_engine.disposed is True
    assert standard_engine.disposed is True
