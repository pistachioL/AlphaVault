from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd

from alphavault_reflex.services import stock_hot_read


def _setup_single_source(monkeypatch) -> None:
    monkeypatch.setattr(stock_hot_read, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        stock_hot_read,
        "load_configured_turso_sources_from_env",
        lambda: [SimpleNamespace(name="weibo", url="u1", token="t1")],
    )
    monkeypatch.setattr(stock_hot_read, "ensure_turso_engine", lambda *_args: object())
    monkeypatch.setattr(
        stock_hot_read,
        "load_stock_alias_relations_from_env",
        lambda: (pd.DataFrame(), ""),
    )
    stock_hot_read.clear_stock_hot_read_caches()


def test_load_stock_cached_view_without_running_worker_has_no_processing_warning(
    monkeypatch,
) -> None:
    _setup_single_source(monkeypatch)
    monkeypatch.setattr(
        stock_hot_read,
        "load_entity_page_signal_snapshot",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_worker_job_cursor",
        lambda *_args, **_kwargs: "",
    )

    payload = stock_hot_read.load_stock_cached_view_from_env(
        "600519.SH",
        signal_page=1,
        signal_page_size=5,
    )

    assert str(payload.get("load_warning") or "").strip() == ""
    assert bool(payload.get("worker_running", True)) is False


def test_load_stock_cached_view_with_running_worker_includes_status(
    monkeypatch,
) -> None:
    _setup_single_source(monkeypatch)
    monkeypatch.setattr(
        stock_hot_read,
        "load_entity_page_signal_snapshot",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_worker_job_cursor",
        lambda *_args, **_kwargs: "",
    )

    def _fake_state(_engine, *, state_key: str) -> str:
        if state_key.endswith(".cycle"):
            return json.dumps(
                {
                    "status": "running",
                    "next_run_at": "2026-03-28 15:10:00",
                    "updated_at": "2026-03-28 15:00:00",
                },
                ensure_ascii=False,
            )
        return ""

    monkeypatch.setattr(stock_hot_read, "load_worker_job_cursor", _fake_state)

    payload = stock_hot_read.load_stock_cached_view_from_env(
        "600519.SH",
        signal_page=1,
        signal_page_size=5,
    )

    assert "后台处理中" in str(payload.get("load_warning") or "")
    assert bool(payload.get("worker_running", False)) is True
    assert str(payload.get("worker_next_run_at") or "") == "2026-03-28 15:10:00"
    assert str(payload.get("worker_cycle_updated_at") or "") == "2026-03-28 15:00:00"


def test_load_stock_cached_view_does_not_emit_nat_created_at(monkeypatch) -> None:
    _setup_single_source(monkeypatch)
    monkeypatch.setattr(
        stock_hot_read,
        "load_entity_page_signal_snapshot",
        lambda *_args, **_kwargs: {
            "entity_key": "stock:600519.SH",
            "entity_type": "stock",
            "header": {"title": "600519.SH"},
            "signal_top": [
                {
                    "post_uid": "p1",
                    "summary": "s",
                    "action": "trade.buy",
                    "author": "a",
                    "created_at": "",
                    "created_at_line": "",
                }
            ],
            "related": [],
            "counters": {"signal_total": 1},
        },
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_worker_job_cursor",
        lambda *_args, **_kwargs: "",
    )

    payload = stock_hot_read.load_stock_cached_view_from_env(
        "600519.SH",
        signal_page=1,
        signal_page_size=5,
    )

    signals = payload.get("signals") or []
    assert isinstance(signals, list)
    assert signals
    first = signals[0]
    assert isinstance(first, dict)
    assert first.get("created_at") == ""


def test_load_stock_cached_view_has_no_backfill_posts(monkeypatch) -> None:
    _setup_single_source(monkeypatch)
    monkeypatch.setattr(
        stock_hot_read,
        "load_entity_page_signal_snapshot",
        lambda *_args, **_kwargs: {
            "entity_key": "stock:600519.SH",
            "entity_type": "stock",
            "header": {"title": "600519.SH"},
            "signal_top": [],
            "related": [
                {
                    "entity_key": "cluster:white_liquor",
                    "entity_type": "sector",
                    "mention_count": "1",
                }
            ],
            "counters": {"signal_total": 0},
        },
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_worker_job_cursor",
        lambda *_args, **_kwargs: "",
    )

    payload = stock_hot_read.load_stock_cached_view_from_env(
        "600519.SH",
        signal_page=1,
        signal_page_size=5,
    )

    assert "pending_candidates" not in payload
    assert "backfill_posts" not in payload


def test_load_stock_cached_view_returns_relation_error_when_standard_alias_fails(
    monkeypatch,
) -> None:
    monkeypatch.setattr(stock_hot_read, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        stock_hot_read,
        "load_configured_turso_sources_from_env",
        lambda: [SimpleNamespace(name="weibo", url="u1", token="t1")],
    )
    monkeypatch.setattr(stock_hot_read, "ensure_turso_engine", lambda *_args: object())
    monkeypatch.setattr(
        stock_hot_read,
        "load_stock_alias_relations_from_env",
        lambda: (pd.DataFrame(), "turso_connect_error:standard:RuntimeError"),
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_entity_page_signal_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should_not_load_source_payload")
        ),
    )
    stock_hot_read.clear_stock_hot_read_caches()

    payload = stock_hot_read.load_stock_cached_view_from_env(
        "600519.SH",
        signal_page=1,
        signal_page_size=5,
    )

    assert payload["load_error"] == "turso_connect_error:standard:RuntimeError"
    assert payload["signals"] == []
