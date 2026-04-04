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
        stock_hot_read, "load_stock_hot_view", lambda *_args, **_kwargs: {}
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_stock_extras_snapshot",
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
        stock_hot_read, "load_stock_hot_view", lambda *_args, **_kwargs: {}
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_stock_extras_snapshot",
        lambda *_args, **_kwargs: {},
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
        "load_stock_hot_view",
        lambda *_args, **_kwargs: {
            "entity_key": "stock:600519.SH",
            "header_title": "600519.SH",
            "signals": [
                {
                    "post_uid": "p1",
                    "summary": "s",
                    "action": "trade.buy",
                    "author": "a",
                    "created_at": "",
                    "created_at_line": "",
                }
            ],
            "signal_total": 1,
            "related_sectors": [],
        },
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_stock_extras_snapshot",
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

    signals = payload.get("signals") or []
    assert isinstance(signals, list)
    assert signals
    first = signals[0]
    assert isinstance(first, dict)
    assert first.get("created_at") == ""


def test_load_stock_cached_view_does_not_expose_pending_candidates(monkeypatch) -> None:
    _setup_single_source(monkeypatch)
    monkeypatch.setattr(
        stock_hot_read,
        "load_stock_hot_view",
        lambda *_args, **_kwargs: {
            "entity_key": "stock:600519.SH",
            "header_title": "600519.SH",
            "signals": [],
            "signal_total": 0,
            "related_sectors": [{"sector_key": "white_liquor"}],
        },
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_stock_extras_snapshot",
        lambda *_args, **_kwargs: {
            "pending_candidates": [{"candidate_id": "cand-1"}],
            "backfill_posts": [{"post_uid": "p1"}],
            "updated_at": "2026-04-04 10:00:00",
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
    assert payload["backfill_posts"] == [{"post_uid": "p1"}]
