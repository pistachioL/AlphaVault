from __future__ import annotations

from contextlib import contextmanager
import threading
import time
from typing import cast

from alphavault.db.turso_db import TursoConnection
from alphavault.rss.utils import RateLimiter
from alphavault.worker import research_relation_candidates_cache as relation_cache


class _FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@contextmanager
def _fake_savepoint(_conn):
    yield


def _patch_common(monkeypatch, *, stock_keys: list[str]) -> None:
    monkeypatch.setattr(
        relation_cache,
        "try_acquire_worker_job_lock",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        relation_cache,
        "release_worker_job_lock",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        relation_cache,
        "ensure_research_workbench_schema",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        relation_cache,
        "load_worker_job_cursor",
        lambda *_args, **_kwargs: "",
    )
    monkeypatch.setattr(
        relation_cache,
        "save_worker_job_cursor",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        relation_cache,
        "_load_distinct_trade_stock_keys",
        lambda *, db_path: stock_keys,
    )
    monkeypatch.setattr(
        relation_cache, "enrich_candidates_with_ai", lambda rows, **_k: rows
    )
    monkeypatch.setattr(
        relation_cache,
        "_upsert_candidate_batch",
        lambda *_args, **_kwargs: (0, [], [], ""),
    )
    monkeypatch.setattr(
        relation_cache,
        "_delete_stale_pending_candidates",
        lambda *_args, **_kwargs: 0,
    )
    monkeypatch.setattr(
        relation_cache,
        "mark_entity_page_dirty",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(relation_cache, "turso_savepoint", _fake_savepoint)
    monkeypatch.setattr(
        relation_cache, "_build_stock_alias_candidates", lambda **_k: []
    )


def test_sync_relation_candidates_cache_respects_ai_max_inflight(monkeypatch) -> None:
    _patch_common(
        monkeypatch,
        stock_keys=["stock:1", "stock:2", "stock:3", "stock:4"],
    )
    lock = threading.Lock()
    active = 0
    max_active = 0
    calls: list[str] = []

    def _fake_build(*, stock_key: str, **_kwargs):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        calls.append(stock_key)
        time.sleep(0.03)
        with lock:
            active -= 1
        return []

    monkeypatch.setattr(relation_cache, "_build_stock_alias_candidates", _fake_build)

    stats = relation_cache.sync_relation_candidates_cache(
        cast(TursoConnection, _FakeConn()),
        limiter=RateLimiter(0.0),
        ai_enabled=True,
        max_stocks_per_run=4,
        max_sectors_per_run=0,
        ai_max_inflight=2,
    )

    assert max_active <= 2
    assert len(calls) == 4
    assert int(stats.get("processed", 0)) == 4


def test_sync_relation_candidates_cache_stops_when_should_continue_false(
    monkeypatch,
) -> None:
    _patch_common(
        monkeypatch,
        stock_keys=["stock:1", "stock:2", "stock:3", "stock:4"],
    )
    stop = {"value": False}
    calls: list[str] = []

    def _fake_build(*, stock_key: str, **_kwargs):
        calls.append(stock_key)
        stop["value"] = True
        return []

    monkeypatch.setattr(relation_cache, "_build_stock_alias_candidates", _fake_build)

    stats = relation_cache.sync_relation_candidates_cache(
        cast(TursoConnection, _FakeConn()),
        limiter=RateLimiter(0.0),
        ai_enabled=True,
        max_stocks_per_run=4,
        max_sectors_per_run=0,
        ai_max_inflight=1,
        should_continue=lambda: not bool(stop["value"]),
    )

    assert calls == ["stock:1"]
    assert int(stats.get("processed", 0)) == 1
    assert bool(stats.get("has_more", False)) is True


def test_collect_candidates_parallel_respects_shared_slot_gate() -> None:
    keys = ["stock:1", "stock:2", "stock:3"]
    lock = threading.Lock()
    active = 0
    max_active = 0
    gated_inflight = 0
    calls: list[str] = []

    def _acquire_slot() -> bool:
        nonlocal gated_inflight
        with lock:
            if gated_inflight >= 1:
                return False
            gated_inflight += 1
            return True

    def _release_slot() -> None:
        nonlocal gated_inflight
        with lock:
            if gated_inflight > 0:
                gated_inflight -= 1

    def _build_candidates(stock_key: str) -> list[dict[str, str]]:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        calls.append(stock_key)
        time.sleep(0.03)
        with lock:
            active -= 1
        return []

    rows, stopped_early = relation_cache._collect_candidates_parallel(
        keys=keys,
        ai_enabled=True,
        ai_max_inflight=3,
        should_continue=lambda: True,
        acquire_low_priority_slot=_acquire_slot,
        release_low_priority_slot=_release_slot,
        limiter=RateLimiter(0.0),
        build_candidates=_build_candidates,
    )

    assert [key for key, _ in rows] == keys
    assert stopped_early is False
    assert len(calls) == len(keys)
    assert max_active <= 1
    assert gated_inflight == 0
