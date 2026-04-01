from __future__ import annotations

from contextlib import contextmanager
from typing import cast

import pandas as pd

from alphavault.db.turso_db import TursoConnection
from alphavault.worker.local_cache import (
    ENV_LOCAL_CACHE_DB_PATH,
    apply_outbox_event_payload,
    open_local_cache,
    resolve_local_cache_db_path,
)
from alphavault.worker.stock_alias_sync import _candidate_alias_pairs
from alphavault.worker.stock_alias_sync import _existing_alias_pairs
from alphavault.worker.stock_alias_sync import sync_stock_alias_relations


def test_candidate_alias_pairs_filters_invalid_and_dedupes() -> None:
    ai_alias_map = {
        "stock:紫金": "stock:601899.SH",
        " stock:紫金 ": "stock:601899.SH",
        "stock:601899.SH": "stock:601899.SH",
        "": "stock:600519.SH",
        "stock:茅台": "",
        "cluster:white_liquor": "stock:600519.SH",
    }

    pairs = _candidate_alias_pairs(ai_alias_map)

    assert pairs == [("stock:601899.SH", "stock:紫金")]


def test_existing_alias_pairs_reads_left_right_from_relations_frame() -> None:
    relations = pd.DataFrame(
        [
            {
                "relation_type": "stock_alias",
                "left_key": "stock:601899.SH",
                "right_key": "stock:紫金",
                "relation_label": "alias_of",
            },
            {
                "relation_type": "stock_alias",
                "left_key": "",
                "right_key": "stock:无效",
                "relation_label": "alias_of",
            },
        ]
    )

    pairs = _existing_alias_pairs(relations)

    assert pairs == {("stock:601899.SH", "stock:紫金")}


def test_sync_stock_alias_relations_marks_dirty_for_changed_pairs(
    monkeypatch, tmp_path
) -> None:
    marked: list[str] = []
    inserted_pairs: list[tuple[str, str]] = []

    monkeypatch.setenv(ENV_LOCAL_CACHE_DB_PATH, str(tmp_path / "cache.sqlite3"))
    db_path = resolve_local_cache_db_path(source_name="")
    with open_local_cache(db_path=db_path) as cache_conn:
        apply_outbox_event_payload(
            cache_conn,
            payload={
                "event_type": "ai_done",
                "post_uid": "p1",
                "author": "alice",
                "created_at": "2026-03-25 10:00:00",
                "final_status": "relevant",
                "assertions": [
                    {
                        "topic_key": "stock:紫金",
                        "action": "trade.buy",
                        "action_strength": 1,
                        "confidence": 0.9,
                        "stock_codes": ["601899.SH"],
                        "stock_names": ["紫金矿业"],
                    }
                ],
            },
        )
        apply_outbox_event_payload(
            cache_conn,
            payload={
                "event_type": "ai_done",
                "post_uid": "p2",
                "author": "alice",
                "created_at": "2026-03-26 10:00:00",
                "final_status": "relevant",
                "assertions": [
                    {
                        "topic_key": "stock:紫金",
                        "action": "trade.buy",
                        "action_strength": 1,
                        "confidence": 0.9,
                        "stock_codes": ["601899.SH"],
                        "stock_names": ["紫金矿业"],
                    }
                ],
            },
        )

    @contextmanager
    def _fake_conn_ctx(_engine_or_conn):
        yield object()

    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync._use_conn",
        _fake_conn_ctx,
    )
    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync.ensure_research_workbench_schema",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync._load_stock_alias_relations",
        lambda _conn: pd.DataFrame(),
    )
    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync.get_alias_resolve_tasks_map",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync._resolve_alias_max_retries",
        lambda: 3,
    )
    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync.increment_alias_resolve_attempts",
        lambda *_args, **_kwargs: {"stock:紫金": 1},
    )
    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync.set_alias_resolve_task_status",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync.record_stock_alias_relation",
        lambda _conn, *, stock_key, alias_key, source: inserted_pairs.append(
            (stock_key, alias_key)
        ),
    )
    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync.mark_stock_dirty",
        lambda _conn, *, stock_key, reason: marked.append(stock_key),
    )

    sync_stock_alias_relations(cast(TursoConnection, object()))

    assert inserted_pairs == [("stock:601899.SH", "stock:紫金")]
    assert marked == ["stock:601899.SH", "stock:紫金"]


def test_sync_stock_alias_relations_returns_early_when_low_priority_slot_busy(
    monkeypatch,
) -> None:
    @contextmanager
    def _fake_conn_ctx(_engine_or_conn):
        yield object()

    def _should_not_load(_conn, **_kwargs):
        raise AssertionError("assertion load should be skipped when gate is busy")

    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync._use_conn",
        _fake_conn_ctx,
    )
    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync.ensure_research_workbench_schema",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync._load_stock_alias_relations",
        _should_not_load,
    )

    stats = sync_stock_alias_relations(
        cast(TursoConnection, object()),
        acquire_low_priority_slot=lambda: False,
    )

    assert stats == {
        "assertions": 0,
        "resolved": 0,
        "candidates": 0,
        "inserted": 0,
        "attempted": 0,
        "eligible": 0,
        "queued": 0,
        "has_more": True,
        "remaining_aliases": -1,
        "locked": True,
    }
