from __future__ import annotations

from contextlib import contextmanager
from typing import cast

import pandas as pd

from alphavault.db.turso_db import TursoConnection
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


def test_sync_stock_alias_relations_marks_dirty_for_changed_pairs(monkeypatch) -> None:
    marked: list[str] = []
    inserted_pairs: list[tuple[str, str]] = []

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
        "alphavault.worker.stock_alias_sync._load_alias_assertions",
        lambda _conn: pd.DataFrame(),
    )
    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync._load_stock_alias_relations",
        lambda _conn: pd.DataFrame(),
    )
    monkeypatch.setattr(
        "alphavault.worker.stock_alias_sync.pick_unresolved_stock_alias_keys",
        lambda *_args, **_kwargs: ["stock:紫金"],
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
        "alphavault.worker.stock_alias_sync.build_ai_stock_alias_map",
        lambda *_args, **_kwargs: {"stock:紫金": "stock:601899.SH"},
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
