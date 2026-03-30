from __future__ import annotations

from typing import cast

from alphavault.db.turso_db import TursoConnection
from alphavault.worker import research_backfill_cache as backfill_cache


class _FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _record_replace_call(calls: list[str]) -> int:
    calls.append("called")
    return 0


def test_sync_stock_backfill_cache_yields_to_rss_after_current_stock(
    monkeypatch,
) -> None:
    saved_cursors: list[str] = []

    monkeypatch.setattr(
        backfill_cache,
        "try_acquire_worker_job_lock",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        backfill_cache,
        "release_worker_job_lock",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        backfill_cache,
        "load_worker_job_cursor",
        lambda *_args, **_kwargs: "",
    )
    monkeypatch.setattr(
        backfill_cache,
        "save_worker_job_cursor",
        lambda *_args, cursor, **_kwargs: saved_cursors.append(str(cursor or "")),
    )
    monkeypatch.setattr(
        backfill_cache,
        "_select_stock_keys_batch",
        lambda *_args, **_kwargs: ["stock:a", "stock:b", "stock:c"],
    )
    monkeypatch.setattr(
        backfill_cache,
        "list_stock_backfill_posts",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        backfill_cache,
        "_build_backfill_candidates_for_stock",
        lambda *_args, **_kwargs: ([{"post_uid": "p1"}], False),
    )
    monkeypatch.setattr(
        backfill_cache,
        "replace_stock_backfill_posts",
        lambda *_args, **_kwargs: 1,
    )
    monkeypatch.setattr(
        backfill_cache,
        "mark_stock_dirty",
        lambda *_args, **_kwargs: None,
    )

    stats = backfill_cache.sync_stock_backfill_cache(
        cast(TursoConnection, _FakeConn()),
        should_continue=lambda: False,
    )

    assert int(stats.get("processed", 0)) == 1
    assert int(stats.get("written", 0)) == 1
    assert bool(stats.get("has_more", False)) is True
    assert saved_cursors[-1] == "stock:a"


def test_sync_stock_backfill_cache_keeps_existing_rows_when_scan_truncated(
    monkeypatch,
) -> None:
    replace_calls: list[str] = []
    mark_dirty_calls: list[str] = []

    monkeypatch.setattr(
        backfill_cache,
        "try_acquire_worker_job_lock",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        backfill_cache,
        "release_worker_job_lock",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        backfill_cache,
        "load_worker_job_cursor",
        lambda *_args, **_kwargs: "",
    )
    monkeypatch.setattr(
        backfill_cache,
        "save_worker_job_cursor",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        backfill_cache,
        "_select_stock_keys_batch",
        lambda *_args, **_kwargs: ["stock:a"],
    )
    monkeypatch.setattr(
        backfill_cache,
        "_has_more_stock_keys",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        backfill_cache,
        "list_stock_backfill_posts",
        lambda *_args, **_kwargs: [{"post_uid": "old"}],
    )
    monkeypatch.setattr(
        backfill_cache,
        "_build_backfill_candidates_for_stock",
        lambda *_args, **_kwargs: ([], True),
    )
    monkeypatch.setattr(
        backfill_cache,
        "replace_stock_backfill_posts",
        lambda *_args, **_kwargs: _record_replace_call(replace_calls),
    )
    monkeypatch.setattr(
        backfill_cache,
        "mark_stock_dirty",
        lambda *_args, stock_key, **_kwargs: mark_dirty_calls.append(
            str(stock_key or "")
        ),
    )

    stats = backfill_cache.sync_stock_backfill_cache(
        cast(TursoConnection, _FakeConn()),
    )

    assert int(stats.get("processed", 0)) == 1
    assert int(stats.get("written", 0)) == 0
    assert replace_calls == []
    assert mark_dirty_calls == []
