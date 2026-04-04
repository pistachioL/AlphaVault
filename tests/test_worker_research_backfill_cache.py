from __future__ import annotations

from typing import cast

from alphavault.db.turso_db import TursoConnection
from alphavault.worker import research_backfill_cache as backfill_cache


class _FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _dirty_key_loader(keys_first: list[str], keys_after: list[str]):
    calls = {"count": 0}

    def _load(*_args, **_kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return list(keys_first)
        return list(keys_after)

    return _load


def test_sync_stock_backfill_cache_yields_to_rss_after_current_stock(
    monkeypatch,
) -> None:
    removed_keys: list[str] = []

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
        "list_stock_backfill_dirty_keys",
        _dirty_key_loader(["stock:a", "stock:b", "stock:c"], []),
    )

    def _remove_dirty_keys(*_args, stock_keys, **_kwargs) -> int:
        removed_keys.extend(stock_keys)
        return len(stock_keys)

    monkeypatch.setattr(
        backfill_cache,
        "remove_stock_backfill_dirty_keys",
        _remove_dirty_keys,
    )
    monkeypatch.setattr(
        backfill_cache,
        "load_stock_backfill_meta",
        lambda *_args, **_kwargs: {},
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
        "save_stock_backfill_meta",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        backfill_cache,
        "mark_entity_page_dirty",
        lambda *_args, **_kwargs: None,
    )

    stats = backfill_cache.sync_stock_backfill_cache(
        cast(TursoConnection, _FakeConn()),
        should_continue=lambda: False,
    )

    assert int(stats.get("processed", 0)) == 1
    assert int(stats.get("changed", 0)) == 1
    assert int(stats.get("written", 0)) == 1
    assert bool(stats.get("has_more", False)) is True
    assert removed_keys == ["stock:a"]


def test_sync_stock_backfill_cache_keeps_existing_rows_when_scan_truncated(
    monkeypatch,
) -> None:
    replace_calls: list[str] = []
    save_meta_calls: list[str] = []
    mark_dirty_calls: list[str] = []
    removed_keys: list[str] = []

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
        "list_stock_backfill_dirty_keys",
        _dirty_key_loader(["stock:a"], []),
    )

    def _remove_dirty_keys(*_args, stock_keys, **_kwargs) -> int:
        removed_keys.extend(stock_keys)
        return len(stock_keys)

    monkeypatch.setattr(
        backfill_cache,
        "remove_stock_backfill_dirty_keys",
        _remove_dirty_keys,
    )
    monkeypatch.setattr(
        backfill_cache,
        "load_stock_backfill_meta",
        lambda *_args, **_kwargs: {"signature": "old", "row_count": 1},
    )
    monkeypatch.setattr(
        backfill_cache,
        "_build_backfill_candidates_for_stock",
        lambda *_args, **_kwargs: ([], True),
    )

    def _replace_posts(*_args, **_kwargs) -> int:
        replace_calls.append("called")
        return 0

    monkeypatch.setattr(
        backfill_cache,
        "replace_stock_backfill_posts",
        _replace_posts,
    )
    monkeypatch.setattr(
        backfill_cache,
        "save_stock_backfill_meta",
        lambda *_args, **_kwargs: save_meta_calls.append("called"),
    )
    monkeypatch.setattr(
        backfill_cache,
        "mark_entity_page_dirty",
        lambda *_args, stock_key, **_kwargs: mark_dirty_calls.append(
            str(stock_key or "")
        ),
    )

    stats = backfill_cache.sync_stock_backfill_cache(
        cast(TursoConnection, _FakeConn()),
    )

    assert int(stats.get("processed", 0)) == 1
    assert int(stats.get("changed", 0)) == 0
    assert int(stats.get("written", 0)) == 0
    assert replace_calls == []
    assert save_meta_calls == []
    assert mark_dirty_calls == []
    assert removed_keys == []


def test_sync_stock_backfill_cache_skips_write_when_signature_unchanged(
    monkeypatch,
) -> None:
    row = {
        "post_uid": "p1",
        "author": "alice",
        "created_at": "2026-03-25 10:00:00",
        "url": "https://example.com/p1",
        "matched_terms": "601899",
        "preview": "preview",
    }
    expected_signature, expected_count = backfill_cache._signature_digest([row])
    replace_calls: list[str] = []
    save_meta_calls: list[str] = []
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
        "list_stock_backfill_dirty_keys",
        _dirty_key_loader(["stock:a"], []),
    )
    monkeypatch.setattr(
        backfill_cache,
        "remove_stock_backfill_dirty_keys",
        lambda *_args, **_kwargs: 1,
    )
    monkeypatch.setattr(
        backfill_cache,
        "load_stock_backfill_meta",
        lambda *_args, **_kwargs: {
            "signature": expected_signature,
            "row_count": expected_count,
        },
    )
    monkeypatch.setattr(
        backfill_cache,
        "_build_backfill_candidates_for_stock",
        lambda *_args, **_kwargs: ([row], False),
    )

    def _replace_posts(*_args, **_kwargs) -> int:
        replace_calls.append("called")
        return 1

    monkeypatch.setattr(
        backfill_cache,
        "replace_stock_backfill_posts",
        _replace_posts,
    )
    monkeypatch.setattr(
        backfill_cache,
        "save_stock_backfill_meta",
        lambda *_args, **_kwargs: save_meta_calls.append("called"),
    )
    monkeypatch.setattr(
        backfill_cache,
        "mark_entity_page_dirty",
        lambda *_args, stock_key, **_kwargs: mark_dirty_calls.append(
            str(stock_key or "")
        ),
    )

    stats = backfill_cache.sync_stock_backfill_cache(
        cast(TursoConnection, _FakeConn()),
    )

    assert int(stats.get("processed", 0)) == 1
    assert int(stats.get("changed", 0)) == 0
    assert int(stats.get("written", 0)) == 0
    assert replace_calls == []
    assert save_meta_calls == []
    assert mark_dirty_calls == []
