from __future__ import annotations

import libsql
from typing import cast

from alphavault.db.turso_db import TursoConnection
from alphavault.research_stock_cache import ensure_research_stock_cache_schema
from alphavault.worker import research_stock_cache as stock_hot_cache


class _FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_list_missing_hot_cache_stock_keys_reads_assertion_entities() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        conn.execute(
            """
            CREATE TABLE assertions(
                post_uid TEXT NOT NULL,
                idx INTEGER NOT NULL,
                topic_key TEXT NOT NULL,
                action TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE assertion_entities(
                post_uid TEXT NOT NULL,
                assertion_idx INTEGER NOT NULL,
                entity_idx INTEGER NOT NULL,
                entity_key TEXT NOT NULL,
                entity_type TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO assertions(post_uid, idx, topic_key, action)
            VALUES ('weibo:1', 1, 'stock:紫金', 'trade.buy')
            """
        )
        conn.execute(
            """
            INSERT INTO assertion_entities(post_uid, assertion_idx, entity_idx, entity_key, entity_type)
            VALUES ('weibo:1', 1, 1, 'stock:601899.SH', 'stock')
            """
        )

        keys = stock_hot_cache._list_missing_hot_cache_stock_keys(conn, limit=10)

        assert keys == ["stock:601899.SH"]
    finally:
        conn.close()


def test_sync_stock_hot_cache_only_consumes_dirty_entries(monkeypatch) -> None:
    removed: list[str] = []

    def _remove_entity_page_dirty_keys(_conn: object, *, stock_keys: list[str]) -> int:
        removed.extend(stock_keys)
        return len(stock_keys)

    monkeypatch.setattr(
        stock_hot_cache,
        "try_acquire_worker_job_lock",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "release_worker_job_lock",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "ensure_research_stock_cache_schema",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "list_entity_page_dirty_entries",
        lambda *_args, **_kwargs: [
            {
                "stock_key": "stock:601899.SH",
                "reason": "ai_done",
                "updated_at": "2026-03-27 10:00:00",
            }
        ],
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "refresh_stock_hot_for_key",
        lambda _conn, **kwargs: str(kwargs.get("stock_key") or ""),
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "refresh_stock_extras_snapshot_for_key",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "remove_entity_page_dirty_keys",
        _remove_entity_page_dirty_keys,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "list_entity_page_dirty_keys",
        lambda *_args, **_kwargs: [],
    )

    stats = stock_hot_cache.sync_stock_hot_cache(
        cast(TursoConnection, _FakeConn()),
        max_stocks_per_run=4,
        dirty_limit=16,
    )

    assert int(stats.get("processed", 0)) == 1
    assert bool(stats.get("has_more", True)) is False
    assert removed == ["stock:601899.SH"]


def test_refresh_stock_extras_snapshot_respects_min_interval(monkeypatch) -> None:
    saved: list[str] = []

    monkeypatch.setattr(
        stock_hot_cache,
        "load_entity_page_backfill_snapshot",
        lambda *_args, **_kwargs: {"updated_at": "2099-01-01 00:00:00"},
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "list_pending_candidates_for_left_key",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("stock extras 不该再读 pending candidates")
        ),
        raising=False,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "list_stock_backfill_posts",
        lambda *_args, **_kwargs: [{"post_uid": "weibo:1"}],
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "save_entity_page_backfill_snapshot",
        lambda _conn, *, stock_key, **_kwargs: saved.append(stock_key),
    )

    skipped = stock_hot_cache.refresh_stock_extras_snapshot_for_key(
        cast(TursoConnection, _FakeConn()),
        stock_key="stock:601899.SH",
        min_refresh_seconds=900,
        force=False,
    )
    forced = stock_hot_cache.refresh_stock_extras_snapshot_for_key(
        cast(TursoConnection, _FakeConn()),
        stock_key="stock:601899.SH",
        min_refresh_seconds=900,
        force=True,
    )

    assert skipped is False
    assert forced is True
    assert saved == ["stock:601899.SH"]


def test_refresh_stock_extras_snapshot_for_key_writes_payload(monkeypatch) -> None:
    saved_args: list[tuple[str, list[dict[str, object]]]] = []

    monkeypatch.setattr(
        stock_hot_cache,
        "load_entity_page_backfill_snapshot",
        lambda *_args, **_kwargs: {"updated_at": ""},
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "list_pending_candidates_for_left_key",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("stock extras 不该再读 pending candidates")
        ),
        raising=False,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "list_stock_backfill_posts",
        lambda *_args, **_kwargs: [{"post_uid": "weibo:2"}],
    )

    def _capture_save(
        _conn,
        *,
        stock_key: str,
        backfill_posts: list[dict[str, object]],
    ) -> None:
        saved_args.append((stock_key, backfill_posts))

    monkeypatch.setattr(
        stock_hot_cache,
        "save_entity_page_backfill_snapshot",
        _capture_save,
    )

    refreshed = stock_hot_cache.refresh_stock_extras_snapshot_for_key(
        cast(TursoConnection, _FakeConn()),
        stock_key="stock:601899.SH",
        min_refresh_seconds=900,
        force=False,
    )

    assert refreshed is True
    assert saved_args == [
        (
            "stock:601899.SH",
            [{"post_uid": "weibo:2"}],
        )
    ]


def test_sync_stock_hot_cache_bootstraps_when_dirty_queue_is_empty(
    monkeypatch,
) -> None:
    removed: list[str] = []

    monkeypatch.setattr(
        stock_hot_cache,
        "try_acquire_worker_job_lock",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "release_worker_job_lock",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "ensure_research_stock_cache_schema",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "list_entity_page_dirty_entries",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "_list_missing_hot_cache_stock_keys",
        lambda *_args, **_kwargs: ["stock:601899.SH"],
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "refresh_stock_hot_for_key",
        lambda _conn, **kwargs: str(kwargs.get("stock_key") or ""),
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "refresh_stock_extras_snapshot_for_key",
        lambda *_args, **_kwargs: True,
    )

    def _remove_entity_page_dirty_keys(_conn: object, *, stock_keys: list[str]) -> int:
        removed.extend(stock_keys)
        return len(stock_keys)

    monkeypatch.setattr(
        stock_hot_cache,
        "remove_entity_page_dirty_keys",
        _remove_entity_page_dirty_keys,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "list_entity_page_dirty_keys",
        lambda *_args, **_kwargs: [],
    )

    stats = stock_hot_cache.sync_stock_hot_cache(
        cast(TursoConnection, _FakeConn()),
        max_stocks_per_run=4,
        dirty_limit=16,
    )

    assert int(stats.get("processed", 0)) == 1
    assert int(stats.get("written", 0)) == 1
    assert removed == ["stock:601899.SH"]


def test_sync_stock_hot_cache_yields_to_rss_after_current_stock(monkeypatch) -> None:
    removed: list[str] = []

    monkeypatch.setattr(
        stock_hot_cache,
        "try_acquire_worker_job_lock",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "release_worker_job_lock",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "ensure_research_stock_cache_schema",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "list_entity_page_dirty_entries",
        lambda *_args, **_kwargs: [
            {"stock_key": "stock:1", "reason": "ai_done", "updated_at": "2026-03-27"},
            {"stock_key": "stock:2", "reason": "ai_done", "updated_at": "2026-03-27"},
        ],
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "refresh_stock_hot_for_key",
        lambda _conn, **kwargs: str(kwargs.get("stock_key") or ""),
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "refresh_stock_extras_snapshot_for_key",
        lambda *_args, **_kwargs: False,
    )

    def _remove_entity_page_dirty_keys(_conn: object, *, stock_keys: list[str]) -> int:
        removed.extend(stock_keys)
        return len(stock_keys)

    monkeypatch.setattr(
        stock_hot_cache,
        "remove_entity_page_dirty_keys",
        _remove_entity_page_dirty_keys,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "list_entity_page_dirty_keys",
        lambda *_args, **_kwargs: ["stock:2"],
    )

    stats = stock_hot_cache.sync_stock_hot_cache(
        cast(TursoConnection, _FakeConn()),
        max_stocks_per_run=4,
        dirty_limit=16,
        should_continue=lambda: False,
    )

    assert int(stats.get("processed", 0)) == 1
    assert bool(stats.get("has_more", False)) is True
    assert removed == ["stock:1"]
