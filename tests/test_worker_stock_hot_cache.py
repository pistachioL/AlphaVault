from __future__ import annotations

from typing import cast

import pytest

from alphavault.constants import SCHEMA_WEIBO
from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.postgres_db import PostgresConnection
from alphavault.research_stock_cache import (
    dirty_reason_mask_for,
)
from alphavault.worker import research_stock_cache as stock_hot_cache

_ASSERTION_ID = "weibo:worker_stock_hot_cache:1#1"
_POST_UID = "weibo:worker_stock_hot_cache:1"


class _FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_list_missing_hot_cache_stock_keys_reads_assertion_entities(pg_conn) -> None:
    apply_cloud_schema(pg_conn, target="source", schema_name=SCHEMA_WEIBO)
    conn = PostgresConnection(pg_conn, schema_name=SCHEMA_WEIBO)
    conn.execute(
        f"""
        INSERT INTO weibo.assertions(
            assertion_id, post_uid, idx, action, action_strength, summary, evidence
        )
        VALUES (
            '{_ASSERTION_ID}', '{_POST_UID}', 1, 'trade.buy', 1, '小仓试错', '原文'
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO weibo.assertion_entities(
            assertion_id, entity_key, entity_type, match_source, is_primary
        )
        VALUES ('{_ASSERTION_ID}', 'stock:601899.SH', 'stock', 'stock_code', 1)
        """
    )

    keys = stock_hot_cache._list_missing_hot_cache_stock_keys(conn, limit=10)

    assert keys == ["stock:601899.SH"]


def test_sync_stock_hot_cache_only_consumes_dirty_entries(monkeypatch) -> None:
    removed: list[tuple[list[str], str]] = []

    def _remove_entity_page_dirty_keys(
        _conn: object,
        *,
        stock_keys: list[str],
        claim_until: str = "",
    ) -> int:
        removed.append((list(stock_keys), claim_until))
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
        "claim_entity_page_dirty_entries",
        lambda *_args, **_kwargs: [
            {
                "stock_key": "stock:601899.SH",
                "reason_mask": dirty_reason_mask_for("ai_done"),
                "claim_until": "2026-04-04 10:10:00+08:00",
                "attempt_count": 0,
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
        "remove_entity_page_dirty_keys",
        _remove_entity_page_dirty_keys,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "list_entity_page_dirty_keys",
        lambda *_args, **_kwargs: [],
    )

    stats = stock_hot_cache.sync_stock_hot_cache(
        cast(PostgresConnection, _FakeConn()),
        max_stocks_per_run=4,
        dirty_limit=16,
    )

    assert int(stats.get("processed", 0)) == 1
    assert bool(stats.get("has_more", True)) is False
    assert removed == [(["stock:601899.SH"], "2026-04-04 10:10:00+08:00")]


def test_sync_stock_hot_cache_bootstraps_when_dirty_queue_is_empty(
    monkeypatch,
) -> None:
    removed: list[tuple[list[str], str]] = []

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
        "claim_entity_page_dirty_entries",
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

    def _remove_entity_page_dirty_keys(
        _conn: object,
        *,
        stock_keys: list[str],
        claim_until: str = "",
    ) -> int:
        removed.append((list(stock_keys), claim_until))
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
        cast(PostgresConnection, _FakeConn()),
        max_stocks_per_run=4,
        dirty_limit=16,
    )

    assert int(stats.get("processed", 0)) == 1
    assert int(stats.get("written", 0)) == 1
    assert removed == [(["stock:601899.SH"], "")]


def test_sync_stock_hot_cache_yields_to_rss_after_current_stock(monkeypatch) -> None:
    removed: list[tuple[list[str], str]] = []
    released: list[tuple[list[str], str]] = []

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
        "claim_entity_page_dirty_entries",
        lambda *_args, **_kwargs: [
            {
                "stock_key": "stock:1",
                "reason_mask": dirty_reason_mask_for("ai_done"),
                "claim_until": "2026-04-04 10:10:00+08:00",
                "attempt_count": 0,
            },
            {
                "stock_key": "stock:2",
                "reason_mask": dirty_reason_mask_for("ai_done"),
                "claim_until": "2026-04-04 10:10:00+08:00",
                "attempt_count": 0,
            },
        ],
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "refresh_stock_hot_for_key",
        lambda _conn, **kwargs: str(kwargs.get("stock_key") or ""),
    )

    def _remove_entity_page_dirty_keys(
        _conn: object,
        *,
        stock_keys: list[str],
        claim_until: str = "",
    ) -> int:
        removed.append((list(stock_keys), claim_until))
        return len(stock_keys)

    monkeypatch.setattr(
        stock_hot_cache,
        "remove_entity_page_dirty_keys",
        _remove_entity_page_dirty_keys,
    )

    def _release_entity_page_dirty_claims(
        _conn: object,
        *,
        stock_keys: list[str],
        claim_until: str,
    ) -> int:
        released.append((list(stock_keys), claim_until))
        return len(stock_keys)

    monkeypatch.setattr(
        stock_hot_cache,
        "release_entity_page_dirty_claims",
        _release_entity_page_dirty_claims,
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "list_entity_page_dirty_keys",
        lambda *_args, **_kwargs: ["stock:2"],
    )

    stats = stock_hot_cache.sync_stock_hot_cache(
        cast(PostgresConnection, _FakeConn()),
        max_stocks_per_run=4,
        dirty_limit=16,
        should_continue=lambda: False,
    )

    assert int(stats.get("processed", 0)) == 1
    assert bool(stats.get("has_more", False)) is True
    assert removed == [(["stock:1"], "2026-04-04 10:10:00+08:00")]
    assert released == [(["stock:2"], "2026-04-04 10:10:00+08:00")]


def test_sync_stock_hot_cache_handles_sector_keys(monkeypatch) -> None:
    removed: list[tuple[list[str], str]] = []
    hot_calls: list[str] = []

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
        "claim_entity_page_dirty_entries",
        lambda *_args, **_kwargs: [
            {
                "stock_key": "cluster:white_liquor",
                "reason_mask": dirty_reason_mask_for("ai_done"),
                "claim_until": "2026-04-04 10:10:00+08:00",
                "attempt_count": 0,
            }
        ],
    )

    def _refresh_stock_hot_for_key(_conn: object, **kwargs: object) -> str:
        stock_key = str(kwargs.get("stock_key") or "")
        hot_calls.append(stock_key)
        return stock_key

    monkeypatch.setattr(
        stock_hot_cache,
        "refresh_stock_hot_for_key",
        _refresh_stock_hot_for_key,
    )

    def _remove_entity_page_dirty_keys(
        _conn: object,
        *,
        stock_keys: list[str],
        claim_until: str = "",
    ) -> int:
        removed.append((list(stock_keys), claim_until))
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
        cast(PostgresConnection, _FakeConn()),
        max_stocks_per_run=4,
        dirty_limit=16,
    )

    assert int(stats.get("processed", 0)) == 1
    assert hot_calls == ["cluster:white_liquor"]
    assert removed == [(["cluster:white_liquor"], "2026-04-04 10:10:00+08:00")]


def test_sync_stock_hot_cache_marks_claim_failed_when_refresh_raises(
    monkeypatch,
) -> None:
    failed: list[tuple[list[str], str]] = []

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
        "claim_entity_page_dirty_entries",
        lambda *_args, **_kwargs: [
            {
                "stock_key": "stock:601899.SH",
                "reason_mask": dirty_reason_mask_for("ai_done"),
                "claim_until": "2026-04-04 10:10:00+08:00",
                "attempt_count": 0,
            }
        ],
    )
    monkeypatch.setattr(
        stock_hot_cache,
        "refresh_stock_hot_for_key",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    def _fail_entity_page_dirty_claims(
        _conn: object,
        *,
        stock_keys: list[str],
        claim_until: str,
    ) -> int:
        failed.append((list(stock_keys), claim_until))
        return len(stock_keys)

    monkeypatch.setattr(
        stock_hot_cache,
        "fail_entity_page_dirty_claims",
        _fail_entity_page_dirty_claims,
    )

    with pytest.raises(RuntimeError, match="boom"):
        stock_hot_cache.sync_stock_hot_cache(
            cast(PostgresConnection, _FakeConn()),
            max_stocks_per_run=4,
            dirty_limit=16,
        )

    assert failed == [(["stock:601899.SH"], "2026-04-04 10:10:00+08:00")]
