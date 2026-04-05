from __future__ import annotations

import libsql
from typing import cast

from alphavault.db.cloud_schema import (
    apply_cloud_schema as ensure_research_stock_cache_schema,
)
from alphavault.db.turso_db import TursoConnection
from alphavault.research_stock_cache import (
    claim_entity_page_dirty_entries,
    dirty_reason_mask_for,
    fail_entity_page_dirty_claims,
    list_entity_page_dirty_keys,
    load_entity_page_backfill_snapshot,
    load_entity_page_signal_snapshot,
    mark_entity_page_dirty,
    mark_entity_page_dirty_from_assertions,
    remove_entity_page_dirty_keys,
    save_entity_page_backfill_snapshot,
    save_entity_page_signal_snapshot,
)


def test_save_and_load_entity_page_signal_snapshot() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        save_entity_page_signal_snapshot(
            conn,
            stock_key="stock:601899.SH",
            payload={
                "entity_key": "stock:601899.SH",
                "header_title": "紫金矿业 (601899.SH)",
                "signal_total": 2,
                "signals": [
                    {
                        "post_uid": "weibo:2",
                        "summary": "继续拿着",
                        "action": "trade.hold",
                        "author": "alice",
                        "created_at": "2026-03-26 10:00:00",
                        "created_at_line": "2026-03-26 10:00 · 1小时前",
                        "raw_text": "原文2",
                        "display_md": "",
                        "tree_label": "主贴",
                        "tree_text": "root -> child",
                    },
                    {
                        "post_uid": "weibo:1",
                        "summary": "小仓试错",
                        "action": "trade.buy",
                        "author": "bob",
                        "created_at": "2026-03-25 10:00:00",
                        "created_at_line": "2026-03-25 10:00 · 1天前",
                        "raw_text": "原文1",
                        "display_md": "",
                        "tree_label": "",
                        "tree_text": "",
                    },
                ],
                "related_sectors": [{"sector_key": "gold", "mention_count": "2"}],
            },
        )
        loaded = load_entity_page_signal_snapshot(
            conn,
            stock_key="stock:601899.SH",
        )
        assert cast(str, loaded["entity_key"]) == "stock:601899.SH"
        assert cast(str, loaded["header_title"]) == "紫金矿业 (601899.SH)"
        assert cast(int, loaded["signal_total"]) == 2
        signals = cast(list[dict[str, str]], loaded["signals"])
        related_sectors = cast(list[dict[str, str]], loaded["related_sectors"])
        assert signals[0]["post_uid"] == "weibo:2"
        assert signals[0]["tree_text"] == "root -> child"
        assert related_sectors[0]["sector_key"] == "gold"
    finally:
        conn.close()


def test_save_and_load_entity_page_signal_snapshot_for_sector() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        save_entity_page_signal_snapshot(
            conn,
            stock_key="cluster:white_liquor",
            payload={
                "entity_key": "cluster:white_liquor",
                "header_title": "white_liquor",
                "signal_total": 1,
                "signals": [
                    {
                        "post_uid": "weibo:3",
                        "summary": "板块继续走强",
                        "action": "trade.buy",
                        "author": "alice",
                        "created_at": "2026-03-26 10:00:00",
                    }
                ],
                "related_stocks": [
                    {"stock_key": "stock:600519.SH", "mention_count": "2"}
                ],
            },
        )
        loaded = load_entity_page_signal_snapshot(
            conn,
            stock_key="cluster:white_liquor",
        )
        assert cast(str, loaded["entity_key"]) == "cluster:white_liquor"
        assert cast(str, loaded["header_title"]) == "white_liquor"
        assert cast(int, loaded["signal_total"]) == 1
        related_stocks = cast(list[dict[str, str]], loaded["related_stocks"])
        assert related_stocks[0]["stock_key"] == "stock:600519.SH"
    finally:
        conn.close()


def test_save_and_load_entity_page_backfill_snapshot() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        save_entity_page_backfill_snapshot(
            conn,
            stock_key="stock:601899.SH",
            backfill_posts=[{"post_uid": "weibo:9"}],
        )
        loaded = load_entity_page_backfill_snapshot(
            conn,
            stock_key="stock:601899.SH",
        )
        backfill_posts = cast(list[dict[str, str]], loaded["backfill_posts"])
        assert "pending_candidates" not in loaded
        assert backfill_posts[0]["post_uid"] == "weibo:9"
        assert str(loaded["updated_at"]).strip() != ""
    finally:
        conn.close()


def test_entity_page_signal_snapshot_skips_write_when_content_unchanged(
    monkeypatch,
) -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        timestamps = iter(
            [
                "2026-04-04 10:00:00",
                "2026-04-04 10:00:01",
                "2026-04-04 10:00:02",
            ]
        )
        monkeypatch.setattr(
            "alphavault.research_stock_cache._now_str",
            lambda: next(timestamps),
        )
        payload = {
            "entity_key": "stock:601899.SH",
            "header_title": "紫金矿业 (601899.SH)",
            "signal_total": 1,
            "signals": [{"post_uid": "weibo:1", "summary": "继续拿着"}],
            "related_sectors": [{"sector_key": "gold"}],
        }
        save_entity_page_signal_snapshot(
            conn,
            stock_key="stock:601899.SH",
            payload=payload,
        )
        first = (
            conn.execute(
                """
SELECT updated_at, content_hash
FROM entity_page_snapshot
WHERE entity_key = :entity_key
""",
                {"entity_key": "stock:601899.SH"},
            )
            .mappings()
            .fetchone()
        )
        assert first is not None

        save_entity_page_signal_snapshot(
            conn,
            stock_key="stock:601899.SH",
            payload=payload,
        )
        second = (
            conn.execute(
                """
SELECT updated_at, content_hash
FROM entity_page_snapshot
WHERE entity_key = :entity_key
""",
                {"entity_key": "stock:601899.SH"},
            )
            .mappings()
            .fetchone()
        )
        assert second == first

        save_entity_page_signal_snapshot(
            conn,
            stock_key="stock:601899.SH",
            payload={
                **payload,
                "signal_total": 2,
                "signals": [
                    {"post_uid": "weibo:1", "summary": "继续拿着"},
                    {"post_uid": "weibo:2", "summary": "小仓试错"},
                ],
            },
        )
        third = (
            conn.execute(
                """
SELECT updated_at, content_hash
FROM entity_page_snapshot
WHERE entity_key = :entity_key
""",
                {"entity_key": "stock:601899.SH"},
            )
            .mappings()
            .fetchone()
        )
        assert third is not None
        assert third["updated_at"] == "2026-04-04 10:00:01"
        assert third["content_hash"] != first["content_hash"]
    finally:
        conn.close()


def test_entity_page_backfill_snapshot_skips_write_when_content_unchanged(
    monkeypatch,
) -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        timestamps = iter(
            [
                "2026-04-04 11:00:00",
                "2026-04-04 11:00:01",
                "2026-04-04 11:00:02",
            ]
        )
        monkeypatch.setattr(
            "alphavault.research_stock_cache._now_str",
            lambda: next(timestamps),
        )
        rows = [{"post_uid": "weibo:9", "summary": "补找结果"}]
        save_entity_page_backfill_snapshot(
            conn,
            stock_key="stock:601899.SH",
            backfill_posts=rows,
        )
        first = (
            conn.execute(
                """
SELECT updated_at, content_hash
FROM entity_page_snapshot
WHERE entity_key = :entity_key
""",
                {"entity_key": "stock:601899.SH"},
            )
            .mappings()
            .fetchone()
        )
        assert first is not None

        save_entity_page_backfill_snapshot(
            conn,
            stock_key="stock:601899.SH",
            backfill_posts=rows,
        )
        second = (
            conn.execute(
                """
SELECT updated_at, content_hash
FROM entity_page_snapshot
WHERE entity_key = :entity_key
""",
                {"entity_key": "stock:601899.SH"},
            )
            .mappings()
            .fetchone()
        )
        assert second == first

        save_entity_page_backfill_snapshot(
            conn,
            stock_key="stock:601899.SH",
            backfill_posts=[
                {"post_uid": "weibo:9", "summary": "补找结果"},
                {"post_uid": "weibo:10", "summary": "第二条"},
            ],
        )
        third = (
            conn.execute(
                """
SELECT updated_at, content_hash
FROM entity_page_snapshot
WHERE entity_key = :entity_key
""",
                {"entity_key": "stock:601899.SH"},
            )
            .mappings()
            .fetchone()
        )
        assert third is not None
        assert third["updated_at"] == "2026-04-04 11:00:01"
        assert third["content_hash"] != first["content_hash"]
    finally:
        conn.close()


def test_stock_page_snapshot_uses_single_entity_page_snapshot_table() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        save_entity_page_signal_snapshot(
            conn,
            stock_key="stock:601899.SH",
            payload={
                "entity_key": "stock:601899.SH",
                "header_title": "紫金矿业 (601899.SH)",
                "signal_total": 1,
                "signals": [{"post_uid": "weibo:1"}],
                "related_sectors": [{"sector_key": "gold"}],
            },
        )
        save_entity_page_backfill_snapshot(
            conn,
            stock_key="stock:601899.SH",
            backfill_posts=[{"post_uid": "weibo:9"}],
        )

        table_names = {
            str(row["name"])
            for row in conn.execute(
                """
SELECT name
FROM sqlite_schema
WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
"""
            )
            .mappings()
            .all()
        }
        assert "entity_page_snapshot" in table_names
        assert "research_stock_signals_hot" not in table_names
        assert "research_stock_extras_snapshot" not in table_names

        rows = (
            conn.execute(
                """
SELECT entity_key, signal_total, backfill_posts_json
FROM entity_page_snapshot
ORDER BY entity_key ASC
"""
            )
            .mappings()
            .all()
        )
        assert len(rows) == 1
        assert rows[0]["entity_key"] == "stock:601899.SH"
        assert rows[0]["signal_total"] == 1

        hot = load_entity_page_signal_snapshot(conn, stock_key="stock:601899.SH")
        extras = load_entity_page_backfill_snapshot(
            conn,
            stock_key="stock:601899.SH",
        )
        assert cast(int, hot["signal_total"]) == 1
        assert cast(list[dict[str, str]], hot["signals"])[0]["post_uid"] == "weibo:1"
        assert cast(list[dict[str, str]], extras["backfill_posts"])[0]["post_uid"] == (
            "weibo:9"
        )
    finally:
        conn.close()


def test_mark_entity_page_dirty_merges_reason_mask_and_keeps_dirty_since(
    monkeypatch,
) -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        timestamps = iter(
            [
                "2026-04-04 10:00:00+08:00",
                "2026-04-04 10:05:00+08:00",
            ]
        )
        monkeypatch.setattr(
            "alphavault.research_stock_cache._now_str",
            lambda: next(timestamps),
        )

        mark_entity_page_dirty(conn, stock_key="stock:601899.SH", reason="ai_done")
        mark_entity_page_dirty(
            conn,
            stock_key="stock:601899.SH",
            reason="backfill_cache",
        )

        row = (
            conn.execute(
                """
SELECT reason_mask, dirty_since, last_dirty_at, claim_until, attempt_count
FROM projection_dirty
WHERE job_type = 'entity_page' AND target_key = 'stock:601899.SH'
"""
            )
            .mappings()
            .fetchone()
        )
        assert row is not None
        assert int(row["reason_mask"] or 0) == (
            dirty_reason_mask_for("ai_done") | dirty_reason_mask_for("backfill_cache")
        )
        assert str(row["dirty_since"]) == "2026-04-04 10:00:00+08:00"
        assert str(row["last_dirty_at"]) == "2026-04-04 10:05:00+08:00"
        assert str(row["claim_until"]) == ""
        assert int(row["attempt_count"] or 0) == 0
    finally:
        conn.close()


def test_claim_and_fail_entity_page_dirty_entries_track_attempts() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        mark_entity_page_dirty(conn, stock_key="stock:601899.SH", reason="ai_done")

        claimed = claim_entity_page_dirty_entries(
            conn,
            limit=10,
            claim_ttl_seconds=600,
        )
        assert len(claimed) == 1
        assert str(claimed[0]["stock_key"]) == "stock:601899.SH"
        assert int(claimed[0]["reason_mask"]) == dirty_reason_mask_for("ai_done")
        claim_until = str(claimed[0]["claim_until"])
        assert claim_until != ""
        assert (
            claim_entity_page_dirty_entries(
                conn,
                limit=10,
                claim_ttl_seconds=600,
            )
            == []
        )

        failed = fail_entity_page_dirty_claims(
            conn,
            stock_keys=["stock:601899.SH"],
            claim_until=claim_until,
        )
        assert failed == 1
        row = (
            conn.execute(
                """
SELECT attempt_count, claim_until
FROM projection_dirty
WHERE job_type = 'entity_page' AND target_key = 'stock:601899.SH'
"""
            )
            .mappings()
            .fetchone()
        )
        assert row is not None
        assert int(row["attempt_count"] or 0) == 1
        assert str(row["claim_until"]) == ""
    finally:
        conn.close()


def test_mark_list_and_remove_entity_page_dirty_keys() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        mark_entity_page_dirty(conn, stock_key="stock:601899.SH", reason="rss")
        mark_entity_page_dirty(conn, stock_key="stock:600519.SH", reason="ai")
        keys = list_entity_page_dirty_keys(conn, limit=10)
        assert set(keys) == {"stock:601899.SH", "stock:600519.SH"}
        remove_entity_page_dirty_keys(conn, stock_keys=["stock:601899.SH"])
        keys_after = list_entity_page_dirty_keys(conn, limit=10)
        assert keys_after == ["stock:600519.SH"]
    finally:
        conn.close()


def test_stock_dirty_uses_projection_dirty_table() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        mark_entity_page_dirty(conn, stock_key="stock:601899.SH", reason="rss")

        table_names = {
            str(row["name"])
            for row in conn.execute(
                """
SELECT name
FROM sqlite_schema
WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
"""
            )
            .mappings()
            .all()
        }
        assert "projection_dirty" in table_names
        assert "research_stock_dirty_keys" not in table_names

        rows = (
            conn.execute(
                """
SELECT job_type, target_key, reason_mask, dirty_since, last_dirty_at, claim_until, attempt_count
FROM projection_dirty
ORDER BY job_type ASC, target_key ASC
"""
            )
            .mappings()
            .all()
        )
        assert rows == [
            {
                "job_type": "entity_page",
                "target_key": "stock:601899.SH",
                "reason_mask": dirty_reason_mask_for("rss"),
                "dirty_since": str(rows[0]["dirty_since"]),
                "last_dirty_at": str(rows[0]["last_dirty_at"]),
                "claim_until": "",
                "attempt_count": 0,
            }
        ]
    finally:
        conn.close()


def test_mark_entity_page_dirty_from_assertions_reads_stock_and_sector_entities() -> (
    None
):
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        marked = mark_entity_page_dirty_from_assertions(
            conn,
            assertions=[
                {
                    "topic_key": "stock:紫金",
                    "stock_codes_json": '["601899.SH"]',
                    "assertion_entities": [
                        {
                            "entity_key": "stock:601899.SH",
                            "entity_type": "stock",
                        },
                        {
                            "entity_key": "industry:黄金",
                            "entity_type": "industry",
                        },
                    ],
                    "cluster_keys_json": '["gold"]',
                },
                {
                    "topic_key": "stock:阿紫",
                    "stock_codes_json": "[]",
                    "assertion_entities": [],
                },
                {"topic_key": "cluster:energy", "stock_codes_json": '["600519.SH"]'},
            ],
            reason="ai",
        )
        assert marked == 3
        keys = set(list_entity_page_dirty_keys(conn, limit=10))
        assert keys == {"stock:601899.SH", "cluster:gold", "cluster:energy"}
    finally:
        conn.close()
