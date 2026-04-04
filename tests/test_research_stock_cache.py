from __future__ import annotations

import libsql
from typing import cast

from alphavault.db.turso_db import TursoConnection
from alphavault.research_stock_cache import (
    ensure_research_stock_cache_schema,
    list_stock_dirty_keys,
    load_stock_extras_snapshot,
    load_stock_hot_view,
    mark_stock_dirty,
    mark_stock_dirty_from_assertions,
    remove_stock_dirty_keys,
    save_stock_extras_snapshot,
    save_stock_hot_view,
)


def test_save_and_load_stock_hot_view() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        save_stock_hot_view(
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
        loaded = load_stock_hot_view(conn, stock_key="stock:601899.SH")
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


def test_save_and_load_stock_extras_snapshot() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        save_stock_extras_snapshot(
            conn,
            stock_key="stock:601899.SH",
            backfill_posts=[{"post_uid": "weibo:9"}],
        )
        loaded = load_stock_extras_snapshot(conn, stock_key="stock:601899.SH")
        backfill_posts = cast(list[dict[str, str]], loaded["backfill_posts"])
        assert "pending_candidates" not in loaded
        assert backfill_posts[0]["post_uid"] == "weibo:9"
        assert str(loaded["updated_at"]).strip() != ""
    finally:
        conn.close()


def test_stock_page_snapshot_uses_single_entity_page_snapshot_table() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        save_stock_hot_view(
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
        save_stock_extras_snapshot(
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

        hot = load_stock_hot_view(conn, stock_key="stock:601899.SH")
        extras = load_stock_extras_snapshot(conn, stock_key="stock:601899.SH")
        assert cast(int, hot["signal_total"]) == 1
        assert cast(list[dict[str, str]], hot["signals"])[0]["post_uid"] == "weibo:1"
        assert cast(list[dict[str, str]], extras["backfill_posts"])[0]["post_uid"] == (
            "weibo:9"
        )
    finally:
        conn.close()


def test_mark_list_and_remove_dirty_keys() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        mark_stock_dirty(conn, stock_key="stock:601899.SH", reason="rss")
        mark_stock_dirty(conn, stock_key="stock:600519.SH", reason="ai")
        keys = list_stock_dirty_keys(conn, limit=10)
        assert set(keys) == {"stock:601899.SH", "stock:600519.SH"}
        remove_stock_dirty_keys(conn, stock_keys=["stock:601899.SH"])
        keys_after = list_stock_dirty_keys(conn, limit=10)
        assert keys_after == ["stock:600519.SH"]
    finally:
        conn.close()


def test_stock_dirty_uses_projection_dirty_table() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        mark_stock_dirty(conn, stock_key="stock:601899.SH", reason="rss")

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
SELECT job_type, target_key, reason
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
                "reason": "rss",
            }
        ]
    finally:
        conn.close()


def test_mark_stock_dirty_from_assertions_reads_stock_entities_only() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_stock_cache_schema(conn)
        marked = mark_stock_dirty_from_assertions(
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
                },
                {
                    "topic_key": "stock:阿紫",
                    "stock_codes_json": "[]",
                    "assertion_entities": [],
                },
                {"topic_key": "cluster:gold", "stock_codes_json": '["600519.SH"]'},
            ],
            reason="ai",
        )
        assert marked == 1
        keys = set(list_stock_dirty_keys(conn, limit=10))
        assert keys == {"stock:601899.SH"}
    finally:
        conn.close()
