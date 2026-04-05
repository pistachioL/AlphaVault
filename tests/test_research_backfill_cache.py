from __future__ import annotations

import libsql

from alphavault.db.cloud_schema import (
    apply_cloud_schema as ensure_research_backfill_cache_schema,
)
from alphavault.db.turso_db import TursoConnection
from alphavault.research_backfill_cache import (
    list_stock_backfill_dirty_keys,
    list_stock_backfill_posts,
    load_stock_backfill_meta,
    mark_stock_backfill_dirty_from_assertions,
    remove_stock_backfill_dirty_keys,
    replace_stock_backfill_posts,
    save_stock_backfill_meta,
)


def test_replace_and_list_stock_backfill_posts() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_backfill_cache_schema(conn)
        written = replace_stock_backfill_posts(
            conn,
            stock_key="stock:601899.SH",
            posts=[
                {
                    "post_uid": "p1",
                    "author": "alice",
                    "created_at": "2026-03-25 10:00:00",
                    "url": "https://example.com/p1",
                    "matched_terms": "紫金矿业",
                    "preview": "先买一点紫金矿业",
                },
                {
                    "post_uid": "p2",
                    "author": "bob",
                    "created_at": "2026-03-26 11:00:00",
                    "url": "https://example.com/p2",
                    "matched_terms": "紫金矿业",
                    "preview": "我觉得紫金矿业这里先别急",
                },
            ],
        )
        assert written == 2

        rows = list_stock_backfill_posts(conn, stock_key="stock:601899.SH", limit=10)
        assert [row["post_uid"] for row in rows] == ["p2", "p1"]
        assert rows[0]["author"] == "bob"
        assert "紫金矿业" in str(rows[0]["matched_terms"])
    finally:
        conn.close()


def test_mark_list_and_remove_backfill_dirty_keys() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_backfill_cache_schema(conn)
        marked = mark_stock_backfill_dirty_from_assertions(
            conn,
            assertions=[
                {
                    "topic_key": "stock:紫金",
                    "stock_codes_json": '["601899.SH"]',
                    "assertion_entities": [
                        {
                            "entity_key": "stock:601899.SH",
                            "entity_type": "stock",
                        }
                    ],
                },
                {
                    "topic_key": "stock:阿紫",
                    "stock_codes_json": "[]",
                    "assertion_entities": [],
                },
                {"topic_key": "cluster:gold", "stock_codes_json": '["600519.SH"]'},
            ],
            reason="ai_done",
        )
        assert marked == 1

        keys = list_stock_backfill_dirty_keys(conn, limit=10)
        assert set(keys) == {"stock:601899.SH"}

        removed = remove_stock_backfill_dirty_keys(
            conn,
            stock_keys=["stock:601899.SH"],
        )
        assert removed == 1
        assert list_stock_backfill_dirty_keys(conn, limit=10) == []
    finally:
        conn.close()


def test_save_and_load_stock_backfill_meta() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_backfill_cache_schema(conn)
        save_stock_backfill_meta(
            conn,
            stock_key="stock:601899.SH",
            signature="sig-1",
            row_count=7,
        )
        meta = load_stock_backfill_meta(conn, stock_key="stock:601899.SH")
        assert meta["stock_key"] == "stock:601899.SH"
        assert meta["signature"] == "sig-1"
        assert meta["row_count"] == 7
        assert str(meta["updated_at"]).strip() != ""
    finally:
        conn.close()
