from __future__ import annotations

import libsql

from alphavault.db.turso_db import TursoConnection
from alphavault.research_backfill_cache import (
    ensure_research_backfill_cache_schema,
    list_stock_backfill_posts,
    replace_stock_backfill_posts,
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
