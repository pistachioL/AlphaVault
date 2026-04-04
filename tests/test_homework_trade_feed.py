from __future__ import annotations

import libsql

from alphavault.db.turso_db import TursoConnection
from alphavault.homework_trade_feed import (
    HOMEWORK_DEFAULT_VIEW_KEY,
    HOMEWORK_TRADE_FEED_TABLE,
    ensure_homework_trade_feed_schema,
    load_homework_trade_feed,
    save_homework_trade_feed,
)


def test_homework_trade_feed_schema_and_roundtrip() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_homework_trade_feed_schema(conn)
        save_homework_trade_feed(
            conn,
            view_key=HOMEWORK_DEFAULT_VIEW_KEY,
            caption="窗口：最近 3 天：2026-04-02 ~ 2026-04-04",
            used_window_days=3,
            rows=[
                {
                    "topic": "stock:600519.SH",
                    "summary": "小仓试错",
                    "recent_action": "trade.buy · 中等偏强 · 强度 2",
                    "tree_post_uid": "weibo:1",
                }
            ],
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
        assert HOMEWORK_TRADE_FEED_TABLE in table_names

        row = (
            conn.execute(
                f"""
SELECT view_key
FROM {HOMEWORK_TRADE_FEED_TABLE}
WHERE view_key = :view_key
""",
                {"view_key": HOMEWORK_DEFAULT_VIEW_KEY},
            )
            .mappings()
            .fetchone()
        )
        assert row == {"view_key": HOMEWORK_DEFAULT_VIEW_KEY}

        payload = load_homework_trade_feed(conn, view_key=HOMEWORK_DEFAULT_VIEW_KEY)
        assert payload["caption"] == "窗口：最近 3 天：2026-04-02 ~ 2026-04-04"
        assert payload["used_window_days"] == 3
        assert payload["row_count"] == 1
        assert payload["rows"] == [
            {
                "topic": "stock:600519.SH",
                "summary": "小仓试错",
                "recent_action": "trade.buy · 中等偏强 · 强度 2",
                "tree_post_uid": "weibo:1",
            }
        ]
        assert str(payload["updated_at"]).strip() != ""
    finally:
        conn.close()


def test_ensure_homework_trade_feed_schema_adds_content_hash_column() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        conn.execute(
            """
CREATE TABLE homework_trade_feed (
    view_key TEXT PRIMARY KEY,
    header_json TEXT NOT NULL DEFAULT '{}',
    items_json TEXT NOT NULL DEFAULT '[]',
    counters_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL
)
"""
        )
        ensure_homework_trade_feed_schema(conn)
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(homework_trade_feed)")
            .mappings()
            .all()
        }
        assert "content_hash" in columns
    finally:
        conn.close()


def test_homework_trade_feed_skips_write_when_content_unchanged(monkeypatch) -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_homework_trade_feed_schema(conn)
        timestamps = iter(
            [
                "2026-04-04 12:00:00",
                "2026-04-04 12:00:01",
                "2026-04-04 12:00:02",
            ]
        )
        monkeypatch.setattr(
            "alphavault.homework_trade_feed._now_str",
            lambda: next(timestamps),
        )
        rows = [{"topic": "stock:600519.SH", "summary": "小仓试错"}]
        save_homework_trade_feed(
            conn,
            view_key=HOMEWORK_DEFAULT_VIEW_KEY,
            caption="窗口：最近 3 天",
            used_window_days=3,
            rows=rows,
        )
        first = (
            conn.execute(
                f"""
SELECT updated_at, content_hash
FROM {HOMEWORK_TRADE_FEED_TABLE}
WHERE view_key = :view_key
""",
                {"view_key": HOMEWORK_DEFAULT_VIEW_KEY},
            )
            .mappings()
            .fetchone()
        )
        assert first is not None

        save_homework_trade_feed(
            conn,
            view_key=HOMEWORK_DEFAULT_VIEW_KEY,
            caption="窗口：最近 3 天",
            used_window_days=3,
            rows=rows,
        )
        second = (
            conn.execute(
                f"""
SELECT updated_at, content_hash
FROM {HOMEWORK_TRADE_FEED_TABLE}
WHERE view_key = :view_key
""",
                {"view_key": HOMEWORK_DEFAULT_VIEW_KEY},
            )
            .mappings()
            .fetchone()
        )
        assert second == first

        save_homework_trade_feed(
            conn,
            view_key=HOMEWORK_DEFAULT_VIEW_KEY,
            caption="窗口：最近 5 天",
            used_window_days=5,
            rows=rows,
        )
        third = (
            conn.execute(
                f"""
SELECT updated_at, content_hash
FROM {HOMEWORK_TRADE_FEED_TABLE}
WHERE view_key = :view_key
""",
                {"view_key": HOMEWORK_DEFAULT_VIEW_KEY},
            )
            .mappings()
            .fetchone()
        )
        assert third is not None
        assert third["updated_at"] == "2026-04-04 12:00:01"
        assert third["content_hash"] != first["content_hash"]
    finally:
        conn.close()
