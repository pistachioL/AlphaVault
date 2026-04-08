from __future__ import annotations

from alphavault.constants import SCHEMA_STANDARD
from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.postgres_db import PostgresConnection
from alphavault.homework_trade_feed import (
    HOMEWORK_DEFAULT_VIEW_KEY,
    HOMEWORK_TRADE_FEED_TABLE,
    load_homework_trade_feed,
    save_homework_trade_feed,
)


def _homework_conn(pg_conn) -> PostgresConnection:
    apply_cloud_schema(pg_conn, target="standard", schema_name=SCHEMA_STANDARD)
    return PostgresConnection(pg_conn)


def test_homework_trade_feed_schema_and_roundtrip(pg_conn) -> None:
    conn = _homework_conn(pg_conn)
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
        str(row[0])
        for row in conn.execute(
            """
SELECT tablename
FROM pg_tables
WHERE schemaname = :schema_name
""",
            {"schema_name": SCHEMA_STANDARD},
        ).fetchall()
    }
    assert HOMEWORK_TRADE_FEED_TABLE.removeprefix(f"{SCHEMA_STANDARD}.") in table_names

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


def test_homework_trade_feed_skips_write_when_content_unchanged(
    monkeypatch,
    pg_conn,
) -> None:
    conn = _homework_conn(pg_conn)
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
