from __future__ import annotations

from alphavault_reflex.services.stock_related_feed import (
    DEFAULT_RELATED_LIMIT,
    MAX_RELATED_LIMIT,
    RELATED_FILTER_ALL,
    RELATED_FILTER_SIGNAL,
    build_related_feed,
    normalize_related_filter,
    normalize_related_limit,
)


def test_normalize_related_filter_defaults_to_all() -> None:
    assert normalize_related_filter("") == RELATED_FILTER_ALL
    assert normalize_related_filter("wat") == RELATED_FILTER_ALL
    assert normalize_related_filter("ALL") == RELATED_FILTER_ALL
    assert normalize_related_filter("signal") == RELATED_FILTER_SIGNAL


def test_normalize_related_limit_clamps_and_defaults() -> None:
    assert normalize_related_limit("") == DEFAULT_RELATED_LIMIT
    assert normalize_related_limit("0") == DEFAULT_RELATED_LIMIT
    assert normalize_related_limit("-1") == DEFAULT_RELATED_LIMIT
    assert normalize_related_limit("2") == 2
    assert normalize_related_limit(str(MAX_RELATED_LIMIT + 1000)) == MAX_RELATED_LIMIT


def test_build_related_feed_sorts_and_slices_signals_only() -> None:
    signals = [
        {
            "post_uid": "weibo:1",
            "summary": "s1",
            "action": "trade.buy",
            "author": "a",
            "created_at": "2026-04-01 10:00",
            "created_at_line": "2026-04-01 10:00",
            "url": "u1",
            "raw_text": "t",
            "tree_text": "tree1",
        },
        {
            "post_uid": "weibo:2",
            "summary": "s2",
            "action": "trade.sell",
            "author": "b",
            "created_at": "2026-04-01 09:00",
            "created_at_line": "2026-04-01 09:00",
            "url": "u2",
            "raw_text": "t2",
            "tree_text": "tree2",
        },
    ]

    feed = build_related_feed(
        signals=signals,
        related_filter="all",
        limit=2,
        now="2026-04-01 10:30",
    )
    assert feed.total == 2
    assert [row["post_uid"] for row in feed.rows] == ["weibo:1", "weibo:2"]
    assert feed.rows[0]["is_signal"] == "1"
    assert feed.rows[0]["signal_badge"] == "买"
    assert feed.rows[0]["title"] == "s1"
    assert feed.rows[0]["created_at_line"] == "2026-04-01 10:00 · 30分钟前"
    assert feed.rows[0]["tree_lines"][0]["content"] == "tree1"
    assert feed.rows[1]["is_signal"] == "1"
    assert feed.rows[1]["signal_badge"] == "卖"
    assert feed.rows[1]["action"] == "trade.sell"
    assert feed.rows[1]["raw_text"] == "t2"
    assert feed.rows[1]["title"] == "s2"
    assert feed.rows[1]["created_at_line"] == "2026-04-01 09:00 · 1小时前"
    assert feed.rows[1]["tree_lines"][0]["content"] == "tree2"
    assert "display_md" not in feed.rows[0]


def test_build_related_feed_signal_filter_keeps_signals() -> None:
    feed = build_related_feed(
        signals=[{"post_uid": "p1", "summary": "s", "action": "trade.buy"}],
        related_filter="signal",
        limit=20,
    )
    assert feed.total == 1
    assert [row["post_uid"] for row in feed.rows] == ["p1"]
