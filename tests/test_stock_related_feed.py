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


def test_build_related_feed_merges_and_sorts_and_slices() -> None:
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
            "display_md": "",
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
            "display_md": "",
            "tree_text": "tree2",
        },
    ]
    backfill = [
        {
            "post_uid": "weibo:3",
            "author": "c",
            "created_at": "2026-04-01 09:30:00",
            "url": "u3",
            "matched_terms": "m",
            "preview": "p",
            "tree_text": "tree3",
        }
    ]

    feed = build_related_feed(
        signals=signals,
        backfill_posts=backfill,
        related_filter="all",
        limit=2,
    )
    assert feed.total == 3
    assert [row["post_uid"] for row in feed.rows] == ["weibo:1", "weibo:3"]
    assert feed.rows[0]["is_signal"] == "1"
    assert feed.rows[0]["signal_badge"] == "买"
    assert feed.rows[0]["title"] == "s1"
    assert feed.rows[1]["is_signal"] == ""
    assert feed.rows[1]["signal_badge"] == ""
    assert feed.rows[1]["action"] == ""
    assert feed.rows[1]["raw_text"] == ""
    assert feed.rows[1]["display_md"] == ""
    assert feed.rows[1]["title"] == "m"
    assert feed.rows[1]["created_at_line"] == "2026-04-01 09:30"


def test_build_related_feed_signal_filter_excludes_backfill() -> None:
    feed = build_related_feed(
        signals=[{"post_uid": "p1", "summary": "s", "action": "trade.buy"}],
        backfill_posts=[{"post_uid": "p2", "matched_terms": "m"}],
        related_filter="signal",
        limit=20,
    )
    assert feed.total == 1
    assert [row["post_uid"] for row in feed.rows] == ["p1"]


def test_build_related_feed_dedupes_by_post_uid_prefers_signals() -> None:
    feed = build_related_feed(
        signals=[{"post_uid": "p1", "summary": "s", "action": "trade.buy"}],
        backfill_posts=[
            {"post_uid": "p1", "matched_terms": "m", "created_at": "2026-01-01"}
        ],
        related_filter="all",
        limit=20,
    )
    assert feed.total == 1
    assert feed.rows[0]["post_uid"] == "p1"
    assert feed.rows[0]["is_signal"] == "1"
