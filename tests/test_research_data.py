from __future__ import annotations

import pandas as pd

from alphavault_reflex.services.research_data import (
    build_search_index,
    build_sector_research_view,
    build_stock_research_view,
)


def test_build_stock_research_view_groups_recent_signals_and_related_sectors() -> None:
    posts = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "author": "alice",
                "raw_text": "看多茅台",
                "display_md": "看多茅台",
            }
        ]
    )
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:600519.SH",
                "action": "trade.buy",
                "action_strength": 3,
                "summary": "继续加仓",
                "created_at": "2026-03-25 10:00:00",
                "cluster_keys": ["white_liquor"],
            }
        ]
    )

    view = build_stock_research_view(posts, assertions, stock_key="stock:600519.SH")
    assert view.entity_key == "stock:600519.SH"
    assert view.header_title == "600519.SH"
    assert view.signals[0]["summary"] == "继续加仓"
    assert view.related_sectors[0]["sector_key"] == "white_liquor"


def test_build_stock_research_view_aggregates_one_stock_object() -> None:
    posts = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "author": "alice",
                "raw_text": "先买一点紫金矿业",
                "display_md": "先买一点紫金矿业",
            },
            {
                "post_uid": "p2",
                "author": "bob",
                "raw_text": "紫金先拿着",
                "display_md": "紫金先拿着",
            },
            {
                "post_uid": "p3",
                "author": "cindy",
                "raw_text": "继续观察601899.SH",
                "display_md": "继续观察601899.SH",
            },
        ]
    )
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:601899.SH",
                "action": "trade.buy",
                "action_strength": 3,
                "summary": "先建一点仓",
                "created_at": "2026-03-25 10:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": '["601899.SH"]',
                "stock_names_json": '["紫金矿业"]',
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:紫金",
                "action": "trade.hold",
                "action_strength": 1,
                "summary": "继续拿着",
                "created_at": "2026-03-24 10:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["紫金"]',
            },
            {
                "post_uid": "p3",
                "topic_key": "stock:紫金矿业",
                "action": "trade.watch",
                "action_strength": 1,
                "summary": "先继续观察",
                "created_at": "2026-03-23 10:00:00",
                "cluster_keys": ["copper"],
                "stock_codes_json": "[]",
                "stock_names_json": '["紫金矿业"]',
            },
        ]
    )

    view = build_stock_research_view(
        posts,
        assertions,
        stock_key="stock:紫金",
        ai_alias_map={"stock:紫金": "stock:601899.SH"},
    )

    assert view.entity_key == "stock:601899.SH"
    assert [row["summary"] for row in view.signals] == [
        "先建一点仓",
        "继续拿着",
        "先继续观察",
    ]
    assert [row["sector_key"] for row in view.related_sectors] == ["gold", "copper"]


def test_build_stock_research_view_lists_unstructured_backfill_posts() -> None:
    posts = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "author": "alice",
                "created_at": "2026-03-25 10:00:00",
                "raw_text": "先买一点紫金矿业",
                "display_md": "先买一点紫金矿业",
                "url": "https://example.com/p1",
            },
            {
                "post_uid": "p2",
                "author": "bob",
                "created_at": "2026-03-26 11:00:00",
                "raw_text": "我觉得紫金矿业这里先别急，等一等",
                "display_md": "我觉得紫金矿业这里先别急，等一等",
                "url": "https://example.com/p2",
            },
        ]
    )
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:601899.SH",
                "action": "trade.buy",
                "action_strength": 3,
                "summary": "先建一点仓",
                "created_at": "2026-03-25 10:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": '["601899.SH"]',
                "stock_names_json": '["紫金矿业"]',
            }
        ]
    )

    view = build_stock_research_view(posts, assertions, stock_key="stock:601899.SH")

    assert [row["post_uid"] for row in view.backfill_posts] == ["p2"]
    assert "紫金矿业" in view.backfill_posts[0]["matched_terms"]
    assert "先别急" in view.backfill_posts[0]["preview"]


def test_build_sector_research_view_groups_recent_signals_and_related_stocks() -> None:
    posts = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "author": "alice",
                "raw_text": "白酒继续走强",
                "display_md": "白酒继续走强",
            }
        ]
    )
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:600519.SH",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "板块继续走强",
                "created_at": "2026-03-25 10:00:00",
                "cluster_keys": ["white_liquor"],
            }
        ]
    )

    view = build_sector_research_view(posts, assertions, sector_key="white_liquor")
    assert view.header_title == "white_liquor"
    assert view.signals[0]["summary"] == "板块继续走强"
    assert view.related_stocks[0]["stock_key"] == "stock:600519.SH"


def test_build_search_index_returns_stock_and_sector_hits() -> None:
    posts = pd.DataFrame([])
    assertions = pd.DataFrame(
        [
            {
                "topic_key": "stock:600519.SH",
                "cluster_keys": ["white_liquor"],
            }
        ]
    )

    rows = build_search_index(posts, assertions)
    assert rows[0]["href"] == "/research/stocks/600519.SH"
    assert rows[1]["href"] == "/research/sectors/white_liquor"


def test_build_search_index_deduplicates_stock_objects_and_keeps_alias_search_text() -> (
    None
):
    posts = pd.DataFrame([])
    assertions = pd.DataFrame(
        [
            {
                "topic_key": "stock:601899.SH",
                "cluster_keys": ["gold"],
                "stock_codes_json": '["601899.SH"]',
                "stock_names_json": '["紫金矿业"]',
            },
            {
                "topic_key": "stock:紫金",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["紫金"]',
            },
            {
                "topic_key": "stock:紫金矿业",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["紫金矿业"]',
            },
        ]
    )

    rows = build_search_index(
        posts,
        assertions,
        ai_alias_map={"stock:紫金": "stock:601899.SH"},
    )
    stock_rows = [row for row in rows if row["entity_type"] == "stock"]

    assert len(stock_rows) == 1
    assert stock_rows[0]["entity_key"] == "stock:601899.SH"
    assert stock_rows[0]["href"] == "/research/stocks/601899.SH"
    assert "紫金" in stock_rows[0]["search_text"]
