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
    assert view.signals[0]["tree_text"].endswith("[原帖 ID: p1]")
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
        now=pd.Timestamp("2026-03-26 10:23:00"),
    )

    assert view.entity_key == "stock:601899.SH"
    assert [row["summary"] for row in view.signals] == [
        "先建一点仓",
        "继续拿着",
        "先继续观察",
    ]
    assert [row["sector_key"] for row in view.related_sectors] == ["gold", "copper"]
    assert view.signals[0]["created_at_line"] == "2026-03-25 10:00 · 1天前"


def test_build_stock_research_view_formats_created_at_line() -> None:
    posts = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "author": "alice",
                "raw_text": "原文内容",
                "display_md": "格式化原文",
                "created_at": "2026-03-25 10:23:00",
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
                "created_at": "2026-03-25 10:23:00",
                "cluster_keys": ["white_liquor"],
            }
        ]
    )

    view = build_stock_research_view(
        posts,
        assertions,
        stock_key="stock:600519.SH",
        now=pd.Timestamp("2026-03-26 10:23:00"),
    )

    assert view.signals[0]["created_at_line"] == "2026-03-25 10:23 · 1天前"
    assert view.signals[0]["display_md"] == "格式化原文"
    assert view.signals[0]["raw_text"] == "原文内容"


def test_build_stock_research_view_does_not_scan_backfill_posts_on_page_load() -> None:
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

    assert view.backfill_posts == []


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


def test_build_stock_research_view_keeps_tree_for_xueqiu_weibo_url_post_uid() -> None:
    post_uid = "xueqiu:https://weibo.com/3962719063/QxH0rF27I"
    raw_text = (
        "回复@落晚平沙:看走势，可以看出大家想法//@落晚平沙:请教公公碰到好几次"
        "这个问题但是没有想明白//@挖地瓜的超级鹿鼎公:并没有，还是老样子"
    )
    posts = pd.DataFrame(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": "https://weibo.com/3962719063/QxH0rF27I",
                "author": "挖地瓜的超级鹿鼎公",
                "raw_text": raw_text,
                "display_md": raw_text,
                "created_at": "2026-03-25 10:23:48",
            }
        ]
    )
    assertions = pd.DataFrame(
        [
            {
                "post_uid": post_uid,
                "topic_key": "stock:600011.SH",
                "action": "trade.watch",
                "action_strength": 0,
                "summary": "看走势，可以看出大家想法",
                "created_at": "2026-03-25 10:23:48",
                "cluster_keys": [],
            }
        ]
    )

    view = build_stock_research_view(posts, assertions, stock_key="stock:600011.SH")
    assert (
        "[原帖 ID: https://weibo.com/3962719063/QxH0rF27I]"
        in view.signals[0]["tree_text"]
    )


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
