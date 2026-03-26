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
    assert view.header_title == "600519.SH"
    assert view.signals[0]["summary"] == "继续加仓"
    assert view.signals[0]["tree_text"].endswith("[原帖 ID: p1]")
    assert view.related_sectors[0]["sector_key"] == "white_liquor"


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
