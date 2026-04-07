from __future__ import annotations

import pandas as pd

from alphavault_reflex.services.homework_board import build_board


def test_build_board_caption_only_shows_window_line() -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "entity_key": "stock:600519.SH",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "小仓",
                "author": "alice",
                "created_at": pd.Timestamp("2026-03-25 10:00:00"),
            },
            {
                "post_uid": "p2",
                "entity_key": "stock:600519.SH",
                "action": "trade.hold",
                "action_strength": 1,
                "summary": "继续",
                "author": "bob",
                "created_at": pd.Timestamp("2026-03-26 10:00:00"),
            },
        ]
    )

    result = build_board(
        assertions,
        pd.DataFrame(),
        group_col="entity_key",
        group_label="主题",
        window_days=3,
        trade_filter="全部",
    )

    assert "窗口：" in result.caption
    assert "总时间：" not in result.caption


def test_build_board_keeps_xueqiu_tree_post_uid_without_strip() -> None:
    raw_uid = "xueqiu:status:381213336\t"
    assertions = pd.DataFrame(
        [
            {
                "post_uid": raw_uid,
                "entity_key": "stock:600519.SH",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "小仓",
                "author": "alice",
                "created_at": pd.Timestamp("2026-03-25 10:00:00"),
            }
        ]
    )

    result = build_board(
        assertions,
        pd.DataFrame(),
        group_col="entity_key",
        group_label="主题",
        window_days=3,
        trade_filter="全部",
    )

    assert result.rows[0]["tree_post_uid"] == raw_uid


def test_build_board_trade_filter_only_keeps_latest_buy() -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "a1",
                "entity_key": "stock:600519.SH",
                "action": "trade.sell",
                "action_strength": 2,
                "summary": "先卖一点",
                "author": "alice",
                "created_at": pd.Timestamp("2026-03-25 10:00:00"),
            },
            {
                "post_uid": "a2",
                "entity_key": "stock:600519.SH",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "又买回来",
                "author": "alice",
                "created_at": pd.Timestamp("2026-03-26 10:00:00"),
            },
            {
                "post_uid": "b1",
                "entity_key": "stock:000001.SZ",
                "action": "trade.buy",
                "action_strength": 1,
                "summary": "先试试",
                "author": "bob",
                "created_at": pd.Timestamp("2026-03-25 09:00:00"),
            },
            {
                "post_uid": "b2",
                "entity_key": "stock:000001.SZ",
                "action": "trade.sell",
                "action_strength": 3,
                "summary": "卖掉",
                "author": "bob",
                "created_at": pd.Timestamp("2026-03-27 10:00:00"),
            },
        ]
    )

    result = build_board(
        assertions,
        pd.DataFrame(),
        group_col="entity_key",
        group_label="主题",
        window_days=10,
        trade_filter="买",
    )

    assert [row["topic"] for row in result.rows] == ["stock:600519.SH"]


def test_build_board_trade_filter_hold_includes_watch() -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "a1",
                "entity_key": "stock:600519.SH",
                "action": "trade.watch",
                "action_strength": 2,
                "summary": "先看看",
                "author": "alice",
                "created_at": pd.Timestamp("2026-03-27 10:00:00"),
            },
            {
                "post_uid": "b1",
                "entity_key": "stock:000001.SZ",
                "action": "trade.sell",
                "action_strength": 3,
                "summary": "卖掉",
                "author": "bob",
                "created_at": pd.Timestamp("2026-03-27 10:00:00"),
            },
        ]
    )

    result = build_board(
        assertions,
        pd.DataFrame(),
        group_col="entity_key",
        group_label="主题",
        window_days=10,
        trade_filter="只看",
    )

    assert [row["topic"] for row in result.rows] == ["stock:600519.SH"]
