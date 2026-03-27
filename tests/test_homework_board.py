from __future__ import annotations

import pandas as pd

from alphavault_reflex.services.homework_board import build_board


def test_build_board_caption_only_shows_window_line() -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:600519.SH",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "小仓",
                "author": "alice",
                "created_at": pd.Timestamp("2026-03-25 10:00:00"),
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:600519.SH",
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
        group_col="topic_key",
        group_label="主题",
        window_days=3,
        sort_mode="最新",
        consensus_filter="全部",
    )

    assert "窗口：" in result.caption
    assert "总时间：" not in result.caption
