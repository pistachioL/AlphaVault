from __future__ import annotations

import pandas as pd

from alphavault_reflex.homework_state import HomeworkState


def test_homework_state_keeps_unlinked_stock_alias_as_separate_rows(
    monkeypatch,
) -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:601899.SH",
                "action": "trade.buy",
                "action_strength": 3,
                "summary": "先建一点仓",
                "author": "alice",
                "created_at": pd.Timestamp("2026-03-25 10:00:00"),
                "stock_codes_json": '["601899.SH"]',
                "stock_names_json": '["紫金矿业"]',
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:紫金",
                "action": "trade.hold",
                "action_strength": 1,
                "summary": "继续拿着",
                "author": "bob",
                "created_at": pd.Timestamp("2026-03-26 10:00:00"),
                "stock_codes_json": "[]",
                "stock_names_json": '["紫金"]',
            },
        ]
    )

    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_homework_board_payload_from_env",
        lambda lookback_days: (assertions, pd.DataFrame(), ""),
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_post_urls_from_env",
        lambda post_uids: ({}, ""),
    )

    state = HomeworkState()
    state._refresh()

    assert len(state.rows) == 2
    topics = {row["topic"] for row in state.rows}
    assert topics == {"stock:601899.SH", "stock:紫金"}


def test_homework_state_uses_accepted_stock_alias_relation_for_board_grouping(
    monkeypatch,
) -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:601899.SH",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "先上车",
                "author": "alice",
                "created_at": pd.Timestamp("2026-03-25 10:00:00"),
                "stock_codes_json": '["601899.SH"]',
                "stock_names_json": '["紫金矿业"]',
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:阿紫",
                "action": "trade.watch",
                "action_strength": 1,
                "summary": "继续看",
                "author": "bob",
                "created_at": pd.Timestamp("2026-03-26 10:00:00"),
                "stock_codes_json": "[]",
                "stock_names_json": '["阿紫"]',
            },
        ]
    )
    relations = pd.DataFrame(
        [
            {
                "relation_type": "stock_alias",
                "left_key": "stock:601899.SH",
                "right_key": "stock:阿紫",
                "relation_label": "alias_of",
            }
        ]
    )

    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_homework_board_payload_from_env",
        lambda lookback_days: (assertions, relations, ""),
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_post_urls_from_env",
        lambda post_uids: ({}, ""),
    )

    state = HomeworkState()
    state._refresh()

    assert len(state.rows) == 1
    assert state.rows[0]["topic"] == "stock:601899.SH"
    assert state.rows[0]["stock_route"] == "/research/stocks/601899.SH"


def test_homework_state_refresh_does_not_call_ai_alias_map(monkeypatch) -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:600519.SH",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "小仓试错",
                "author": "alice",
                "created_at": pd.Timestamp("2026-03-25 10:00:00"),
                "stock_codes_json": '["600519.SH"]',
                "stock_names_json": '["贵州茅台"]',
            }
        ]
    )

    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_homework_board_payload_from_env",
        lambda lookback_days: (assertions, pd.DataFrame(), ""),
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_post_urls_from_env",
        lambda post_uids: ({}, ""),
    )

    def _should_not_call(*args, **kwargs):
        raise AssertionError("build_ai_stock_alias_map should not be called on refresh")

    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects.build_ai_stock_alias_map",
        _should_not_call,
    )

    state = HomeworkState()
    state._refresh()

    assert len(state.rows) == 1
    assert state.rows[0]["topic"] == "stock:600519.SH"
