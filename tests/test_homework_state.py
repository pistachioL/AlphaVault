from __future__ import annotations

import pandas as pd

from alphavault_reflex.homework_state import HomeworkState


def test_homework_state_groups_same_stock_object_into_one_row(monkeypatch) -> None:
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
        "alphavault_reflex.homework_state.load_trade_assertions_from_env",
        lambda: (assertions, ""),
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_stock_alias_relations_from_env",
        lambda: (pd.DataFrame(), ""),
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.build_ai_stock_alias_map",
        lambda assertions, stock_relations: {"stock:紫金": "stock:601899.SH"},
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_post_urls_from_env",
        lambda post_uids: ({}, ""),
    )

    state = HomeworkState()
    state._refresh()

    assert len(state.rows) == 1
    assert state.rows[0]["topic"] == "stock:601899.SH"
    assert state.rows[0]["topic_label"] == "紫金矿业 (601899.SH)"
    assert state.rows[0]["stock_route"] == "/research/stocks/601899.SH"
    assert state.rows[0]["mentions"] == "2"
    assert state.rows[0]["summary"] == "继续拿着"


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
        "alphavault_reflex.homework_state.load_trade_assertions_from_env",
        lambda: (assertions, ""),
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_stock_alias_relations_from_env",
        lambda: (relations, ""),
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.build_ai_stock_alias_map",
        lambda assertions, stock_relations: {"stock:阿紫": "stock:601899.SH"},
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
