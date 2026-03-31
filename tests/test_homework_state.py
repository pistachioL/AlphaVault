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
        "alphavault.infra.ai.stock_alias.build_ai_stock_alias_map",
        _should_not_call,
    )

    state = HomeworkState()
    state._refresh()

    assert len(state.rows) == 1
    assert state.rows[0]["topic"] == "stock:600519.SH"


def test_open_tree_dialog_keeps_xueqiu_post_uid_for_loader(monkeypatch) -> None:
    requested_uid = "xueqiu:status:381213336\t"
    seen_uids: list[str] = []

    def _fake_load_single_post_for_tree_from_env(post_uid: str):
        seen_uids.append(post_uid)
        return (
            pd.DataFrame(
                [
                    {
                        "post_uid": requested_uid,
                        "platform_post_id": "status:381213336",
                        "author": "雪球作者",
                        "raw_text": "A：叶子",
                        "display_md": "A：叶子",
                        "created_at": "2026-03-25 10:23:48",
                    }
                ]
            ),
            "",
        )

    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_single_post_for_tree_from_env",
        _fake_load_single_post_for_tree_from_env,
    )

    state = HomeworkState()
    list(state.open_tree_dialog(requested_uid))

    assert seen_uids == [requested_uid]
    assert state.selected_tree_text != ""
    assert state.selected_tree_message == ""


def test_open_tree_dialog_shows_debug_info_on_missing_tree(monkeypatch) -> None:
    requested_uid = "xueqiu:status:381213336\t"

    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_single_post_for_tree_from_env",
        lambda post_uid: (pd.DataFrame(), ""),
    )

    state = HomeworkState()
    list(state.open_tree_dialog(requested_uid))

    assert state.selected_tree_message == "没有对话流。"
    assert "请求UID" in state.selected_tree_debug_text
    assert requested_uid in state.selected_tree_debug_text
    assert "阶段码: posts_empty" in state.selected_tree_debug_text


def test_load_data_clears_reflex_source_caches(monkeypatch) -> None:
    calls: list[str] = []
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "weibo:1",
                "topic_key": "stock:600519.SH",
                "action": "trade.buy",
                "action_strength": 1,
                "summary": "小仓",
                "author": "alice",
                "created_at": pd.Timestamp("2026-03-25 10:00:00"),
                "stock_codes_json": '["600519.SH"]',
                "stock_names_json": '["贵州茅台"]',
            }
        ]
    )

    monkeypatch.setattr(
        "alphavault_reflex.homework_state.clear_reflex_source_caches",
        lambda: calls.append("cleared"),
        raising=False,
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
    list(state.load_data())

    assert calls == ["cleared"]
