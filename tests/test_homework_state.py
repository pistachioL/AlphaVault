from __future__ import annotations

import pandas as pd
import pytest

from alphavault.homework_trade_feed import HOMEWORK_DEFAULT_VIEW_KEY
from alphavault_reflex.homework_state import HomeworkState
from alphavault_reflex.homework_state import TREE_PREVIEW_LINE_COUNT
from alphavault_reflex.services.homework_constants import (
    TRADE_BOARD_DEFAULT_WINDOW_DAYS,
)


@pytest.fixture(autouse=True)
def _stub_homework_trade_feed(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_homework_trade_feed_from_env",
        lambda *, view_key=HOMEWORK_DEFAULT_VIEW_KEY: {},
        raising=False,
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.save_homework_trade_feed_from_env",
        lambda **kwargs: None,
        raising=False,
    )


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


def test_homework_state_refresh_uses_default_feed_when_hit(monkeypatch) -> None:
    calls: list[str] = []
    feed_rows = [
        {
            "topic": "stock:600519.SH",
            "topic_label": "贵州茅台",
            "recent_action": "trade.buy · 中等偏强 · 强度 2",
            "summary": "小仓试错",
            "tree_post_uid": "weibo:1",
            "url": "https://example.com/p/1",
            "stock_slug": "600519.SH",
            "stock_route": "/research/stocks/600519.SH",
            "sector_slug": "",
            "sector_route": "",
        }
    ]

    def _fake_load_trade_feed(*, view_key=HOMEWORK_DEFAULT_VIEW_KEY):  # type: ignore[no-untyped-def]
        calls.append(f"feed:{view_key}")
        return {
            "view_key": view_key,
            "caption": "窗口：最近 3 天：2026-04-02 ~ 2026-04-04",
            "used_window_days": 3,
            "rows": feed_rows,
        }

    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_homework_trade_feed_from_env",
        _fake_load_trade_feed,
        raising=False,
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_homework_board_payload_from_env",
        lambda lookback_days: (_ for _ in ()).throw(
            AssertionError("feed hit 时不该再走 live 查询")
        ),
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.save_homework_trade_feed_from_env",
        lambda **kwargs: calls.append("save"),
        raising=False,
    )

    state = HomeworkState()
    state._refresh()

    assert calls == [f"feed:{HOMEWORK_DEFAULT_VIEW_KEY}"]
    assert state.caption == "窗口：最近 3 天：2026-04-02 ~ 2026-04-04"
    assert state.window_days == 3
    assert state.rows == feed_rows


def test_homework_state_refresh_bootstraps_default_feed_on_miss(monkeypatch) -> None:
    feed_calls: list[str] = []
    save_calls: list[dict[str, object]] = []
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "weibo:1",
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

    def _fake_load_trade_feed(*, view_key=HOMEWORK_DEFAULT_VIEW_KEY):  # type: ignore[no-untyped-def]
        feed_calls.append(f"feed:{view_key}")
        return {}

    def _fake_load_board_payload(lookback_days):  # type: ignore[no-untyped-def]
        feed_calls.append(f"live:{lookback_days}")
        return assertions, pd.DataFrame(), ""

    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_homework_trade_feed_from_env",
        _fake_load_trade_feed,
        raising=False,
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_homework_board_payload_from_env",
        _fake_load_board_payload,
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_post_urls_from_env",
        lambda post_uids: ({}, ""),
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.save_homework_trade_feed_from_env",
        lambda **kwargs: save_calls.append(kwargs),
        raising=False,
    )

    state = HomeworkState()
    state._refresh()

    assert feed_calls == [
        f"feed:{HOMEWORK_DEFAULT_VIEW_KEY}",
        f"live:{TRADE_BOARD_DEFAULT_WINDOW_DAYS}",
    ]
    assert len(state.rows) == 1
    assert len(save_calls) == 1
    assert save_calls[0]["view_key"] == HOMEWORK_DEFAULT_VIEW_KEY
    assert save_calls[0]["caption"] == state.caption
    assert save_calls[0]["used_window_days"] == state.window_days
    assert save_calls[0]["rows"] == state.rows


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


def test_load_data_if_needed_runs_on_first_load(monkeypatch) -> None:
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
    list(state.load_data_if_needed())

    assert calls == ["cleared"]
    assert state.loaded_once is True


def test_load_data_if_needed_skips_when_already_loaded(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "alphavault_reflex.homework_state.clear_reflex_source_caches",
        lambda: calls.append("cleared"),
        raising=False,
    )
    monkeypatch.setattr(
        "alphavault_reflex.homework_state.load_homework_board_payload_from_env",
        lambda lookback_days: calls.append("loaded"),
    )

    state = HomeworkState()
    state.loaded_once = True
    state.caption = "已有数据"
    state.rows = [{"topic": "stock:600519.SH"}]

    result = state.load_data_if_needed()

    assert list(result) == []
    assert calls == []
    assert state.caption == "已有数据"
    assert state.rows == [{"topic": "stock:600519.SH"}]


def test_selected_tree_render_text_collapses_long_tree() -> None:
    lines = [f"line {idx}" for idx in range(TREE_PREVIEW_LINE_COUNT + 3)]
    state = HomeworkState()
    state.selected_tree_text = "\n".join(lines)

    assert state.selected_tree_line_count == TREE_PREVIEW_LINE_COUNT + 3
    assert state.tree_text_collapsible is True
    assert "已折叠 3 行" in state.selected_tree_render_text
    assert f"line {TREE_PREVIEW_LINE_COUNT}" not in state.selected_tree_render_text


def test_expand_tree_text_shows_full_tree_content() -> None:
    lines = [f"line {idx}" for idx in range(TREE_PREVIEW_LINE_COUNT + 1)]
    state = HomeworkState()
    state.selected_tree_text = "\n".join(lines)

    state.expand_tree_text()

    assert state.tree_show_full_text is True
    assert state.selected_tree_render_text == state.selected_tree_text


def test_toggle_tree_wrap_lines_switches_flag() -> None:
    state = HomeworkState()

    state.toggle_tree_wrap_lines()

    assert state.tree_wrap_lines is False


def test_selected_tree_render_lines_preserves_tree_prefix_and_content() -> None:
    state = HomeworkState()
    state.selected_tree_text = "根节点\n│   └── 子节点A 子节点A 子节点A"

    lines = state.selected_tree_render_lines

    assert len(lines) == 2
    assert lines[0]["prefix"] == ""
    assert lines[0]["content"] == "根节点"
    assert lines[1]["prefix"] == "│   └── "
    assert lines[1]["content"] == "子节点A 子节点A 子节点A"


def test_selected_tree_render_lines_splits_id_suffix_from_content() -> None:
    state = HomeworkState()
    state.selected_tree_text = (
        "根节点 [原帖 ID: xueqiu:comment:401613598]\n"
        "│   └── 子节点A [转发 ID: 400768409]"
    )

    lines = state.selected_tree_render_lines

    assert len(lines) == 2
    assert lines[0]["prefix"] == ""
    assert lines[0]["content"] == "根节点"
    assert lines[0]["id_suffix"] == "[原帖 ID: xueqiu:comment:401613598]"
    assert lines[1]["prefix"] == "│   └── "
    assert lines[1]["content"] == "子节点A"
    assert lines[1]["id_suffix"] == "[转发 ID: 400768409]"


def test_selected_tree_render_lines_keeps_multiline_child_as_continuation() -> None:
    state = HomeworkState()
    state.selected_tree_text = (
        "根节点\n│   └── 子节点第一行\n子节点第二行（续）\n│   └── 子节点B"
    )

    lines = state.selected_tree_render_lines

    assert len(lines) == 4
    assert lines[1]["prefix"] == "│   └── "
    assert lines[2]["prefix"] == " " * len("│   └── ")
    assert lines[2]["content"] == "子节点第二行（续）"
    assert "av-tree-line-continuation" in lines[2]["row_class"]
