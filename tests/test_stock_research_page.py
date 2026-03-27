from __future__ import annotations

import json

import alphavault_reflex.alphavault_reflex as app_module
from alphavault_reflex.pages.stock_research import stock_research_page
from alphavault_reflex.pages.stock_research import _signal_card


def _rendered_text(child: dict) -> str:
    return json.loads(child["children"][0]["contents"])


def _collect_cond_states(node: object) -> list[str]:
    if not isinstance(node, dict):
        return []
    found: list[str] = []
    cond_state = node.get("cond_state")
    if isinstance(cond_state, str):
        found.append(cond_state)
    for child in node.get("children", []):
        found.extend(_collect_cond_states(child))
    true_value = node.get("true_value")
    if isinstance(true_value, dict):
        found.extend(_collect_cond_states(true_value))
    false_value = node.get("false_value")
    if isinstance(false_value, dict):
        found.extend(_collect_cond_states(false_value))
    return found


def test_signal_card_renders_action_author_and_time_in_one_row() -> None:
    component = _signal_card(
        {
            "summary": "继续看",
            "action": "trade.watch",
            "author": "挖地瓜的超级鹿鼎公",
            "created_at_line": "2026-03-25 06:19 · 19小时前",
            "tree_text": "",
            "display_md": "",
            "raw_text": "原文内容",
        }
    )
    rendered = component.render()
    meta_row = rendered["children"][1]
    time_fragment = meta_row["children"][2]
    time_child = time_fragment["children"][0]["true_value"]["children"][0]

    assert meta_row["name"] == '"div"'
    assert [_rendered_text(child) for child in meta_row["children"][:2]] == [
        "trade.watch",
        "挖地瓜的超级鹿鼎公",
    ]
    assert _rendered_text(time_child) == "2026-03-25 06:19 · 19小时前"


def test_stock_research_page_uses_root_hydration_to_hide_stale_content() -> None:
    rendered = stock_research_page().render()
    cond_states = _collect_cond_states(rendered)

    assert any("is_hydrated_rx_state_" in value for value in cond_states)


def test_stock_research_page_heading_uses_route_slug_while_loading() -> None:
    rendered = stock_research_page().render()
    heading_contents = rendered["children"][0]["children"][0]["children"][0]["contents"]

    assert "stock_slug_rx_state_" in heading_contents


def test_stock_research_page_heading_title_not_blocked_by_loading_state() -> None:
    rendered = stock_research_page().render()
    heading_contents = rendered["children"][0]["children"][0]["children"][0]["contents"]

    assert "show_loading_rx_state_" not in heading_contents


def test_stock_research_page_heading_does_not_fallback_to_previous_page_title() -> None:
    rendered = stock_research_page().render()
    heading_contents = rendered["children"][0]["children"][0]["children"][0]["contents"]

    assert "page_title_rx_state_" not in heading_contents


def test_stock_route_browser_title_uses_dynamic_stock_slug() -> None:
    page = app_module.app._unevaluated_pages["research/stocks/[stock_slug]"]

    assert "stock_slug_rx_state_" in str(page.title)


def test_stock_research_page_no_longer_uses_full_refreshing_state() -> None:
    rendered = stock_research_page().render()
    cond_states = _collect_cond_states(rendered)

    assert not any("full_refreshing_rx_state_" in value for value in cond_states)
