from __future__ import annotations

import json

from alphavault_reflex.homework_state import HomeworkState
from alphavault_reflex.pages.homework import _topic_cell
from alphavault_reflex.pages.homework import _row_tr
from alphavault_reflex.pages.homework import _tree_dialog

STOCK_ROUTE = "/research/stocks/600519.SH"


def _sample_row() -> dict[str, str]:
    return {
        "topic": "stock:600519.SH",
        "topic_label": "贵州茅台",
        "stock_slug": "600519.SH",
        "stock_route": STOCK_ROUTE,
        "sector_slug": "",
        "sector_route": "",
        "tree_post_uid": "",
        "summary": "",
        "url": "",
        "recent_action": "",
        "recent_age": "",
        "recent_author": "",
        "net_strength": "",
        "buy_strength": "",
        "sell_strength": "",
        "mentions": "",
        "author_count": "",
    }


def _tree_dialog_close_button():
    dialog = _tree_dialog()
    content = dialog.children[0]
    footer = content.children[3]
    return _find_component_with_event(footer, "on_click")


def _find_component_with_event(component, event_name: str):
    triggers = getattr(component, "event_triggers", {})
    if event_name in triggers:
        return component
    for child in getattr(component, "children", []):
        matched = _find_component_with_event(child, event_name)
        if matched is not None:
            return matched
    return None


def test_topic_link_row_does_not_bind_row_click() -> None:
    component = _row_tr(_sample_row())

    assert "on_click" not in component.event_triggers


def test_stock_topic_link_does_not_preload_before_navigation() -> None:
    topic_cell = _topic_cell(_sample_row())
    link = _find_component_with_event(topic_cell, "on_click")

    assert link is None


def test_tree_dialog_close_button_updates_state() -> None:
    button = _tree_dialog_close_button()

    assert button is not None
    assert button.event_triggers["on_click"].events[0].handler.fn.__name__ == (
        "close_tree_dialog"
    )


def test_close_tree_dialog_stops_loading_and_closes() -> None:
    state = HomeworkState()
    state.tree_dialog_open = True
    state.tree_loading = True

    state.close_tree_dialog()

    assert state.tree_dialog_open is False
    assert state.tree_loading is False


def test_tree_dialog_includes_debug_text_binding() -> None:
    rendered = _tree_dialog().render()
    payload = json.dumps(rendered, ensure_ascii=False)

    assert "selected_tree_debug_text_rx_state_" in payload
