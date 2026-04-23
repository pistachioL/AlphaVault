from __future__ import annotations

from alphavault_reflex.alphavault_reflex import app
from alphavault_reflex.services.original_link import ORIGINAL_LINK_SCRIPT_PATH


def test_homework_page_on_load_uses_if_needed_handler() -> None:
    handler = app._load_events["homework"][0]

    assert handler.fn.__name__ == "load_data_if_needed"


def test_stock_page_on_load_uses_if_needed_handler() -> None:
    handler = app._load_events["research/stocks/[stock_slug]"][0]

    assert handler.fn.__name__ == "load_stock_page_if_needed"


def test_sector_page_on_load_uses_if_needed_handler() -> None:
    handler = app._load_events["research/sectors/[sector_slug]"][0]

    assert handler.fn.__name__ == "load_sector_page_if_needed"


def test_app_registers_original_link_script() -> None:
    head_children = []
    for component in app.head_components:
        rendered = component.render()
        head_children.extend(rendered["children"])

    assert {
        "name": '"script"',
        "props": [f'src:"{ORIGINAL_LINK_SCRIPT_PATH}"'],
        "children": [],
    } in head_children
