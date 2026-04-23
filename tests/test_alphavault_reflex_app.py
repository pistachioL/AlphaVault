from __future__ import annotations

from alphavault_reflex.alphavault_reflex import app
from alphavault_reflex.pages.organizer import organizer_page


def test_homework_page_on_load_uses_if_needed_handler() -> None:
    handler = app._load_events["homework"][0]

    assert handler.fn.__name__ == "load_data_if_needed"


def test_stock_page_on_load_uses_if_needed_handler() -> None:
    handler = app._load_events["research/stocks/[stock_slug]"][0]

    assert handler.fn.__name__ == "load_stock_page_if_needed"


def test_sector_page_on_load_uses_if_needed_handler() -> None:
    handler = app._load_events["research/sectors/[sector_slug]"][0]

    assert handler.fn.__name__ == "load_sector_page_if_needed"


def test_organizer_page_builds_without_type_errors() -> None:
    organizer_page()
