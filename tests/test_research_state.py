from __future__ import annotations

from alphavault_reflex.research_state import ResearchState


def test_load_stock_page_sets_primary_signal(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_view",
        lambda stock_slug: {
            "header_title": "600519.SH",
            "signals": [{"summary": "继续加仓"}],
            "related_sectors": [{"sector_key": "white_liquor"}],
            "pending_candidates": [],
        },
    )

    state = ResearchState()
    state.load_stock_page("600519.SH")
    assert state.page_title == "600519.SH"
    assert state.primary_signals[0]["summary"] == "继续加仓"
    assert state.related_items[0]["sector_key"] == "white_liquor"


def test_load_sector_page_sets_primary_signal(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_sector_page_view",
        lambda sector_slug: {
            "header_title": "white_liquor",
            "signals": [{"summary": "板块继续走强"}],
            "related_stocks": [{"stock_key": "stock:600519.SH"}],
            "pending_candidates": [{"candidate_key": "consumer"}],
        },
    )

    state = ResearchState()
    state.load_sector_page("white_liquor")
    assert state.page_title == "white_liquor"
    assert state.primary_signals[0]["summary"] == "板块继续走强"
    assert state.related_items[0]["stock_key"] == "stock:600519.SH"
    assert state.pending_candidates[0]["candidate_key"] == "consumer"
