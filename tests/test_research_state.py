from __future__ import annotations

from types import SimpleNamespace

from alphavault_reflex.research_state import ResearchState
from alphavault_reflex.research_state import _resolve_route_slug


def test_research_state_starts_in_loading_state() -> None:
    state = ResearchState()

    assert state.show_loading is True
    assert state.show_signal_empty is False
    assert state.show_related_empty is False
    assert state.show_pending_empty is False


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
    assert state.loaded_once is True
    assert state.show_loading is False
    assert state.show_signal_empty is False
    assert state.primary_signals[0]["summary"] == "继续加仓"
    assert state.related_items[0]["sector_key"] == "white_liquor"


def test_load_stock_page_shows_empty_state_after_loaded(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_view",
        lambda stock_slug: {
            "header_title": "000001.SZ",
            "signals": [],
            "related_sectors": [],
            "pending_candidates": [],
            "load_error": "",
        },
    )

    state = ResearchState()
    state.load_stock_page("000001.SZ")

    assert state.loaded_once is True
    assert state.show_loading is False
    assert state.show_signal_empty is True
    assert state.show_related_empty is True
    assert state.show_pending_empty is True


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


def test_accept_candidate_clears_caches_and_marks_candidate_accepted(
    monkeypatch,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "alphavault_reflex.research_state.apply_candidate_action",
        lambda candidate_row, action: calls.append(
            f"{action}:{candidate_row['candidate_id']}"
        ),
    )
    monkeypatch.setattr(
        "alphavault_reflex.research_state.clear_reflex_source_caches",
        lambda: calls.append("cleared"),
    )
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
    state.entity_type = "stock"
    state.entity_key = "stock:600519.SH"
    state.pending_candidates = [{"candidate_id": "cand-1"}]

    state.accept_candidate("cand-1")

    assert calls == ["accept:cand-1", "cleared"]
    assert state.pending_candidates == []


def test_resolve_route_slug_reads_router_url_without_touching_page() -> None:
    class ExplodingRouter:
        route_id = "/research/stocks/[stock_slug]"
        url = SimpleNamespace(
            path="/research/stocks/600519.SH",
            query_parameters={},
        )

        @property
        def page(self):
            raise AssertionError("router.page should not be used")

    state = SimpleNamespace(router=ExplodingRouter())

    assert (
        _resolve_route_slug(state, explicit_slug=None, route_key="stock_slug")
        == "600519.SH"
    )


def test_resolve_route_slug_prefers_current_router_url_over_stale_state_value() -> None:
    state = SimpleNamespace(
        stock_slug="600519.SH",
        router=SimpleNamespace(
            route_id="/research/stocks/[stock_slug]",
            url=SimpleNamespace(
                path="/research/stocks/000001.SZ",
                query_parameters={},
            ),
        ),
    )

    assert (
        _resolve_route_slug(state, explicit_slug=None, route_key="stock_slug")
        == "000001.SZ"
    )
