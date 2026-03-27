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
        lambda stock_slug, **_kwargs: {
            "entity_key": "stock:600519.SH",
            "header_title": "600519.SH",
            "signals": [{"summary": "继续加仓"}],
            "related_sectors": [{"sector_key": "white_liquor"}],
            "pending_candidates": [],
            "backfill_posts": [{"post_uid": "p2", "preview": "补一篇"}],
        },
    )

    state = ResearchState()
    state.load_stock_page("600519.SH")
    assert state.page_title == "600519.SH"
    assert state.entity_key == "stock:600519.SH"
    assert state.loaded_once is True
    assert state.show_loading is False
    assert state.show_signal_empty is False
    assert state.primary_signals[0]["summary"] == "继续加仓"
    assert state.related_items[0]["sector_key"] == "white_liquor"
    assert state.backfill_posts[0]["post_uid"] == "p2"


def test_load_stock_page_shows_empty_state_after_loaded(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_view",
        lambda stock_slug, **_kwargs: {
            "entity_key": "stock:000001.SZ",
            "header_title": "000001.SZ",
            "signals": [],
            "related_sectors": [],
            "pending_candidates": [],
            "backfill_posts": [],
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
    assert state.show_backfill_empty is True


def test_load_stock_page_uses_canonical_entity_key_from_view(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_view",
        lambda stock_slug, **_kwargs: {
            "entity_key": "stock:601899.SH",
            "header_title": "紫金矿业 (601899.SH)",
            "signals": [{"summary": "继续拿着"}],
            "related_sectors": [],
            "pending_candidates": [],
            "backfill_posts": [],
            "load_error": "",
        },
    )

    state = ResearchState()
    state.load_stock_page("紫金")

    assert state.page_title == "紫金矿业 (601899.SH)"
    assert state.entity_key == "stock:601899.SH"


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
        lambda stock_slug, **_kwargs: {
            "entity_key": "stock:600519.SH",
            "header_title": "600519.SH",
            "signals": [{"summary": "继续加仓"}],
            "related_sectors": [{"sector_key": "white_liquor"}],
            "pending_candidates": [],
            "backfill_posts": [],
        },
    )

    state = ResearchState()
    state.entity_type = "stock"
    state.entity_key = "stock:600519.SH"
    state.pending_candidates = [{"candidate_id": "cand-1"}]

    state.accept_candidate("cand-1")

    assert calls == ["accept:cand-1", "cleared"]
    assert state.pending_candidates == []


def test_queue_backfill_post_marks_notice_and_clears_caches(monkeypatch) -> None:
    calls: list[str] = []

    def _fake_queue_post_for_ai_backfill(post_uid: str) -> None:
        calls.append(str(post_uid or "").strip())

    monkeypatch.setattr(
        "alphavault_reflex.research_state.queue_post_for_ai_backfill",
        _fake_queue_post_for_ai_backfill,
    )
    monkeypatch.setattr(
        "alphavault_reflex.research_state.clear_reflex_source_caches",
        lambda: calls.append("cleared"),
    )

    state = ResearchState()
    state.entity_key = "stock:601899.SH"
    state.page_title = "紫金矿业 (601899.SH)"
    state.backfill_posts = [{"post_uid": "weibo:123"}]

    state.queue_backfill_post("weibo:123")

    assert calls == ["weibo:123", "cleared"]
    assert "已排队" in state.backfill_notice


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
