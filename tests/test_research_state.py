from __future__ import annotations

from types import SimpleNamespace

from alphavault_reflex.research_state import ResearchState
from alphavault_reflex.research_state import _resolve_route_slug


def test_research_state_starts_in_loading_state() -> None:
    state = ResearchState()

    assert state.show_loading is True
    assert state.show_signal_empty is False
    assert state.show_related_empty is False
    assert not hasattr(state, "show_pending_empty")
    assert state.stock_sidebar_open is False


def test_load_stock_page_sets_primary_signal(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_cached_view",
        lambda stock_slug, **_kwargs: {
            "entity_key": "stock:600519.SH",
            "page_title": "600519.SH",
            "signals": [{"summary": "继续加仓"}],
            "signal_total": 1,
            "signal_page": 1,
            "signal_page_size": 5,
            "load_error": "",
        },
    )

    state = ResearchState()
    events = state.load_stock_page("600519.SH")
    assert state.page_title == "600519.SH"
    assert state.entity_key == "stock:600519.SH"
    assert state.loaded_once is True
    assert state.show_loading is False
    assert state.show_signal_empty is False
    assert state.extras_loading is False
    assert state.signals_ready is True
    assert state.extras_ready is False
    assert state.primary_signals[0]["summary"] == "继续加仓"
    assert state.related_items == []
    assert not hasattr(state, "backfill_posts")
    assert not hasattr(state, "pending_candidates")
    assert events is None


def test_load_stock_page_shows_empty_state_after_loaded(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_cached_view",
        lambda stock_slug, **_kwargs: {
            "entity_key": "stock:000001.SZ",
            "page_title": "000001.SZ",
            "signals": [],
            "related_sectors": [],
            "signal_total": 0,
            "signal_page": 1,
            "signal_page_size": 5,
            "load_error": "",
        },
    )

    state = ResearchState()
    state.load_stock_page("000001.SZ")

    assert state.loaded_once is True
    assert state.show_loading is False
    assert state.show_signal_empty is True
    assert state.show_related_empty is False
    assert not hasattr(state, "show_pending_empty")
    assert not hasattr(state, "show_backfill_empty")


def test_load_stock_page_sets_signals_not_ready_when_cache_preparing(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_cached_view",
        lambda stock_slug, **_kwargs: {
            "entity_key": "stock:000001.SZ",
            "page_title": "000001.SZ",
            "signals": [],
            "related_sectors": [],
            "signal_total": 0,
            "signal_page": 1,
            "signal_page_size": 5,
            "load_error": "",
            "load_warning": "缓存准备中，请稍后刷新。",
        },
    )

    state = ResearchState()
    state.load_stock_page("000001.SZ")

    assert state.signals_ready is False


def test_load_stock_page_maps_worker_progress_fields(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_cached_view",
        lambda stock_slug, **_kwargs: {
            "entity_key": "stock:000001.SZ",
            "page_title": "000001.SZ",
            "signals": [],
            "related_sectors": [],
            "signal_total": 0,
            "signal_page": 1,
            "signal_page_size": 5,
            "load_error": "",
            "load_warning": "",
            "worker_status_text": "本轮补数中",
            "worker_next_run_at": "2026-03-28 16:00:00",
            "worker_cycle_updated_at": "2026-03-28 15:59:58",
            "worker_running": True,
        },
    )

    state = ResearchState()
    state.load_stock_page("000001.SZ")

    assert state.worker_status_text == "本轮补数中"
    assert state.worker_next_run_at == "2026-03-28 16:00:00"
    assert state.worker_cycle_updated_at == "2026-03-28 15:59:58"
    assert state.worker_running is True


def test_load_stock_page_uses_canonical_entity_key_from_view(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_cached_view",
        lambda stock_slug, **_kwargs: {
            "entity_key": "stock:601899.SH",
            "page_title": "紫金矿业 (601899.SH)",
            "signals": [{"summary": "继续拿着"}],
            "related_sectors": [],
            "signal_total": 1,
            "signal_page": 1,
            "signal_page_size": 5,
            "load_error": "",
        },
    )

    state = ResearchState()
    state.load_stock_page("紫金")

    assert state.page_title == "紫金矿业 (601899.SH)"
    assert state.entity_key == "stock:601899.SH"


def test_load_stock_page_if_needed_resets_stock_sidebar_when_stock_changes(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_cached_view",
        lambda stock_slug, **_kwargs: {
            "entity_key": "stock:601899.SH",
            "page_title": "紫金矿业 (601899.SH)",
            "signals": [],
            "related_sectors": [],
            "signal_total": 0,
            "signal_page": 1,
            "signal_page_size": 5,
            "load_error": "",
        },
    )

    state = ResearchState()
    state.loaded_once = True
    state.entity_type = "stock"
    state.entity_key = "stock:000001.SZ"
    state.stock_sidebar_open = True

    state.load_stock_page_if_needed("紫金")

    assert state.stock_sidebar_open is False
    assert state.stock_sidebar_loaded is False


def test_open_stock_sidebar_loads_sidebar_once(monkeypatch) -> None:
    calls: list[str] = []

    def _fake_load_stock_sidebar_cached_view(stock_slug: str) -> dict[str, object]:
        calls.append(stock_slug)
        return {
            "related_sectors": [{"sector_key": "gold"}],
            "extras_updated_at": "2026-04-03 12:00:00",
            "load_error": "",
        }

    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_sidebar_cached_view",
        _fake_load_stock_sidebar_cached_view,
        raising=False,
    )

    state = ResearchState()
    state.loaded_once = True
    state.entity_type = "stock"
    state.entity_key = "stock:601899.SH"

    state.open_stock_sidebar()
    assert state.stock_sidebar_open is True
    assert calls == ["stock:601899.SH"]

    state.open_stock_sidebar()
    assert calls == ["stock:601899.SH"]
    assert state.stock_sidebar_loaded is True
    assert state.related_items[0]["sector_key"] == "gold"
    assert not hasattr(state, "pending_candidates")


def test_open_feedback_dialog_resets_form_for_post() -> None:
    state = ResearchState()
    state.feedback_tag = "其他"
    state.feedback_note = "旧留言"
    state.feedback_error = "旧错误"
    state.feedback_success = "旧成功"

    state.open_feedback_dialog("weibo:1")

    assert state.feedback_dialog_open is True
    assert state.feedback_post_uid == "weibo:1"
    assert state.feedback_tag == ""
    assert state.feedback_note == ""
    assert state.feedback_error == ""
    assert state.feedback_success == ""


def test_submit_feedback_success_closes_dialog_and_refreshes(monkeypatch) -> None:
    submit_calls: list[dict[str, str]] = []
    clear_calls: list[str] = []
    reload_calls: list[str] = []

    def _fake_submit_post_analysis_feedback(
        *, post_uid: str, feedback_tag: str, feedback_note: str, entrypoint: str
    ) -> dict[str, str]:
        submit_calls.append(
            {
                "post_uid": post_uid,
                "feedback_tag": feedback_tag,
                "feedback_note": feedback_note,
                "entrypoint": entrypoint,
            }
        )
        return {
            "ok": "1",
            "message": "已记下，后台会在下一轮自动重跑。",
        }

    monkeypatch.setattr(
        "alphavault_reflex.research_state.submit_post_analysis_feedback",
        _fake_submit_post_analysis_feedback,
        raising=False,
    )
    monkeypatch.setattr(
        "alphavault_reflex.research_state.clear_reflex_source_caches",
        lambda: clear_calls.append("source"),
        raising=False,
    )
    monkeypatch.setattr(
        "alphavault_reflex.research_state.clear_stock_hot_read_caches",
        lambda: clear_calls.append("stock"),
        raising=False,
    )

    def _fake_load_stock_page_cached_view(stock_slug, **_kwargs) -> dict[str, object]:  # type: ignore[no-untyped-def]
        reload_calls.append(str(stock_slug or ""))
        return {
            "entity_key": "stock:600519.SH",
            "page_title": "贵州茅台",
            "signals": [],
            "signal_total": 0,
            "signal_page": 1,
            "signal_page_size": 5,
            "load_error": "",
        }

    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_cached_view",
        _fake_load_stock_page_cached_view,
        raising=False,
    )

    state = ResearchState()
    state.entity_key = "stock:600519.SH"
    state.feedback_dialog_open = True
    state.feedback_post_uid = "weibo:1"
    state.feedback_tag = "摘要错了"
    state.feedback_note = "原文是减仓，不是加仓"

    result = state.submit_feedback()

    assert result is None
    assert submit_calls == [
        {
            "post_uid": "weibo:1",
            "feedback_tag": "摘要错了",
            "feedback_note": "原文是减仓，不是加仓",
            "entrypoint": "stock_research",
        }
    ]
    assert clear_calls == ["source", "stock"]
    assert reload_calls == ["600519.SH"]
    assert state.feedback_dialog_open is False
    assert state.feedback_post_uid == ""
    assert state.feedback_tag == ""
    assert state.feedback_note == ""
    assert state.feedback_error == ""
    assert state.feedback_success == "已记下，后台会在下一轮自动重跑。"


def test_submit_feedback_failure_keeps_dialog_open_and_sets_error(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.research_state.submit_post_analysis_feedback",
        lambda **_kwargs: {
            "ok": "0",
            "message": "加入待重跑失败",
        },
        raising=False,
    )
    monkeypatch.setattr(
        ResearchState,
        "load_stock_page",
        lambda self, stock_slug=None: (_ for _ in ()).throw(
            AssertionError("失败时不该刷新")
        ),
    )

    state = ResearchState()
    state.entity_key = "stock:600519.SH"
    state.feedback_dialog_open = True
    state.feedback_post_uid = "weibo:1"
    state.feedback_tag = "动作错了"
    state.feedback_note = "原文是先看看"

    result = state.submit_feedback()

    assert result is None
    assert state.feedback_dialog_open is True
    assert state.feedback_post_uid == "weibo:1"
    assert state.feedback_tag == "动作错了"
    assert state.feedback_note == "原文是先看看"
    assert state.feedback_error == "加入待重跑失败"
    assert state.feedback_success == ""


def test_load_sector_page_sets_primary_signal(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_sector_page_view",
        lambda sector_slug: {
            "page_title": "white_liquor",
            "signals": [{"summary": "板块继续走强"}],
            "related_stocks": [{"stock_key": "stock:600519.SH"}],
        },
    )

    state = ResearchState()
    state.load_sector_page("white_liquor")
    assert state.page_title == "white_liquor"
    assert state.primary_signals[0]["summary"] == "板块继续走强"
    assert state.related_items[0]["stock_key"] == "stock:600519.SH"
    assert not hasattr(state, "pending_candidates")


def test_load_stock_page_if_needed_runs_on_first_load(monkeypatch) -> None:
    calls: list[str] = []

    def _fake_load_stock_page_cached_view(stock_slug, **_kwargs):
        calls.append(stock_slug)
        return {
            "entity_key": "stock:600519.SH",
            "page_title": "600519.SH",
            "signals": [{"summary": "继续加仓"}],
            "related_sectors": [],
            "signal_total": 1,
            "signal_page": 1,
            "signal_page_size": 5,
            "load_error": "",
        }

    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_cached_view",
        _fake_load_stock_page_cached_view,
    )

    state = ResearchState()
    state.load_stock_page_if_needed("600519.SH")

    assert calls == ["600519.SH"]
    assert state.entity_key == "stock:600519.SH"


def test_load_stock_page_if_needed_skips_when_same_stock_loaded(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_cached_view",
        lambda stock_slug, **_kwargs: calls.append(stock_slug),
    )

    state = ResearchState()
    state.loaded_once = True
    state.entity_type = "stock"
    state.entity_key = "stock:600519.SH"
    state.page_title = "贵州茅台"

    result = state.load_stock_page_if_needed("600519.SH")

    assert result is None
    assert calls == []
    assert state.page_title == "贵州茅台"


def test_load_stock_page_if_needed_loads_when_stock_changes(monkeypatch) -> None:
    calls: list[str] = []

    def _fake_load_stock_page_cached_view(stock_slug, **_kwargs):
        calls.append(stock_slug)
        return {
            "entity_key": "stock:600519.SH",
            "page_title": "600519.SH",
            "signals": [{"summary": "继续加仓"}],
            "related_sectors": [],
            "signal_total": 1,
            "signal_page": 1,
            "signal_page_size": 5,
            "load_error": "",
        }

    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_cached_view",
        _fake_load_stock_page_cached_view,
    )

    state = ResearchState()
    state.loaded_once = True
    state.entity_type = "stock"
    state.entity_key = "stock:000001.SZ"

    state.load_stock_page_if_needed("600519.SH")

    assert calls == ["600519.SH"]
    assert state.entity_key == "stock:600519.SH"


def test_next_signal_page_requests_next_page_from_loader(monkeypatch) -> None:
    seen_calls: list[dict[str, object]] = []

    def _fake_load_stock_page_cached_view(
        stock_slug: str,
        *,
        signal_page: int,
        signal_page_size: int,
    ) -> dict[str, object]:
        seen_calls.append(
            {
                "stock_slug": stock_slug,
                "signal_page": signal_page,
                "signal_page_size": signal_page_size,
            }
        )
        return {
            "entity_key": "stock:600519.SH",
            "page_title": "600519.SH",
            "signals": [{"summary": "第2页"}],
            "signal_total": 50,
            "signal_page": signal_page,
            "signal_page_size": signal_page_size,
            "load_error": "",
        }

    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_cached_view",
        _fake_load_stock_page_cached_view,
    )

    state = ResearchState()
    state.loaded_once = True
    state.entity_type = "stock"
    state.entity_key = "stock:600519.SH"
    state.signal_page = 1
    state.signal_page_size = 40
    state.signal_total = 90

    state.next_signal_page()

    assert seen_calls == [
        {
            "stock_slug": "600519.SH",
            "signal_page": 2,
            "signal_page_size": 40,
        }
    ]
    assert state.signal_page == 2
    assert state.signal_page_size == 40


def test_set_signal_page_size_uses_selected_page_size_in_loader(monkeypatch) -> None:
    seen_calls: list[dict[str, object]] = []

    def _fake_load_stock_page_cached_view(
        stock_slug: str,
        *,
        signal_page: int,
        signal_page_size: int,
    ) -> dict[str, object]:
        seen_calls.append(
            {
                "stock_slug": stock_slug,
                "signal_page": signal_page,
                "signal_page_size": signal_page_size,
            }
        )
        return {
            "entity_key": "stock:600519.SH",
            "page_title": "600519.SH",
            "signals": [{"summary": "第1页"}],
            "signal_total": 50,
            "signal_page": signal_page,
            "signal_page_size": signal_page_size,
            "load_error": "",
        }

    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_stock_page_cached_view",
        _fake_load_stock_page_cached_view,
    )

    state = ResearchState()
    state.loaded_once = True
    state.entity_type = "stock"
    state.entity_key = "stock:600519.SH"
    state.signal_page = 3
    state.signal_page_size = 20
    state.related_limit = 500

    state.set_signal_page_size("60")

    assert seen_calls == [
        {
            "stock_slug": "600519.SH",
            "signal_page": 1,
            "signal_page_size": 60,
        }
    ]
    assert state.signal_page == 1
    assert state.signal_page_size == 60


def test_load_sector_page_if_needed_skips_when_same_sector_loaded(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_sector_page_view",
        lambda sector_slug: calls.append(sector_slug),
    )

    state = ResearchState()
    state.loaded_once = True
    state.entity_type = "sector"
    state.entity_key = "cluster:white_liquor"
    state.page_title = "白酒"

    result = state.load_sector_page_if_needed("white_liquor")

    assert result is None
    assert calls == []
    assert state.page_title == "白酒"


def test_load_sector_page_if_needed_loads_when_sector_changes(monkeypatch) -> None:
    calls: list[str] = []

    def _fake_load_sector_page_view(sector_slug):
        calls.append(sector_slug)
        return {
            "page_title": "white_liquor",
            "signals": [{"summary": "板块继续走强"}],
            "related_stocks": [{"stock_key": "stock:600519.SH"}],
        }

    monkeypatch.setattr(
        "alphavault_reflex.research_state.load_sector_page_view",
        _fake_load_sector_page_view,
    )

    state = ResearchState()
    state.loaded_once = True
    state.entity_type = "sector"
    state.entity_key = "cluster:coal"

    state.load_sector_page_if_needed("white_liquor")

    assert calls == ["white_liquor"]
    assert state.entity_key == "cluster:white_liquor"


def test_research_state_does_not_keep_candidate_mutation_api() -> None:
    state = ResearchState()

    assert not hasattr(state, "pending_candidates")
    assert not hasattr(state, "accept_candidate")
    assert not hasattr(state, "ignore_candidate")
    assert not hasattr(state, "block_candidate")


def test_research_state_does_not_keep_backfill_api() -> None:
    state = ResearchState()

    assert not hasattr(state, "backfill_posts")
    assert not hasattr(state, "backfill_notice")
    assert not hasattr(state, "show_backfill_empty")
    assert not hasattr(state, "has_backfill_posts")
    assert not hasattr(state, "queue_backfill_post")


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
