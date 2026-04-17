from __future__ import annotations

from typing import cast

from alphavault_reflex.services.research_data import SectorResearchView
from alphavault_reflex.services.research_page_loader import load_sector_page_view
from alphavault_reflex.services.research_page_loader import (
    load_stock_sidebar_cached_view,
)
from alphavault_reflex.services.research_page_loader import (
    load_stock_signal_detail_view,
)


def test_load_sector_page_view_prefers_cached_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.services.research_page_loader.load_sector_cached_view_from_env",
        lambda sector_key: {
            "entity_key": "cluster:white_liquor",
            "page_title": "white_liquor",
            "signals": [{"summary": "板块继续走强"}],
            "related_stocks": [{"stock_key": "stock:600519.SH"}],
            "load_error": "",
            "snapshot_hit": True,
        },
    )
    monkeypatch.setattr(
        "alphavault_reflex.services.research_page_loader.load_sources_from_env",
        lambda: (_ for _ in ()).throw(
            AssertionError("snapshot hit 时不该回退 live 读大表")
        ),
    )

    view = load_sector_page_view("white_liquor")
    signals = cast(list[dict[str, str]], view["signals"])
    related_stocks = cast(list[dict[str, str]], view["related_stocks"])

    assert view["page_title"] == "white_liquor"
    assert signals[0]["summary"] == "板块继续走强"
    assert related_stocks[0]["stock_key"] == "stock:600519.SH"


def test_load_sector_page_view_falls_back_to_live_when_snapshot_misses(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.services.research_page_loader.load_sector_cached_view_from_env",
        lambda sector_key: {
            "entity_key": f"cluster:{sector_key}",
            "page_title": sector_key,
            "signals": [],
            "related_stocks": [],
            "load_error": "",
            "snapshot_hit": False,
        },
    )
    monkeypatch.setattr(
        "alphavault_reflex.services.research_page_loader.load_sources_from_env",
        lambda: ([], [], ""),
    )
    monkeypatch.setattr(
        "alphavault_reflex.services.research_page_loader.build_sector_research_view",
        lambda posts, assertions, sector_key: SectorResearchView(
            page_title=sector_key,
            signals=[{"summary": "live fallback"}],
            related_stocks=[{"stock_key": "stock:000001.SZ"}],
        ),
    )

    view = load_sector_page_view("white_liquor")
    signals = cast(list[dict[str, str]], view["signals"])
    related_stocks = cast(list[dict[str, str]], view["related_stocks"])

    assert signals[0]["summary"] == "live fallback"
    assert related_stocks[0]["stock_key"] == "stock:000001.SZ"


def test_load_stock_sidebar_cached_view_uses_direct_sidebar_reader(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.services.research_page_loader.load_stock_cached_view_from_env",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("sidebar 不该再走主列表读取")
        ),
    )
    monkeypatch.setattr(
        "alphavault_reflex.services.research_page_loader.load_stock_sidebar_cached_view_from_env",
        lambda stock_key: {
            "related_sectors": [{"sector_key": f"from:{stock_key}"}],
            "load_error": "",
        },
        raising=False,
    )

    view = load_stock_sidebar_cached_view("600519.SH")
    related = cast(list[dict[str, str]], view["related_sectors"])

    assert related == [{"sector_key": "from:stock:600519.SH"}]


def test_load_stock_signal_detail_view_loads_single_post_raw_and_tree(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.services.research_page_loader.load_single_post_for_tree_from_env",
        lambda post_uid: (
            [
                {
                    "post_uid": post_uid,
                    "raw_text": "原文内容",
                }
            ],
            "",
        ),
        raising=False,
    )
    monkeypatch.setattr(
        "alphavault_reflex.services.research_page_loader.slice_posts_for_single_post_tree",
        lambda *, post_uid, posts: posts,
        raising=False,
    )
    monkeypatch.setattr(
        "alphavault_reflex.services.research_page_loader.build_tree",
        lambda *, post_uid, posts: ("标题", "根\n└── 叶"),
        raising=False,
    )

    view = load_stock_signal_detail_view("weibo:1")

    assert view["post_uid"] == "weibo:1"
    assert view["raw_text"] == "原文内容"
    assert view["tree_text"] == "根\n└── 叶"
    assert view["message"] == ""
