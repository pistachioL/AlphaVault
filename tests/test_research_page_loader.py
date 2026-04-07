from __future__ import annotations

import pandas as pd
from typing import cast

from alphavault_reflex.services.research_data import SectorResearchView
from alphavault_reflex.services.research_page_loader import load_sector_page_view


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
        lambda: (pd.DataFrame(), pd.DataFrame(), ""),
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
