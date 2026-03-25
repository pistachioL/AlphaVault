from __future__ import annotations

from alphavault_reflex.services.research_models import (
    build_sector_route,
    build_stock_route,
    canonical_stock_key,
)


def test_canonical_stock_key_normalizes_code_and_name_aliases() -> None:
    assert canonical_stock_key("stock:600519.sh") == "stock:600519.SH"
    assert canonical_stock_key("stock: 贵州茅台 ") == "stock:贵州茅台"


def test_build_stock_route_and_sector_route() -> None:
    assert build_stock_route("stock:600519.SH") == "/research/stocks/600519.SH"
    assert build_sector_route("cluster:gold") == "/research/sectors/gold"
