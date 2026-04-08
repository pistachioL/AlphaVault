from __future__ import annotations

from alphavault.domains.stock.keys import canonical_stock_key as _canonical_stock_key

STOCK_KEY_PREFIX = "stock:"
CLUSTER_KEY_PREFIX = "cluster:"
RESEARCH_STOCK_ROUTE_PREFIX = "/research/stocks"
RESEARCH_SECTOR_ROUTE_PREFIX = "/research/sectors"


def canonical_stock_key(raw_key: str) -> str:
    return _canonical_stock_key(raw_key)


def build_stock_route(stock_key: str) -> str:
    canonical = canonical_stock_key(stock_key)
    stock_slug = canonical.removeprefix(STOCK_KEY_PREFIX)
    return f"{RESEARCH_STOCK_ROUTE_PREFIX}/{stock_slug}"


def build_sector_route(sector_key: str) -> str:
    value = str(sector_key or "").strip()
    if value.startswith(CLUSTER_KEY_PREFIX):
        value = value[len(CLUSTER_KEY_PREFIX) :]
    return f"{RESEARCH_SECTOR_ROUTE_PREFIX}/{value}"
