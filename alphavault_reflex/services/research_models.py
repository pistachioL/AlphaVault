from __future__ import annotations


STOCK_KEY_PREFIX = "stock:"
CLUSTER_KEY_PREFIX = "cluster:"
RESEARCH_STOCK_ROUTE_PREFIX = "/research/stocks"
RESEARCH_SECTOR_ROUTE_PREFIX = "/research/sectors"


def canonical_stock_key(raw_key: str) -> str:
    value = str(raw_key or "").strip()
    if not value:
        return ""
    if not value.startswith(STOCK_KEY_PREFIX):
        return value
    stock_value = value[len(STOCK_KEY_PREFIX) :].strip()
    if not stock_value:
        return STOCK_KEY_PREFIX
    if "." in stock_value:
        code, market = stock_value.rsplit(".", 1)
        if code.isdigit() and market:
            return f"{STOCK_KEY_PREFIX}{code}.{market.upper()}"
    return f"{STOCK_KEY_PREFIX}{stock_value}"


def build_stock_route(stock_key: str) -> str:
    canonical = canonical_stock_key(stock_key)
    stock_slug = canonical.removeprefix(STOCK_KEY_PREFIX)
    return f"{RESEARCH_STOCK_ROUTE_PREFIX}/{stock_slug}"


def build_sector_route(sector_key: str) -> str:
    value = str(sector_key or "").strip()
    if value.startswith(CLUSTER_KEY_PREFIX):
        value = value[len(CLUSTER_KEY_PREFIX) :]
    return f"{RESEARCH_SECTOR_ROUTE_PREFIX}/{value}"
