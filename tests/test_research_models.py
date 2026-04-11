from __future__ import annotations

from urllib.parse import quote

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


def test_build_stock_route_normalizes_prefixed_cn_slug() -> None:
    assert build_stock_route("stock:SZ000725.US") == "/research/stocks/000725.SZ"


def test_build_stock_route_appends_author_query() -> None:
    author = "挖地瓜的超级鹿鼎公"

    assert build_stock_route("stock:600519.SH", author=author) == (
        f"/research/stocks/600519.SH?author={quote(author)}"
    )
