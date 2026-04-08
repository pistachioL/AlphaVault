from __future__ import annotations

from urllib.parse import unquote

from alphavault.domains.stock.keys import normalize_stock_key as _normalize_stock_key
from alphavault_reflex.services.research_models import (
    build_sector_route,
    build_stock_route,
)
from alphavault_reflex.services.research_status_text import STOCK_CACHE_PREPARING_HINTS

DEFAULT_SIGNAL_PAGE_SIZE = 20
SIGNAL_PAGE_SIZE_OPTIONS = (20, 40, 60)
MAX_SIGNAL_PAGE_SIZE = 500


def resolve_route_slug(
    state: object,
    *,
    explicit_slug: str | None,
    route_key: str,
) -> str:
    slug = str(explicit_slug or "").strip()
    if slug:
        return slug
    router = getattr(state, "router", None)
    slug = resolve_route_slug_from_url(router, route_key=route_key)
    if slug:
        return slug
    route_value = getattr(state, route_key, "")
    slug = str(route_value or "").strip()
    if slug:
        return slug
    return ""


def resolve_route_slug_from_url(router: object, *, route_key: str) -> str:
    url = getattr(router, "url", None)
    query_parameters = getattr(url, "query_parameters", None)
    if query_parameters is not None:
        getter = getattr(query_parameters, "get", None)
        if callable(getter):
            slug = str(getter(route_key, "") or "").strip()
            if slug:
                return slug

    route_parts = split_route_parts(getattr(router, "route_id", ""))
    path_parts = split_route_parts(getattr(url, "path", ""))
    if len(route_parts) != len(path_parts):
        return ""

    target_part = f"[{route_key}]"
    for route_part, path_part in zip(route_parts, path_parts):
        if route_part == target_part:
            return unquote(path_part).strip()
    return ""


def split_route_parts(value: object) -> tuple[str, ...]:
    return tuple(
        part for part in str(value or "").strip().strip("/").split("/") if part
    )


def normalize_stock_key(stock_slug: str) -> str:
    return _normalize_stock_key(stock_slug)


def normalize_signal_page(value: object) -> int:
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        return 1
    if parsed <= 0:
        return 1
    return int(parsed)


def normalize_signal_page_size(value: object) -> int:
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        return DEFAULT_SIGNAL_PAGE_SIZE
    if parsed <= 0:
        return DEFAULT_SIGNAL_PAGE_SIZE
    return max(1, min(int(parsed), int(MAX_SIGNAL_PAGE_SIZE)))


def normalize_signal_total(value: object, *, fallback: int) -> int:
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        return max(int(fallback), 0)
    return max(parsed, 0)


def coerce_rows(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                str(key): str(raw or "").strip()
                for key, raw in item.items()
                if str(key).strip()
            }
        )
    return out


def prepare_sector_links(value: object) -> list[dict[str, str]]:
    rows = coerce_rows(value)
    out: list[dict[str, str]] = []
    for row in rows:
        sector_key = str(row.get("sector_key") or "").strip()
        if not sector_key:
            continue
        out.append(
            {
                **row,
                "label": sector_key,
                "href": build_sector_route(f"cluster:{sector_key}"),
            }
        )
    return out


def prepare_stock_links(value: object) -> list[dict[str, str]]:
    rows = coerce_rows(value)
    out: list[dict[str, str]] = []
    for row in rows:
        stock_key = str(row.get("stock_key") or "").strip()
        if not stock_key:
            continue
        out.append(
            {
                **row,
                "label": stock_key.removeprefix("stock:"),
                "href": build_stock_route(stock_key),
            }
        )
    return out


def is_stock_cache_preparing_warning(warning: str) -> bool:
    text = str(warning or "").strip()
    if not text:
        return False
    return any(hint in text for hint in STOCK_CACHE_PREPARING_HINTS)


__all__ = [
    "DEFAULT_SIGNAL_PAGE_SIZE",
    "MAX_SIGNAL_PAGE_SIZE",
    "SIGNAL_PAGE_SIZE_OPTIONS",
    "coerce_rows",
    "is_stock_cache_preparing_warning",
    "normalize_signal_page",
    "normalize_signal_page_size",
    "normalize_signal_total",
    "normalize_stock_key",
    "prepare_sector_links",
    "prepare_stock_links",
    "resolve_route_slug",
    "resolve_route_slug_from_url",
    "split_route_parts",
]
