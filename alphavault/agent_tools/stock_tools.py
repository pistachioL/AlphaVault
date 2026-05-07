from __future__ import annotations

from typing import TypedDict

from alphavault.capabilities.stock_analysis import (
    DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS,
    DEFAULT_STOCK_EVIDENCE_MAX_POSTS,
    DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
    PortfolioContext,
    StockEvidencePack,
    get_portfolio_context,
    get_stock_evidence_pack,
)
from alphavault.capabilities.stock_lookup import (
    STOCK_RESOLVE_REQUIRED_ERROR,
    StockLookupRow,
    resolve_requested_stock_key,
    resolve_stock,
)
from alphavault.capabilities.stock_page import get_stock_page
from alphavault.domains.stock.view_scope import (
    DEFAULT_STOCK_VIEW_SCOPE,
    normalize_stock_view_scope,
)

DEFAULT_AGENT_STOCK_CANDIDATE_LIMIT = 5
DEFAULT_AGENT_SIGNAL_PAGE_SIZE = 10


class AgentStockCandidate(TypedDict):
    stock_key: str
    label: str
    subtitle: str
    match_reason: str
    is_exact: str


class AgentResolveStockResult(TypedDict):
    query: str
    resolved_stock_key: str
    has_unique_match: bool
    candidates: list[AgentStockCandidate]
    error: str


class AgentStockSignalRow(TypedDict):
    post_uid: str
    title: str
    preview: str
    author: str
    created_at: str
    url: str
    action: str
    signal_badge: str
    match_kind: str


AgentStockPageResult = TypedDict(
    "AgentStockPageResult",
    {
        "requested_stock": str,
        "resolved_stock_key": str,
        "view_scope": str,
        "covered_stock_keys": list[str],
        "page_title": str,
        "signal_total": int,
        "signal_page": int,
        "signal_page_size": int,
        "signals": list[AgentStockSignalRow],
        "related_sectors": list[dict[str, str]],
        "same_company_stocks": list[dict[str, str]],
        "load_error": str,
    },
)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _bool_text(value: object) -> bool:
    return bool(value)


def _coerce_int(value: object, *, fallback: int) -> int:
    try:
        return int(str(value or "").strip())
    except (TypeError, ValueError):
        return int(fallback)


def _trim_stock_candidates(rows: list[StockLookupRow]) -> list[AgentStockCandidate]:
    out: list[AgentStockCandidate] = []
    for row in rows:
        out.append(
            {
                "stock_key": _clean_text(row.get("stock_key")),
                "label": _clean_text(row.get("label")),
                "subtitle": _clean_text(row.get("subtitle")),
                "match_reason": _clean_text(row.get("match_reason")),
                "is_exact": _clean_text(row.get("is_exact")),
            }
        )
    return out


def ai_resolve_stock(
    query: str,
    *,
    limit: int = DEFAULT_AGENT_STOCK_CANDIDATE_LIMIT,
) -> AgentResolveStockResult:
    result = resolve_stock(query, limit=limit)
    resolved_stock_key = _clean_text(result.get("resolved_stock_key"))
    return {
        "query": _clean_text(query),
        "resolved_stock_key": resolved_stock_key,
        "has_unique_match": _bool_text(resolved_stock_key),
        "candidates": _trim_stock_candidates(result.get("rows") or []),
        "error": _clean_text(result.get("error")),
    }


def _trim_signal_rows(rows: object) -> list[AgentStockSignalRow]:
    if not isinstance(rows, list):
        return []
    out: list[AgentStockSignalRow] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "post_uid": _clean_text(row.get("post_uid")),
                "title": _clean_text(row.get("title")),
                "preview": _clean_text(row.get("preview")),
                "author": _clean_text(row.get("author")),
                "created_at": _clean_text(row.get("created_at_line"))
                or _clean_text(row.get("created_at")),
                "url": _clean_text(row.get("url")),
                "action": _clean_text(row.get("action")),
                "signal_badge": _clean_text(row.get("signal_badge")),
                "match_kind": _clean_text(row.get("match_kind")),
            }
        )
    return out


def _trim_named_rows(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for row in value:
        if not isinstance(row, dict):
            continue
        out.append(
            {str(key): _clean_text(raw) for key, raw in row.items() if str(key).strip()}
        )
    return out


def _trim_text_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = _clean_text(item)
        if text:
            out.append(text)
    return out


def ai_get_stock_page(
    stock: str,
    *,
    signal_page: int = 1,
    signal_page_size: int = DEFAULT_AGENT_SIGNAL_PAGE_SIZE,
    author: str = "",
    related_filter: str = "all",
    view_scope: str = DEFAULT_STOCK_VIEW_SCOPE,
) -> AgentStockPageResult:
    normalized_view_scope = normalize_stock_view_scope(view_scope)
    resolved_stock_key = resolve_requested_stock_key(
        stock,
        view_scope=normalized_view_scope,
    )
    if not resolved_stock_key:
        return {
            "requested_stock": _clean_text(stock),
            "resolved_stock_key": "",
            "view_scope": normalized_view_scope,
            "covered_stock_keys": [],
            "page_title": "",
            "signal_total": 0,
            "signal_page": 1,
            "signal_page_size": max(1, int(signal_page_size)),
            "signals": [],
            "related_sectors": [],
            "same_company_stocks": [],
            "load_error": STOCK_RESOLVE_REQUIRED_ERROR,
        }
    view = get_stock_page(
        resolved_stock_key,
        signal_page=signal_page,
        signal_page_size=signal_page_size,
        author=author,
        related_filter=related_filter,
        view_scope=normalized_view_scope,
    )
    return {
        "requested_stock": _clean_text(stock),
        "resolved_stock_key": resolved_stock_key,
        "view_scope": normalized_view_scope,
        "covered_stock_keys": _trim_text_list(view.get("covered_stock_keys")),
        "page_title": _clean_text(view.get("page_title")),
        "signal_total": _coerce_int(view.get("signal_total"), fallback=0),
        "signal_page": _coerce_int(view.get("signal_page"), fallback=1),
        "signal_page_size": _coerce_int(
            view.get("signal_page_size"),
            fallback=max(1, int(signal_page_size)),
        ),
        "signals": _trim_signal_rows(view.get("signals")),
        "related_sectors": _trim_named_rows(view.get("related_sectors")),
        "same_company_stocks": _trim_named_rows(view.get("same_company_stocks")),
        "load_error": _clean_text(view.get("load_error")),
    }


def ai_get_stock_evidence_pack(
    stock: str,
    *,
    window_days: int = DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
    max_posts: int = DEFAULT_STOCK_EVIDENCE_MAX_POSTS,
) -> StockEvidencePack:
    return get_stock_evidence_pack(
        stock,
        window_days=window_days,
        max_posts=max_posts,
    )


def ai_get_portfolio_context(
    stocks: list[str],
    *,
    window_days: int = DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
    max_posts_per_stock: int = DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS,
) -> PortfolioContext:
    return get_portfolio_context(
        stocks,
        window_days=window_days,
        max_posts_per_stock=max_posts_per_stock,
    )


__all__ = [
    "AgentResolveStockResult",
    "AgentStockCandidate",
    "AgentStockPageResult",
    "AgentStockSignalRow",
    "DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS",
    "DEFAULT_AGENT_SIGNAL_PAGE_SIZE",
    "DEFAULT_AGENT_STOCK_CANDIDATE_LIMIT",
    "DEFAULT_STOCK_EVIDENCE_MAX_POSTS",
    "DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS",
    "PortfolioContext",
    "STOCK_RESOLVE_REQUIRED_ERROR",
    "StockEvidencePack",
    "ai_get_portfolio_context",
    "ai_get_stock_evidence_pack",
    "ai_get_stock_page",
    "ai_resolve_stock",
]
