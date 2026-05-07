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
from alphavault.capabilities.stock_obvious_trades import (
    DEFAULT_OBVIOUS_TRADE_FILTER,
    DEFAULT_OBVIOUS_TRADE_LIMIT,
    DEFAULT_OBVIOUS_TRADE_LOOKBACK_DAYS,
    DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH,
    DEFAULT_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE,
    DEFAULT_STOCK_OBVIOUS_TRADE_LOOKBACK_DAYS,
    ObviousTradeListResult,
    StockObviousTradeResult,
    get_stock_obvious_trades,
    list_obvious_trades,
)
from alphavault.capabilities.stock_page import get_stock_page
from alphavault.capabilities.stock_summary import StockSummaryResult, get_stock_summary
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


AgentObviousTradeRow = TypedDict(
    "AgentObviousTradeRow",
    {
        "stock_key": str,
        "page_title": str,
        "summary": str,
        "recent_action": str,
        "recent_author": str,
        "recent_created_at": str,
        "url": str,
        "buy_strength": str,
        "sell_strength": str,
        "net_strength": str,
        "mentions": str,
        "author_count": str,
        "post_uid": str,
    },
)


class AgentObviousTradeSignalRow(TypedDict):
    post_uid: str
    title: str
    preview: str
    author: str
    created_at: str
    url: str
    action: str
    action_strength: str
    signal_badge: str
    match_kind: str


AgentObviousTradeListResult = TypedDict(
    "AgentObviousTradeListResult",
    {
        "lookback_days": int,
        "trade_filter": str,
        "min_strength": int,
        "row_total": int,
        "has_more": bool,
        "rows": list[AgentObviousTradeRow],
        "load_error": str,
    },
)


AgentStockObviousTradeResult = TypedDict(
    "AgentStockObviousTradeResult",
    {
        "requested_stock": str,
        "resolved_stock_key": str,
        "view_scope": str,
        "covered_stock_keys": list[str],
        "page_title": str,
        "lookback_days": int,
        "trade_filter": str,
        "min_strength": int,
        "signal_total": int,
        "signal_page": int,
        "signal_page_size": int,
        "signals": list[AgentObviousTradeSignalRow],
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


def _signal_title(row: dict[str, object]) -> str:
    return (
        _clean_text(row.get("title"))
        or _clean_text(row.get("summary"))
        or _clean_text(row.get("raw_text"))
        or _clean_text(row.get("tree_text"))
    )


def _signal_preview(row: dict[str, object]) -> str:
    return (
        _clean_text(row.get("preview"))
        or _clean_text(row.get("raw_text"))
        or _clean_text(row.get("tree_text"))
        or _clean_text(row.get("summary"))
    )


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
                "title": _signal_title(row),
                "preview": _signal_preview(row),
                "author": _clean_text(row.get("author")),
                "created_at": _clean_text(row.get("created_at_line"))
                or _clean_text(row.get("created_at")),
                "url": _clean_text(row.get("url")),
                "action": _clean_text(row.get("action")),
                "signal_badge": _clean_text(row.get("signal_badge"))
                or _clean_text(row.get("action")),
                "match_kind": _clean_text(row.get("match_kind")),
            }
        )
    return out


def _trim_obvious_trade_rows(rows: object) -> list[AgentObviousTradeRow]:
    if not isinstance(rows, list):
        return []
    out: list[AgentObviousTradeRow] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "stock_key": _clean_text(row.get("stock_key")),
                "page_title": _clean_text(row.get("page_title")),
                "summary": _clean_text(row.get("summary")),
                "recent_action": _clean_text(row.get("recent_action")),
                "recent_author": _clean_text(row.get("recent_author")),
                "recent_created_at": _clean_text(row.get("recent_created_at")),
                "url": _clean_text(row.get("url")),
                "buy_strength": _clean_text(row.get("buy_strength")),
                "sell_strength": _clean_text(row.get("sell_strength")),
                "net_strength": _clean_text(row.get("net_strength")),
                "mentions": _clean_text(row.get("mentions")),
                "author_count": _clean_text(row.get("author_count")),
                "post_uid": _clean_text(row.get("post_uid")),
            }
        )
    return out


def _trim_obvious_trade_signal_rows(
    rows: object,
) -> list[AgentObviousTradeSignalRow]:
    if not isinstance(rows, list):
        return []
    out: list[AgentObviousTradeSignalRow] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "post_uid": _clean_text(row.get("post_uid")),
                "title": _signal_title(row),
                "preview": _signal_preview(row),
                "author": _clean_text(row.get("author")),
                "created_at": _clean_text(row.get("created_at")),
                "url": _clean_text(row.get("url")),
                "action": _clean_text(row.get("action")),
                "action_strength": _clean_text(row.get("action_strength")),
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


def ai_list_obvious_trades(
    *,
    lookback_days: int = DEFAULT_OBVIOUS_TRADE_LOOKBACK_DAYS,
    trade_filter: str = DEFAULT_OBVIOUS_TRADE_FILTER,
    min_strength: int = DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH,
    limit: int = DEFAULT_OBVIOUS_TRADE_LIMIT,
) -> AgentObviousTradeListResult:
    result: ObviousTradeListResult = list_obvious_trades(
        lookback_days=lookback_days,
        trade_filter=trade_filter,
        min_strength=min_strength,
        limit=limit,
    )
    return {
        "lookback_days": _coerce_int(
            result.get("lookback_days"),
            fallback=DEFAULT_OBVIOUS_TRADE_LOOKBACK_DAYS,
        ),
        "trade_filter": _clean_text(result.get("trade_filter")),
        "min_strength": _coerce_int(
            result.get("min_strength"),
            fallback=DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH,
        ),
        "row_total": _coerce_int(result.get("row_total"), fallback=0),
        "has_more": bool(result.get("has_more")),
        "rows": _trim_obvious_trade_rows(result.get("rows")),
        "load_error": _clean_text(result.get("load_error")),
    }


def ai_get_stock_obvious_trades(
    stock: str,
    *,
    lookback_days: int = DEFAULT_STOCK_OBVIOUS_TRADE_LOOKBACK_DAYS,
    trade_filter: str = DEFAULT_OBVIOUS_TRADE_FILTER,
    min_strength: int = DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH,
    signal_page: int = 1,
    signal_page_size: int = DEFAULT_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE,
    view_scope: str = DEFAULT_STOCK_VIEW_SCOPE,
) -> AgentStockObviousTradeResult:
    result: StockObviousTradeResult = get_stock_obvious_trades(
        stock,
        lookback_days=lookback_days,
        trade_filter=trade_filter,
        min_strength=min_strength,
        signal_page=signal_page,
        signal_page_size=signal_page_size,
        view_scope=view_scope,
    )
    return {
        "requested_stock": _clean_text(result.get("requested_stock")),
        "resolved_stock_key": _clean_text(result.get("resolved_stock_key")),
        "view_scope": _clean_text(result.get("view_scope")),
        "covered_stock_keys": _trim_text_list(result.get("covered_stock_keys")),
        "page_title": _clean_text(result.get("page_title")),
        "lookback_days": _coerce_int(
            result.get("lookback_days"),
            fallback=DEFAULT_STOCK_OBVIOUS_TRADE_LOOKBACK_DAYS,
        ),
        "trade_filter": _clean_text(result.get("trade_filter")),
        "min_strength": _coerce_int(
            result.get("min_strength"),
            fallback=DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH,
        ),
        "signal_total": _coerce_int(result.get("signal_total"), fallback=0),
        "signal_page": _coerce_int(result.get("signal_page"), fallback=1),
        "signal_page_size": _coerce_int(
            result.get("signal_page_size"),
            fallback=signal_page_size,
        ),
        "signals": _trim_obvious_trade_signal_rows(result.get("signals")),
        "same_company_stocks": _trim_named_rows(result.get("same_company_stocks")),
        "load_error": _clean_text(result.get("load_error")),
    }


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


def ai_get_stock_summary(
    stock: str,
    *,
    window_days: int = DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
    max_posts: int = DEFAULT_STOCK_EVIDENCE_MAX_POSTS,
) -> StockSummaryResult:
    return get_stock_summary(
        stock,
        window_days=window_days,
        max_posts=max_posts,
    )


__all__ = [
    "AgentObviousTradeListResult",
    "AgentObviousTradeRow",
    "AgentObviousTradeSignalRow",
    "AgentResolveStockResult",
    "AgentStockObviousTradeResult",
    "AgentStockCandidate",
    "AgentStockPageResult",
    "AgentStockSignalRow",
    "DEFAULT_OBVIOUS_TRADE_LOOKBACK_DAYS",
    "DEFAULT_OBVIOUS_TRADE_LIMIT",
    "DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH",
    "DEFAULT_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE",
    "DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS",
    "DEFAULT_AGENT_SIGNAL_PAGE_SIZE",
    "DEFAULT_AGENT_STOCK_CANDIDATE_LIMIT",
    "DEFAULT_STOCK_EVIDENCE_MAX_POSTS",
    "DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS",
    "DEFAULT_STOCK_OBVIOUS_TRADE_LOOKBACK_DAYS",
    "PortfolioContext",
    "STOCK_RESOLVE_REQUIRED_ERROR",
    "StockEvidencePack",
    "StockSummaryResult",
    "ai_get_stock_obvious_trades",
    "ai_get_portfolio_context",
    "ai_get_stock_evidence_pack",
    "ai_get_stock_page",
    "ai_get_stock_summary",
    "ai_list_obvious_trades",
    "ai_resolve_stock",
]
