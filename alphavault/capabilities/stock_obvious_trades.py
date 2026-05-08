from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from datetime import datetime
from functools import cache
from types import ModuleType
from typing import TypedDict

from alphavault.capabilities.stock_lookup import (
    STOCK_RESOLVE_REQUIRED_ERROR,
    resolve_requested_stock_key,
)
from alphavault.domains.stock.keys import stock_value
from alphavault.domains.stock.service import prepare_board_assertion_rows
from alphavault.domains.stock.view_scope import (
    DEFAULT_STOCK_VIEW_SCOPE,
    normalize_stock_view_scope,
)
from alphavault.research_workbench.trade_signal_review_service import (
    TradeReviewResult,
    build_trade_review_for_row,
    enrich_trade_signal_rows,
)


@cache
def _load_homework_constants_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.homework_constants")


def _load_homework_int_constant(name: str, *, default: int) -> int:
    value = getattr(_load_homework_constants_module(), name, default)
    try:
        return int(str(value or "").strip())
    except (TypeError, ValueError):
        return int(default)


@cache
def _load_homework_time_range_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.homework_time_range")


TRADE_BOARD_DEFAULT_WINDOW_DAYS = _load_homework_int_constant(
    "TRADE_BOARD_DEFAULT_WINDOW_DAYS",
    default=3,
)
TRADE_BOARD_MAX_WINDOW_DAYS = _load_homework_int_constant(
    "TRADE_BOARD_MAX_WINDOW_DAYS",
    default=90,
)
DEFAULT_OBVIOUS_TRADE_LOOKBACK_DAYS = TRADE_BOARD_DEFAULT_WINDOW_DAYS
DEFAULT_STOCK_OBVIOUS_TRADE_LOOKBACK_DAYS = 7
DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH = 2
DEFAULT_OBVIOUS_TRADE_LIMIT = 50
DEFAULT_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE = 20
MAX_OBVIOUS_TRADE_MIN_STRENGTH = 3
MAX_OBVIOUS_TRADE_LIMIT = 200
MAX_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE = 500
MAX_STOCK_OBVIOUS_TRADE_FETCH_SIZE = 5000

OBVIOUS_TRADE_FILTER_ALL = "all"
OBVIOUS_TRADE_FILTER_BUY = "buy"
OBVIOUS_TRADE_FILTER_SELL = "sell"
OBVIOUS_TRADE_FILTER_HOLD = "hold"
DEFAULT_OBVIOUS_TRADE_FILTER = OBVIOUS_TRADE_FILTER_ALL
OBVIOUS_TRADE_FILTERS = frozenset(
    {
        OBVIOUS_TRADE_FILTER_ALL,
        OBVIOUS_TRADE_FILTER_BUY,
        OBVIOUS_TRADE_FILTER_SELL,
        OBVIOUS_TRADE_FILTER_HOLD,
    }
)

_RELATED_FILTER_SIGNAL = "signal"
_MATCH_KIND_ASSERTION = "assertion"


ObviousTradeRow = TypedDict(
    "ObviousTradeRow",
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
        "recent_trade_review": TradeReviewResult,
    },
)


_ObviousTradeSeedRow = TypedDict(
    "_ObviousTradeSeedRow",
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
        "platform": str,
        "assertion_id": str,
        "action": str,
        "action_strength": str,
    },
)


ObviousTradeListResult = TypedDict(
    "ObviousTradeListResult",
    {
        "lookback_days": int,
        "trade_filter": str,
        "min_strength": int,
        "row_total": int,
        "has_more": bool,
        "rows": list[ObviousTradeRow],
        "load_error": str,
    },
)


class StockObviousTradeResult(TypedDict):
    requested_stock: str
    resolved_stock_key: str
    view_scope: str
    covered_stock_keys: list[str]
    page_title: str
    lookback_days: int
    trade_filter: str
    min_strength: int
    signal_total: int
    signal_page: int
    signal_page_size: int
    signals: list[dict[str, object]]
    same_company_stocks: list[dict[str, str]]
    load_error: str


@dataclass
class _TradeAggregate:
    recent_row: dict[str, object]
    recent_time: datetime
    buy_strength: int = 0
    sell_strength: int = 0
    mentions: int = 0
    authors: set[str] = field(default_factory=set)


@cache
def _load_homework_board_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.homework_board")


@cache
def _load_stock_hot_read_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.stock_hot_read")


@cache
def _load_trade_board_loader_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.trade_board_loader")


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _coerce_trade_board_timestamp(value: object) -> datetime | None:
    return _load_homework_time_range_module().coerce_homework_timestamp(value)


def _coerce_int(value: object, *, default: int) -> int:
    try:
        return int(str(value or "").strip())
    except (TypeError, ValueError):
        return int(default)


def _normalize_lookback_days(value: object, *, default: int) -> int:
    parsed = _coerce_int(value, default=default)
    return max(1, min(parsed, TRADE_BOARD_MAX_WINDOW_DAYS))


def _normalize_trade_filter(value: object) -> str:
    text = _clean_text(value).lower()
    return text if text in OBVIOUS_TRADE_FILTERS else OBVIOUS_TRADE_FILTER_ALL


def _normalize_min_strength(value: object) -> int:
    parsed = _coerce_int(value, default=DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH)
    return max(0, min(parsed, MAX_OBVIOUS_TRADE_MIN_STRENGTH))


def _normalize_limit(value: object) -> int:
    parsed = _coerce_int(value, default=DEFAULT_OBVIOUS_TRADE_LIMIT)
    return max(1, min(parsed, MAX_OBVIOUS_TRADE_LIMIT))


def _normalize_signal_page(value: object) -> int:
    parsed = _coerce_int(value, default=1)
    return max(1, parsed)


def _normalize_signal_page_size(value: object) -> int:
    parsed = _coerce_int(value, default=DEFAULT_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE)
    return max(1, min(parsed, MAX_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE))


def _allowed_trade_actions(trade_filter: str) -> frozenset[str]:
    homework_board = _load_homework_board_module()
    if trade_filter == OBVIOUS_TRADE_FILTER_BUY:
        return frozenset(homework_board.TRADE_BUY_ACTIONS)
    if trade_filter == OBVIOUS_TRADE_FILTER_SELL:
        return frozenset(homework_board.TRADE_SELL_ACTIONS)
    if trade_filter == OBVIOUS_TRADE_FILTER_HOLD:
        return frozenset(homework_board.TRADE_HOLD_ACTIONS)
    return frozenset(
        set(homework_board.TRADE_BUY_ACTIONS)
        | set(homework_board.TRADE_SELL_ACTIONS)
        | set(homework_board.TRADE_HOLD_ACTIONS)
    )


def _row_matches_trade_rule(
    row: dict[str, object] | dict[str, str],
    *,
    allowed_actions: frozenset[str],
    min_strength: int,
) -> bool:
    action = _clean_text(row.get("action"))
    if action not in allowed_actions:
        return False
    return _coerce_int(row.get("action_strength"), default=0) >= min_strength


def _stock_page_title(stock_key: str, page_titles: dict[str, str]) -> str:
    return (
        _clean_text(page_titles.get(stock_key)) or stock_value(stock_key) or stock_key
    )


def _empty_obvious_trade_list_result(
    *,
    lookback_days: int,
    trade_filter: str,
    min_strength: int,
    load_error: str = "",
) -> ObviousTradeListResult:
    return {
        "lookback_days": lookback_days,
        "trade_filter": trade_filter,
        "min_strength": min_strength,
        "row_total": 0,
        "has_more": False,
        "rows": [],
        "load_error": _clean_text(load_error),
    }


def _empty_stock_obvious_trade_result(
    *,
    requested_stock: str,
    view_scope: str,
    lookback_days: int,
    trade_filter: str,
    min_strength: int,
    signal_page_size: int,
    load_error: str = "",
) -> StockObviousTradeResult:
    return {
        "requested_stock": _clean_text(requested_stock),
        "resolved_stock_key": "",
        "view_scope": normalize_stock_view_scope(view_scope),
        "covered_stock_keys": [],
        "page_title": "",
        "lookback_days": lookback_days,
        "trade_filter": trade_filter,
        "min_strength": min_strength,
        "signal_total": 0,
        "signal_page": 1,
        "signal_page_size": signal_page_size,
        "signals": [],
        "same_company_stocks": [],
        "load_error": _clean_text(load_error),
    }


def _obvious_trade_sort_key(row: _ObviousTradeSeedRow) -> tuple[int, float, str]:
    recent_created_at = _clean_text(row.get("recent_created_at"))
    ts = _coerce_trade_board_timestamp(recent_created_at)
    if ts is None:
        return (1, 0.0, _clean_text(row.get("stock_key")))
    return (0, -ts.timestamp(), _clean_text(row.get("stock_key")))


def _signal_sort_key(row: dict[str, str]) -> tuple[int, float, str]:
    created_at = _clean_text(row.get("created_at"))
    ts = _coerce_trade_board_timestamp(created_at)
    post_uid = _clean_text(row.get("post_uid"))
    if ts is None:
        return (1, 0.0, post_uid)
    return (0, -ts.timestamp(), post_uid)


def _slice_signal_rows(
    rows: list[dict[str, str]],
    *,
    signal_page: int,
    signal_page_size: int,
) -> tuple[list[dict[str, str]], int, int]:
    total = len(rows)
    if total <= 0:
        return [], 0, 1
    safe_page = max(1, int(signal_page))
    safe_page_size = max(1, int(signal_page_size))
    total_pages = max(1, (total + safe_page_size - 1) // safe_page_size)
    safe_page = min(safe_page, total_pages)
    start = (safe_page - 1) * safe_page_size
    end = start + safe_page_size
    return rows[start:end], total, safe_page


def _normalize_market_trade_rows(
    rows: list[dict[str, object]],
    *,
    page_titles: dict[str, str],
    allowed_actions: frozenset[str],
    min_strength: int,
) -> list[_ObviousTradeSeedRow]:
    homework_board = _load_homework_board_module()
    grouped: dict[str, _TradeAggregate] = {}
    for raw_row in rows:
        stock_key = _clean_text(
            raw_row.get("board_group_key") or raw_row.get("entity_key")
        )
        if not stock_key.startswith("stock:"):
            continue
        if not _row_matches_trade_rule(
            raw_row,
            allowed_actions=allowed_actions,
            min_strength=min_strength,
        ):
            continue
        created_at = _coerce_trade_board_timestamp(raw_row.get("created_at"))
        if created_at is None:
            continue
        action = _clean_text(raw_row.get("action"))
        strength = _coerce_int(raw_row.get("action_strength"), default=0)
        bucket = grouped.setdefault(
            stock_key,
            _TradeAggregate(recent_row=dict(raw_row), recent_time=created_at),
        )
        bucket.mentions += 1
        author = _clean_text(raw_row.get("author"))
        if author:
            bucket.authors.add(author)
        if action in homework_board.TRADE_BUY_ACTIONS:
            bucket.buy_strength += strength
        if action in homework_board.TRADE_SELL_ACTIONS:
            bucket.sell_strength += strength
        if created_at >= bucket.recent_time:
            bucket.recent_time = created_at
            bucket.recent_row = dict(raw_row)

    out: list[_ObviousTradeSeedRow] = []
    for stock_key, bucket in grouped.items():
        recent_row = bucket.recent_row
        buy_strength = bucket.buy_strength
        sell_strength = bucket.sell_strength
        out.append(
            {
                "stock_key": stock_key,
                "page_title": _stock_page_title(stock_key, page_titles),
                "summary": _clean_text(recent_row.get("summary")),
                "recent_action": homework_board.trade_action_badge(
                    _clean_text(recent_row.get("action")),
                    recent_row.get("action_strength"),
                ),
                "recent_author": _clean_text(recent_row.get("author")),
                "recent_created_at": _clean_text(recent_row.get("created_at")),
                "url": _clean_text(recent_row.get("url")),
                "buy_strength": str(buy_strength),
                "sell_strength": str(sell_strength),
                "net_strength": str(buy_strength - sell_strength),
                "mentions": str(bucket.mentions),
                "author_count": str(len(bucket.authors)),
                "post_uid": _clean_text(recent_row.get("post_uid")),
                "platform": _clean_text(recent_row.get("source")),
                "assertion_id": _clean_text(recent_row.get("assertion_id")),
                "action": _clean_text(recent_row.get("action")),
                "action_strength": _clean_text(recent_row.get("action_strength")),
            }
        )
    return sorted(out, key=_obvious_trade_sort_key)


def _build_stock_signal_preview(row: dict[str, str]) -> str:
    summary = _clean_text(row.get("summary"))
    raw_text = _clean_text(row.get("raw_text"))
    tree_text = _clean_text(row.get("tree_text"))
    return raw_text or tree_text or summary


def _normalize_stock_signal_rows(
    rows: object,
    *,
    allowed_actions: frozenset[str],
    min_strength: int,
) -> list[dict[str, str]]:
    if not isinstance(rows, list):
        return []
    homework_board = _load_homework_board_module()
    out: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not _row_matches_trade_rule(
            row,
            allowed_actions=allowed_actions,
            min_strength=min_strength,
        ):
            continue
        out.append(
            {
                "platform": _clean_text(row.get("platform") or row.get("source")),
                "assertion_id": _clean_text(row.get("assertion_id")),
                "post_uid": _clean_text(row.get("post_uid")),
                "title": _clean_text(row.get("summary"))
                or _build_stock_signal_preview(row),
                "summary": _clean_text(row.get("summary")),
                "preview": _build_stock_signal_preview(row),
                "raw_text": _clean_text(row.get("raw_text")),
                "tree_text": _clean_text(row.get("tree_text")),
                "author": _clean_text(row.get("author")),
                "created_at": _clean_text(row.get("created_at")),
                "created_at_line": _clean_text(row.get("created_at_line"))
                or _clean_text(row.get("created_at")),
                "url": _clean_text(row.get("url")),
                "action": _clean_text(row.get("action")),
                "action_strength": _clean_text(row.get("action_strength")),
                "signal_badge": homework_board.trade_action_badge(
                    _clean_text(row.get("action")),
                    row.get("action_strength"),
                ),
                "match_kind": _clean_text(row.get("match_kind"))
                or _MATCH_KIND_ASSERTION,
            }
        )
    return sorted(out, key=_signal_sort_key)


def list_obvious_trades(
    *,
    lookback_days: int = DEFAULT_OBVIOUS_TRADE_LOOKBACK_DAYS,
    trade_filter: str = OBVIOUS_TRADE_FILTER_ALL,
    min_strength: int = DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH,
    limit: int = DEFAULT_OBVIOUS_TRADE_LIMIT,
) -> ObviousTradeListResult:
    normalized_lookback_days = _normalize_lookback_days(
        lookback_days,
        default=DEFAULT_OBVIOUS_TRADE_LOOKBACK_DAYS,
    )
    normalized_trade_filter = _normalize_trade_filter(trade_filter)
    normalized_min_strength = _normalize_min_strength(min_strength)
    normalized_limit = _normalize_limit(limit)

    trade_board_loader = _load_trade_board_loader_module()
    assertions, load_error = trade_board_loader.load_trade_board_assertions_from_env(
        normalized_lookback_days
    )
    if load_error:
        empty_error = _clean_text(
            getattr(trade_board_loader, "SOURCE_SCHEMAS_EMPTY_ERROR", "")
        )
        if _clean_text(load_error) != empty_error:
            return _empty_obvious_trade_list_result(
                lookback_days=normalized_lookback_days,
                trade_filter=normalized_trade_filter,
                min_strength=normalized_min_strength,
                load_error=load_error,
            )
        assertions = []
    relations, relation_error = trade_board_loader.load_stock_alias_relations_from_env()
    if relation_error:
        return _empty_obvious_trade_list_result(
            lookback_days=normalized_lookback_days,
            trade_filter=normalized_trade_filter,
            min_strength=normalized_min_strength,
            load_error=relation_error,
        )
    board_rows, page_titles = prepare_board_assertion_rows(
        assertions,
        stock_relations=relations,
    )
    normalized_rows = _normalize_market_trade_rows(
        board_rows,
        page_titles=page_titles,
        allowed_actions=_allowed_trade_actions(normalized_trade_filter),
        min_strength=normalized_min_strength,
    )
    limited_rows = normalized_rows[:normalized_limit]
    public_rows: list[ObviousTradeRow] = []
    for row in limited_rows:
        recent_trade_review = build_trade_review_for_row(
            {
                "platform": row.get("platform"),
                "assertion_id": row.get("assertion_id"),
                "post_uid": row.get("post_uid"),
                "author": row.get("recent_author"),
                "created_at": row.get("recent_created_at"),
                "summary": row.get("summary"),
                "action": row.get("action"),
                "action_strength": row.get("action_strength"),
            },
            stock_key=_clean_text(row.get("stock_key")),
        )
        public_rows.append(
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
                "recent_trade_review": recent_trade_review,
            }
        )
    return {
        "lookback_days": normalized_lookback_days,
        "trade_filter": normalized_trade_filter,
        "min_strength": normalized_min_strength,
        "row_total": len(normalized_rows),
        "has_more": len(normalized_rows) > len(limited_rows),
        "rows": public_rows,
        "load_error": "",
    }


def get_stock_obvious_trades(
    stock: str,
    *,
    lookback_days: int = DEFAULT_STOCK_OBVIOUS_TRADE_LOOKBACK_DAYS,
    trade_filter: str = OBVIOUS_TRADE_FILTER_ALL,
    min_strength: int = DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH,
    signal_page: int = 1,
    signal_page_size: int = DEFAULT_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE,
    view_scope: str = DEFAULT_STOCK_VIEW_SCOPE,
) -> StockObviousTradeResult:
    normalized_lookback_days = _normalize_lookback_days(
        lookback_days,
        default=DEFAULT_STOCK_OBVIOUS_TRADE_LOOKBACK_DAYS,
    )
    normalized_trade_filter = _normalize_trade_filter(trade_filter)
    normalized_min_strength = _normalize_min_strength(min_strength)
    normalized_signal_page = _normalize_signal_page(signal_page)
    normalized_signal_page_size = _normalize_signal_page_size(signal_page_size)
    normalized_view_scope = normalize_stock_view_scope(view_scope)
    resolved_stock_key = resolve_requested_stock_key(
        stock,
        view_scope=normalized_view_scope,
    )
    if not resolved_stock_key:
        return _empty_stock_obvious_trade_result(
            requested_stock=stock,
            view_scope=normalized_view_scope,
            lookback_days=normalized_lookback_days,
            trade_filter=normalized_trade_filter,
            min_strength=normalized_min_strength,
            signal_page_size=normalized_signal_page_size,
            load_error=STOCK_RESOLVE_REQUIRED_ERROR,
        )

    stock_hot_read = _load_stock_hot_read_module()
    view = stock_hot_read.load_stock_page_rows_from_env(
        resolved_stock_key,
        signal_page=1,
        signal_page_size=MAX_STOCK_OBVIOUS_TRADE_FETCH_SIZE,
        signal_window_days=normalized_lookback_days,
        related_filter=_RELATED_FILTER_SIGNAL,
        view_scope=normalized_view_scope,
    )
    signals = _normalize_stock_signal_rows(
        view.get("signals"),
        allowed_actions=_allowed_trade_actions(normalized_trade_filter),
        min_strength=normalized_min_strength,
    )
    signal_page_rows, signal_total, safe_signal_page = _slice_signal_rows(
        signals,
        signal_page=normalized_signal_page,
        signal_page_size=normalized_signal_page_size,
    )
    covered_stock_keys = [
        _clean_text(item)
        for item in (view.get("covered_stock_keys") or [])
        if _clean_text(item)
    ]
    enriched_signal_rows = enrich_trade_signal_rows(
        signal_page_rows,
        stock_key=_clean_text(view.get("entity_key") or resolved_stock_key),
        related_stock_keys=covered_stock_keys,
    )
    load_error = _clean_text(view.get("load_error")) if signal_total <= 0 else ""
    return {
        "requested_stock": _clean_text(stock),
        "resolved_stock_key": _clean_text(resolved_stock_key),
        "view_scope": normalized_view_scope,
        "covered_stock_keys": covered_stock_keys,
        "page_title": _clean_text(view.get("page_title")),
        "lookback_days": normalized_lookback_days,
        "trade_filter": normalized_trade_filter,
        "min_strength": normalized_min_strength,
        "signal_total": signal_total,
        "signal_page": safe_signal_page,
        "signal_page_size": normalized_signal_page_size,
        "signals": enriched_signal_rows,
        "same_company_stocks": [
            {str(key): _clean_text(raw) for key, raw in row.items() if _clean_text(key)}
            for row in (view.get("same_company_stocks") or [])
            if isinstance(row, dict)
        ],
        "load_error": load_error,
    }


__all__ = [
    "DEFAULT_OBVIOUS_TRADE_FILTER",
    "DEFAULT_OBVIOUS_TRADE_LIMIT",
    "DEFAULT_OBVIOUS_TRADE_LOOKBACK_DAYS",
    "DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH",
    "DEFAULT_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE",
    "DEFAULT_STOCK_OBVIOUS_TRADE_LOOKBACK_DAYS",
    "MAX_OBVIOUS_TRADE_LIMIT",
    "MAX_OBVIOUS_TRADE_MIN_STRENGTH",
    "MAX_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE",
    "OBVIOUS_TRADE_FILTER_ALL",
    "OBVIOUS_TRADE_FILTER_BUY",
    "OBVIOUS_TRADE_FILTER_HOLD",
    "OBVIOUS_TRADE_FILTER_SELL",
    "OBVIOUS_TRADE_FILTERS",
    "ObviousTradeListResult",
    "ObviousTradeRow",
    "StockObviousTradeResult",
    "get_stock_obvious_trades",
    "list_obvious_trades",
]
