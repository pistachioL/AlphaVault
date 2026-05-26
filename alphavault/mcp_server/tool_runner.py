from __future__ import annotations

import importlib
import json
from collections.abc import Mapping
from time import perf_counter
from types import ModuleType
from typing import TYPE_CHECKING, Callable, TypeVar
from uuid import uuid4

from .request_meta import McpRequestMeta

if TYPE_CHECKING:
    from alphavault.agent_tools.post_tools import (
        AgentPostDetailResult,
        AgentPostSearchResult,
    )
    from alphavault.agent_tools.stock_tools import (
        AgentObviousTradeListResult,
        AgentResolveStockResult,
        AgentStockObviousTradeResult,
        AgentStockPageResult,
    )
    from alphavault.capabilities.stock_analysis import (
        PortfolioContext,
        StockEvidencePack,
    )
    from alphavault.capabilities.stock_summary import StockSummaryResult

_STATUS_SUCCESS = "success"
_STATUS_ERROR = "error"
_SOURCE_KIND_OBVIOUS_TRADE = "obvious_trade_row"
_SOURCE_KIND_SEARCH_ROW = "search_row"
_SOURCE_KIND_STOCK_SIGNAL = "stock_signal"
_SOURCE_KIND_STOCK_EVIDENCE = "stock_evidence"
_SOURCE_KIND_PORTFOLIO_STOCK_EVIDENCE = "portfolio_stock_evidence"
_SOURCE_KIND_POST_DETAIL = "post_detail"
_MAX_HISTORY_POSTS = 120
_ToolResult = TypeVar("_ToolResult", bound=Mapping[str, object])


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _serialize_input(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _load_stock_tools_module() -> ModuleType:
    return importlib.import_module("alphavault.agent_tools.stock_tools")


def _load_post_tools_module() -> ModuleType:
    return importlib.import_module("alphavault.agent_tools.post_tools")


def _load_history_repo_module() -> ModuleType:
    return importlib.import_module("alphavault.mcp_history.repo")


def _load_research_service_module() -> ModuleType:
    return importlib.import_module("alphavault.research_workbench.service")


def _error_text(result: object, *keys: str) -> str:
    if not isinstance(result, dict):
        return ""
    for key in keys:
        text = _clean_text(result.get(key))
        if text:
            return text
    return ""


def _result_count(result: object, *, rows_key: str) -> int:
    if not isinstance(result, dict):
        return 0
    rows = result.get(rows_key)
    if isinstance(rows, list):
        return len(rows)
    return 0


def _stock_page_history_posts(result: object) -> list[dict[str, str]]:
    if not isinstance(result, dict):
        return []
    rows = result.get("signals")
    if not isinstance(rows, list):
        return []
    return [
        {
            "post_uid": _clean_text(row.get("post_uid"))
            if isinstance(row, dict)
            else "",
            "source_kind": _SOURCE_KIND_STOCK_SIGNAL,
            "title": _clean_text(row.get("title")) if isinstance(row, dict) else "",
            "author": _clean_text(row.get("author")) if isinstance(row, dict) else "",
            "created_at": _clean_text(row.get("created_at"))
            if isinstance(row, dict)
            else "",
            "url": _clean_text(row.get("url")) if isinstance(row, dict) else "",
            "match_reason": _clean_text(row.get("match_kind"))
            if isinstance(row, dict)
            else "",
            "preview": _clean_text(row.get("preview")) if isinstance(row, dict) else "",
        }
        for row in rows
        if isinstance(row, dict) and _clean_text(row.get("post_uid"))
    ]


def _stock_evidence_history_posts(result: object) -> list[dict[str, str]]:
    if not isinstance(result, dict):
        return []
    rows = result.get("evidence_rows")
    if not isinstance(rows, list):
        return []
    return [
        {
            "post_uid": _clean_text(row.get("post_uid"))
            if isinstance(row, dict)
            else "",
            "source_kind": _SOURCE_KIND_STOCK_EVIDENCE,
            "title": _clean_text(row.get("summary")) if isinstance(row, dict) else "",
            "author": _clean_text(row.get("author")) if isinstance(row, dict) else "",
            "created_at": _clean_text(row.get("created_at"))
            if isinstance(row, dict)
            else "",
            "url": _clean_text(row.get("url")) if isinstance(row, dict) else "",
            "match_reason": _clean_text(row.get("stance"))
            if isinstance(row, dict)
            else "",
            "preview": _clean_text(row.get("raw_text"))
            if isinstance(row, dict)
            else "",
        }
        for row in rows
        if isinstance(row, dict) and _clean_text(row.get("post_uid"))
    ][:_MAX_HISTORY_POSTS]


def _portfolio_history_posts(result: object) -> list[dict[str, str]]:
    if not isinstance(result, dict):
        return []
    companies = result.get("companies")
    if not isinstance(companies, list):
        return []
    posts: list[dict[str, str]] = []
    for company in companies:
        if not isinstance(company, dict):
            continue
        page_title = _clean_text(company.get("page_title"))
        evidence_rows = company.get("evidence_rows")
        if not isinstance(evidence_rows, list):
            continue
        for row in evidence_rows:
            if not isinstance(row, dict) or not _clean_text(row.get("post_uid")):
                continue
            summary = _clean_text(row.get("summary"))
            title = f"{page_title} | {summary}" if page_title and summary else summary
            posts.append(
                {
                    "post_uid": _clean_text(row.get("post_uid")),
                    "source_kind": _SOURCE_KIND_PORTFOLIO_STOCK_EVIDENCE,
                    "title": title,
                    "author": _clean_text(row.get("author")),
                    "created_at": _clean_text(row.get("created_at")),
                    "url": _clean_text(row.get("url")),
                    "match_reason": _clean_text(row.get("stance")),
                    "preview": _clean_text(row.get("raw_text")),
                }
            )
            if len(posts) >= _MAX_HISTORY_POSTS:
                return posts
    return posts


def _single_company_resolved_stock_key(result: object) -> str:
    if not isinstance(result, dict):
        return ""
    companies = result.get("companies")
    if not isinstance(companies, list) or len(companies) != 1:
        return ""
    company = companies[0]
    if not isinstance(company, dict):
        return ""
    return _clean_text(company.get("resolved_stock_key"))


def _search_history_posts(result: object) -> list[dict[str, str]]:
    if not isinstance(result, dict):
        return []
    rows = result.get("rows")
    if not isinstance(rows, list):
        return []
    return [
        {
            "post_uid": _clean_text(row.get("post_uid"))
            if isinstance(row, dict)
            else "",
            "source_kind": _SOURCE_KIND_SEARCH_ROW,
            "title": _clean_text(row.get("title")) if isinstance(row, dict) else "",
            "author": _clean_text(row.get("author")) if isinstance(row, dict) else "",
            "created_at": _clean_text(row.get("created_at"))
            if isinstance(row, dict)
            else "",
            "url": _clean_text(row.get("url")) if isinstance(row, dict) else "",
            "match_reason": _clean_text(row.get("match_reason"))
            if isinstance(row, dict)
            else "",
            "preview": _clean_text(row.get("preview")) if isinstance(row, dict) else "",
        }
        for row in rows
        if isinstance(row, dict) and _clean_text(row.get("post_uid"))
    ]


def _obvious_trade_history_posts(result: object) -> list[dict[str, str]]:
    if not isinstance(result, dict):
        return []
    rows = result.get("rows")
    if not isinstance(rows, list):
        return []
    return [
        {
            "post_uid": _clean_text(row.get("post_uid"))
            if isinstance(row, dict)
            else "",
            "source_kind": _SOURCE_KIND_OBVIOUS_TRADE,
            "title": _clean_text(row.get("page_title"))
            if isinstance(row, dict)
            else "",
            "author": _clean_text(row.get("recent_author"))
            if isinstance(row, dict)
            else "",
            "created_at": _clean_text(row.get("recent_created_at"))
            if isinstance(row, dict)
            else "",
            "url": _clean_text(row.get("url")) if isinstance(row, dict) else "",
            "match_reason": _clean_text(row.get("recent_action"))
            if isinstance(row, dict)
            else "",
            "preview": _clean_text(row.get("summary")) if isinstance(row, dict) else "",
        }
        for row in rows
        if isinstance(row, dict) and _clean_text(row.get("post_uid"))
    ]


def _post_detail_history_posts(
    result: object, *, requested_post_uid: str
) -> list[dict[str, str]]:
    if not isinstance(result, dict):
        return []
    resolved_post_uid = _clean_text(result.get("post_uid")) or _clean_text(
        requested_post_uid
    )
    if not resolved_post_uid:
        return []
    return [
        {
            "post_uid": resolved_post_uid,
            "source_kind": _SOURCE_KIND_POST_DETAIL,
            "title": "",
            "author": "",
            "created_at": "",
            "url": "",
            "match_reason": _clean_text(result.get("message")),
            "preview": "",
        }
    ]


def _record_call(
    *,
    request_meta: McpRequestMeta,
    tool_name: str,
    input_payload: dict[str, object],
    resolved_stock_key: str,
    result_count: int,
    error_text: str,
    posts: list[dict[str, str]],
    duration_ms: int,
) -> None:
    history_repo = _load_history_repo_module()
    engine = _load_research_service_module().get_research_workbench_engine_from_env()
    history_repo.record_mcp_call_history(
        engine,
        call_id=uuid4().hex,
        trace_id=request_meta.trace_id,
        tool_name=tool_name,
        status=_STATUS_ERROR if error_text else _STATUS_SUCCESS,
        auth_mode=request_meta.auth_mode,
        request_path=request_meta.request_path,
        input_json=_serialize_input(input_payload),
        resolved_stock_key=_clean_text(resolved_stock_key),
        result_count=max(0, int(result_count or 0)),
        error_text=_clean_text(error_text),
        duration_ms=max(0, int(duration_ms or 0)),
        cf_ray=request_meta.cf_ray,
        access_subject=request_meta.access_subject,
        access_email=request_meta.access_email,
        access_aud=request_meta.access_aud,
        posts=posts,
    )


def _run_tool_call(
    *,
    request_meta: McpRequestMeta,
    tool_name: str,
    input_payload: dict[str, object],
    call_fn: Callable[[], _ToolResult],
    resolved_stock_key_fn: Callable[[Mapping[str, object]], str],
    result_count_fn: Callable[[Mapping[str, object]], int],
    error_text_fn: Callable[[Mapping[str, object]], str],
    posts_fn: Callable[[Mapping[str, object]], list[dict[str, str]]],
) -> _ToolResult:
    started_at = perf_counter()
    result_for_record: Mapping[str, object] = {}
    error_text = ""
    try:
        result = call_fn()
        result_for_record = result
        error_text = _clean_text(error_text_fn(result))
        return result
    except BaseException as err:
        error_text = f"{type(err).__name__}:{_clean_text(err)}"
        raise
    finally:
        duration_ms = int((perf_counter() - started_at) * 1000)
        _record_call(
            request_meta=request_meta,
            tool_name=tool_name,
            input_payload=input_payload,
            resolved_stock_key=resolved_stock_key_fn(result_for_record),
            result_count=result_count_fn(result_for_record),
            error_text=error_text,
            posts=posts_fn(result_for_record),
            duration_ms=duration_ms,
        )


def run_resolve_stock_tool(
    *,
    request_meta: McpRequestMeta,
    query: str,
    limit: int,
) -> AgentResolveStockResult:
    stock_tools = _load_stock_tools_module()
    return _run_tool_call(
        request_meta=request_meta,
        tool_name="resolve_stock",
        input_payload={"query": _clean_text(query), "limit": max(1, int(limit or 0))},
        call_fn=lambda: stock_tools.ai_resolve_stock(query, limit=limit),
        resolved_stock_key_fn=lambda result: _clean_text(
            result.get("resolved_stock_key")
        ),
        result_count_fn=lambda result: _result_count(result, rows_key="candidates"),
        error_text_fn=lambda result: _error_text(result, "error"),
        posts_fn=lambda _result: [],
    )


def run_get_stock_page_tool(
    *,
    request_meta: McpRequestMeta,
    stock: str,
    signal_page: int,
    signal_page_size: int,
    author: str,
    related_filter: str,
    view_scope: str,
) -> AgentStockPageResult:
    stock_tools = _load_stock_tools_module()
    return _run_tool_call(
        request_meta=request_meta,
        tool_name="get_stock_page",
        input_payload={
            "stock": _clean_text(stock),
            "signal_page": max(1, int(signal_page or 0)),
            "signal_page_size": max(1, int(signal_page_size or 0)),
            "author": _clean_text(author),
            "related_filter": _clean_text(related_filter),
            "view_scope": _clean_text(view_scope),
        },
        call_fn=lambda: stock_tools.ai_get_stock_page(
            stock,
            signal_page=signal_page,
            signal_page_size=signal_page_size,
            author=author,
            related_filter=related_filter,
            view_scope=view_scope,
        ),
        resolved_stock_key_fn=lambda result: _clean_text(
            result.get("resolved_stock_key")
        ),
        result_count_fn=lambda result: _result_count(result, rows_key="signals"),
        error_text_fn=lambda result: _error_text(result, "load_error", "error"),
        posts_fn=_stock_page_history_posts,
    )


def run_list_obvious_trades_tool(
    *,
    request_meta: McpRequestMeta,
    lookback_days: int,
    trade_filter: str,
    min_strength: int,
    limit: int,
) -> AgentObviousTradeListResult:
    stock_tools = _load_stock_tools_module()
    return _run_tool_call(
        request_meta=request_meta,
        tool_name="list_obvious_trades",
        input_payload={
            "lookback_days": max(1, int(lookback_days or 0)),
            "trade_filter": _clean_text(trade_filter),
            "min_strength": max(0, int(min_strength or 0)),
            "limit": max(1, int(limit or 0)),
        },
        call_fn=lambda: stock_tools.ai_list_obvious_trades(
            lookback_days=lookback_days,
            trade_filter=trade_filter,
            min_strength=min_strength,
            limit=limit,
        ),
        resolved_stock_key_fn=lambda _result: "",
        result_count_fn=lambda result: _result_count(result, rows_key="rows"),
        error_text_fn=lambda result: _error_text(result, "load_error", "error"),
        posts_fn=_obvious_trade_history_posts,
    )


def run_get_stock_obvious_trades_tool(
    *,
    request_meta: McpRequestMeta,
    stock: str,
    lookback_days: int,
    trade_filter: str,
    min_strength: int,
    signal_page: int,
    signal_page_size: int,
    view_scope: str,
) -> AgentStockObviousTradeResult:
    stock_tools = _load_stock_tools_module()
    return _run_tool_call(
        request_meta=request_meta,
        tool_name="get_stock_obvious_trades",
        input_payload={
            "stock": _clean_text(stock),
            "lookback_days": max(1, int(lookback_days or 0)),
            "trade_filter": _clean_text(trade_filter),
            "min_strength": max(0, int(min_strength or 0)),
            "signal_page": max(1, int(signal_page or 0)),
            "signal_page_size": max(1, int(signal_page_size or 0)),
            "view_scope": _clean_text(view_scope),
        },
        call_fn=lambda: stock_tools.ai_get_stock_obvious_trades(
            stock,
            lookback_days=lookback_days,
            trade_filter=trade_filter,
            min_strength=min_strength,
            signal_page=signal_page,
            signal_page_size=signal_page_size,
            view_scope=view_scope,
        ),
        resolved_stock_key_fn=lambda result: _clean_text(
            result.get("resolved_stock_key")
        ),
        result_count_fn=lambda result: _result_count(result, rows_key="signals"),
        error_text_fn=lambda result: _error_text(result, "load_error", "error"),
        posts_fn=_stock_page_history_posts,
    )


def run_get_stock_evidence_pack_tool(
    *,
    request_meta: McpRequestMeta,
    stock: str,
    window_days: int,
    max_posts: int,
) -> StockEvidencePack:
    stock_tools = _load_stock_tools_module()
    return _run_tool_call(
        request_meta=request_meta,
        tool_name="get_stock_evidence_pack",
        input_payload={
            "stock": _clean_text(stock),
            "window_days": max(1, int(window_days or 0)),
            "max_posts": max(1, int(max_posts or 0)),
        },
        call_fn=lambda: stock_tools.ai_get_stock_evidence_pack(
            stock,
            window_days=window_days,
            max_posts=max_posts,
        ),
        resolved_stock_key_fn=lambda result: _clean_text(
            result.get("resolved_stock_key")
        ),
        result_count_fn=lambda result: _result_count(result, rows_key="evidence_rows"),
        error_text_fn=lambda result: _error_text(result, "load_error", "error"),
        posts_fn=_stock_evidence_history_posts,
    )


def run_get_stock_summary_tool(
    *,
    request_meta: McpRequestMeta,
    stock: str,
    window_days: int,
    max_posts: int,
) -> StockSummaryResult:
    stock_tools = _load_stock_tools_module()
    return _run_tool_call(
        request_meta=request_meta,
        tool_name="get_stock_summary",
        input_payload={
            "stock": _clean_text(stock),
            "window_days": max(1, int(window_days or 0)),
            "max_posts": max(1, int(max_posts or 0)),
        },
        call_fn=lambda: stock_tools.ai_get_stock_summary(
            stock,
            window_days=window_days,
            max_posts=max_posts,
        ),
        resolved_stock_key_fn=lambda result: _clean_text(
            result.get("resolved_stock_key")
        ),
        result_count_fn=lambda result: _result_count(result, rows_key="evidence_rows"),
        error_text_fn=lambda result: _error_text(result, "load_error", "error"),
        posts_fn=_stock_evidence_history_posts,
    )


def run_get_portfolio_context_tool(
    *,
    request_meta: McpRequestMeta,
    stocks: list[str],
    window_days: int,
    max_posts_per_stock: int,
) -> PortfolioContext:
    stock_tools = _load_stock_tools_module()
    return _run_tool_call(
        request_meta=request_meta,
        tool_name="get_portfolio_context",
        input_payload={
            "stocks": [
                str(item or "").strip() for item in stocks if str(item or "").strip()
            ],
            "window_days": max(1, int(window_days or 0)),
            "max_posts_per_stock": max(1, int(max_posts_per_stock or 0)),
        },
        call_fn=lambda: stock_tools.ai_get_portfolio_context(
            stocks,
            window_days=window_days,
            max_posts_per_stock=max_posts_per_stock,
        ),
        resolved_stock_key_fn=_single_company_resolved_stock_key,
        result_count_fn=lambda result: _result_count(result, rows_key="companies"),
        error_text_fn=lambda result: _error_text(result, "load_error", "error"),
        posts_fn=_portfolio_history_posts,
    )


def run_search_posts_tool(
    *,
    request_meta: McpRequestMeta,
    query: str,
    limit: int,
    cursor: str,
) -> AgentPostSearchResult:
    post_tools = _load_post_tools_module()
    return _run_tool_call(
        request_meta=request_meta,
        tool_name="search_posts",
        input_payload={
            "query": _clean_text(query),
            "limit": max(1, int(limit or 0)),
            "cursor": _clean_text(cursor),
        },
        call_fn=lambda: post_tools.ai_search_posts(query, limit=limit, cursor=cursor),
        resolved_stock_key_fn=lambda _result: "",
        result_count_fn=lambda result: _result_count(result, rows_key="rows"),
        error_text_fn=lambda result: _error_text(result, "error"),
        posts_fn=_search_history_posts,
    )


def run_search_posts_semantic_tool(
    *,
    request_meta: McpRequestMeta,
    query: str,
    limit: int,
    cursor: str,
    candidate_limit: int,
) -> AgentPostSearchResult:
    post_tools = _load_post_tools_module()
    return _run_tool_call(
        request_meta=request_meta,
        tool_name="search_posts_semantic",
        input_payload={
            "query": _clean_text(query),
            "limit": max(1, int(limit or 0)),
            "cursor": _clean_text(cursor),
            "candidate_limit": max(1, int(candidate_limit or 0)),
        },
        call_fn=lambda: post_tools.ai_search_posts_semantic(
            query,
            limit=limit,
            cursor=cursor,
            candidate_limit=candidate_limit,
        ),
        resolved_stock_key_fn=lambda _result: "",
        result_count_fn=lambda result: _result_count(result, rows_key="rows"),
        error_text_fn=lambda result: _error_text(result, "error"),
        posts_fn=_search_history_posts,
    )


def run_get_post_detail_tool(
    *,
    request_meta: McpRequestMeta,
    post_uid: str,
) -> AgentPostDetailResult:
    post_tools = _load_post_tools_module()
    return _run_tool_call(
        request_meta=request_meta,
        tool_name="get_post_detail",
        input_payload={"post_uid": _clean_text(post_uid)},
        call_fn=lambda: post_tools.ai_get_post_detail(post_uid),
        resolved_stock_key_fn=lambda _result: "",
        result_count_fn=lambda result: 1 if _clean_text(result.get("post_uid")) else 0,
        error_text_fn=lambda result: _error_text(result, "load_error"),
        posts_fn=lambda result: _post_detail_history_posts(
            result,
            requested_post_uid=post_uid,
        ),
    )


__all__ = [
    "run_get_post_detail_tool",
    "run_get_portfolio_context_tool",
    "run_get_stock_obvious_trades_tool",
    "run_get_stock_evidence_pack_tool",
    "run_get_stock_page_tool",
    "run_get_stock_summary_tool",
    "run_list_obvious_trades_tool",
    "run_resolve_stock_tool",
    "run_search_posts_tool",
    "run_search_posts_semantic_tool",
]
