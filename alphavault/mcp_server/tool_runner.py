from __future__ import annotations

import importlib
import json
from time import perf_counter
from types import ModuleType
from typing import Callable
from uuid import uuid4

from .request_meta import McpRequestMeta

_STATUS_SUCCESS = "success"
_STATUS_ERROR = "error"
_SOURCE_KIND_SEARCH_ROW = "search_row"
_SOURCE_KIND_STOCK_SIGNAL = "stock_signal"
_SOURCE_KIND_POST_DETAIL = "post_detail"


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
    call_fn: Callable[[], dict[str, object]],
    resolved_stock_key_fn: Callable[[dict[str, object]], str],
    result_count_fn: Callable[[dict[str, object]], int],
    error_text_fn: Callable[[dict[str, object]], str],
    posts_fn: Callable[[dict[str, object]], list[dict[str, str]]],
) -> dict[str, object]:
    started_at = perf_counter()
    result: dict[str, object] = {}
    error_text = ""
    try:
        result = dict(call_fn() or {})
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
            resolved_stock_key=resolved_stock_key_fn(result),
            result_count=result_count_fn(result),
            error_text=error_text,
            posts=posts_fn(result),
            duration_ms=duration_ms,
        )


def run_resolve_stock_tool(
    *,
    request_meta: McpRequestMeta,
    query: str,
    limit: int,
) -> dict[str, object]:
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
) -> dict[str, object]:
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
        },
        call_fn=lambda: stock_tools.ai_get_stock_page(
            stock,
            signal_page=signal_page,
            signal_page_size=signal_page_size,
            author=author,
            related_filter=related_filter,
        ),
        resolved_stock_key_fn=lambda result: _clean_text(
            result.get("resolved_stock_key")
        ),
        result_count_fn=lambda result: _result_count(result, rows_key="signals"),
        error_text_fn=lambda result: _error_text(result, "load_error", "error"),
        posts_fn=_stock_page_history_posts,
    )


def run_search_posts_tool(
    *,
    request_meta: McpRequestMeta,
    query: str,
    limit: int,
    cursor: str,
) -> dict[str, object]:
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


def run_get_post_detail_tool(
    *,
    request_meta: McpRequestMeta,
    post_uid: str,
) -> dict[str, object]:
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
    "run_get_stock_page_tool",
    "run_resolve_stock_tool",
    "run_search_posts_tool",
]
