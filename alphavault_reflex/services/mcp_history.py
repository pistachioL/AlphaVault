from __future__ import annotations

import importlib
import json
from functools import cache
from types import ModuleType


DEFAULT_MCP_HISTORY_LIMIT = 100


@cache
def _load_history_repo_module() -> ModuleType:
    return importlib.import_module("alphavault.mcp_history.repo")


@cache
def _load_research_service_module() -> ModuleType:
    return importlib.import_module("alphavault.research_workbench.service")


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _coerce_int_text(value: object) -> str:
    raw = _clean_text(value)
    if not raw:
        return "0"
    try:
        return str(max(0, int(raw)))
    except (TypeError, ValueError):
        return "0"


def _compact_text(value: str, *, limit: int) -> str:
    text = _clean_text(value)
    if len(text) <= int(limit):
        return text
    return text[: max(0, int(limit) - 1)] + "…"


def _input_summary(tool_name: str, input_json: str) -> str:
    parsed: dict[str, object] = {}
    try:
        loaded = json.loads(_clean_text(input_json) or "{}")
        if isinstance(loaded, dict):
            parsed = loaded
    except Exception:
        return _compact_text(input_json, limit=140)

    for key in ("query", "stock", "post_uid"):
        value = _clean_text(parsed.get(key))
        if value:
            return value
    if tool_name == "get_stock_page":
        stock = _clean_text(parsed.get("stock"))
        author = _clean_text(parsed.get("author"))
        if stock and author:
            return f"{stock} / {author}"
    return _compact_text(input_json, limit=140)


def _pretty_json_text(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return "{}"
    try:
        loaded = json.loads(text)
    except Exception:
        return text
    return json.dumps(loaded, ensure_ascii=False, indent=2, sort_keys=True)


def _coerce_list_row(row: dict[str, object]) -> dict[str, str]:
    tool_name = _clean_text(row.get("tool_name"))
    input_json = _clean_text(row.get("input_json"))
    return {
        "call_id": _clean_text(row.get("call_id")),
        "trace_id": _clean_text(row.get("trace_id")),
        "tool_name": tool_name,
        "status": _clean_text(row.get("status")),
        "auth_mode": _clean_text(row.get("auth_mode")),
        "request_path": _clean_text(row.get("request_path")),
        "input_json": input_json,
        "input_summary": _input_summary(tool_name, input_json),
        "resolved_stock_key": _clean_text(row.get("resolved_stock_key")),
        "result_count": _coerce_int_text(row.get("result_count")),
        "error_text": _clean_text(row.get("error_text")),
        "duration_ms": _coerce_int_text(row.get("duration_ms")),
        "cf_ray": _clean_text(row.get("cf_ray")),
        "access_subject": _clean_text(row.get("access_subject")),
        "access_email": _clean_text(row.get("access_email")),
        "access_aud": _clean_text(row.get("access_aud")),
        "created_at": _clean_text(row.get("created_at")),
    }


def _coerce_post_row(row: dict[str, object]) -> dict[str, str]:
    title = _clean_text(row.get("title"))
    post_uid = _clean_text(row.get("post_uid"))
    return {
        "post_uid": post_uid,
        "source_kind": _clean_text(row.get("source_kind")),
        "title": title,
        "display_title": title or post_uid,
        "author": _clean_text(row.get("author")),
        "created_at": _clean_text(row.get("created_at")),
        "url": _clean_text(row.get("url")),
        "match_reason": _clean_text(row.get("match_reason")),
        "preview": _clean_text(row.get("preview")),
    }


def load_mcp_history_rows_from_env(
    *,
    limit: int = DEFAULT_MCP_HISTORY_LIMIT,
) -> tuple[list[dict[str, str]], str]:
    try:
        engine = (
            _load_research_service_module().get_research_workbench_engine_from_env()
        )
        rows = _load_history_repo_module().list_mcp_call_history(
            engine,
            limit=max(1, int(limit or 0)),
        )
    except Exception as err:
        return [], _clean_text(err)
    return [_coerce_list_row(dict(row)) for row in rows], ""


def load_mcp_history_detail_from_env(
    call_id: str,
) -> tuple[dict[str, str], list[dict[str, str]], str]:
    resolved_call_id = _clean_text(call_id)
    if not resolved_call_id:
        return {}, [], "missing_call_id"
    try:
        engine = (
            _load_research_service_module().get_research_workbench_engine_from_env()
        )
        detail = _load_history_repo_module().get_mcp_call_history_detail(
            engine,
            call_id=resolved_call_id,
        )
    except Exception as err:
        return {}, [], _clean_text(err)
    row = detail.get("row") or {}
    posts = detail.get("posts") or []
    if not row:
        return {}, [], "history_call_not_found"
    detail_row = _coerce_list_row(dict(row))
    detail_row["input_json_pretty"] = _pretty_json_text(detail_row["input_json"])
    return detail_row, [_coerce_post_row(dict(post)) for post in posts], ""


__all__ = [
    "DEFAULT_MCP_HISTORY_LIMIT",
    "load_mcp_history_detail_from_env",
    "load_mcp_history_rows_from_env",
]
