from __future__ import annotations

from typing import TypedDict

from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    run_postgres_transaction,
)
from alphavault.db.sql.mcp_history import (
    insert_mcp_call_history as insert_mcp_call_history_sql,
    insert_mcp_call_history_post_rows,
    select_mcp_call_history_by_call_id,
    select_mcp_call_history_posts_by_call_id,
    select_mcp_call_history_rows,
)
from alphavault.timeutil import now_cst_str

from .schema import MCP_CALL_HISTORY_POSTS_TABLE, MCP_CALL_HISTORY_TABLE


class McpCallHistoryPostRow(TypedDict):
    post_uid: str
    source_kind: str
    title: str
    author: str
    created_at: str
    url: str
    match_reason: str
    preview: str


class McpCallHistoryRow(TypedDict):
    call_id: str
    trace_id: str
    tool_name: str
    status: str
    auth_mode: str
    request_path: str
    input_json: str
    resolved_stock_key: str
    result_count: int
    error_text: str
    duration_ms: int
    cf_ray: str
    access_subject: str
    access_email: str
    access_aud: str
    created_at: str


class McpCallHistoryDetail(TypedDict):
    row: McpCallHistoryRow | None
    posts: list[McpCallHistoryPostRow]


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _coerce_int(value: object) -> int:
    raw = _clean_text(value)
    if not raw:
        return 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _coerce_row(row: dict[str, object]) -> McpCallHistoryRow:
    return {
        "call_id": _clean_text(row.get("call_id")),
        "trace_id": _clean_text(row.get("trace_id")),
        "tool_name": _clean_text(row.get("tool_name")),
        "status": _clean_text(row.get("status")),
        "auth_mode": _clean_text(row.get("auth_mode")),
        "request_path": _clean_text(row.get("request_path")),
        "input_json": _clean_text(row.get("input_json")),
        "resolved_stock_key": _clean_text(row.get("resolved_stock_key")),
        "result_count": _coerce_int(row.get("result_count")),
        "error_text": _clean_text(row.get("error_text")),
        "duration_ms": _coerce_int(row.get("duration_ms")),
        "cf_ray": _clean_text(row.get("cf_ray")),
        "access_subject": _clean_text(row.get("access_subject")),
        "access_email": _clean_text(row.get("access_email")),
        "access_aud": _clean_text(row.get("access_aud")),
        "created_at": _clean_text(row.get("created_at")),
    }


def _coerce_post_row(row: dict[str, object]) -> McpCallHistoryPostRow:
    return {
        "post_uid": _clean_text(row.get("post_uid")),
        "source_kind": _clean_text(row.get("source_kind")),
        "title": _clean_text(row.get("title")),
        "author": _clean_text(row.get("author")),
        "created_at": _clean_text(row.get("created_at")),
        "url": _clean_text(row.get("url")),
        "match_reason": _clean_text(row.get("match_reason")),
        "preview": _clean_text(row.get("preview")),
    }


def record_mcp_call_history(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    call_id: str,
    trace_id: str,
    tool_name: str,
    status: str,
    auth_mode: str,
    request_path: str,
    input_json: str,
    resolved_stock_key: str,
    result_count: int,
    error_text: str,
    duration_ms: int,
    cf_ray: str,
    access_subject: str,
    access_email: str,
    access_aud: str,
    posts: list[McpCallHistoryPostRow],
) -> None:
    payload = {
        "call_id": _clean_text(call_id),
        "trace_id": _clean_text(trace_id),
        "tool_name": _clean_text(tool_name),
        "status": _clean_text(status),
        "auth_mode": _clean_text(auth_mode),
        "request_path": _clean_text(request_path),
        "input_json": _clean_text(input_json) or "{}",
        "resolved_stock_key": _clean_text(resolved_stock_key),
        "result_count": max(0, int(result_count or 0)),
        "error_text": _clean_text(error_text),
        "duration_ms": max(0, int(duration_ms or 0)),
        "cf_ray": _clean_text(cf_ray),
        "access_subject": _clean_text(access_subject),
        "access_email": _clean_text(access_email),
        "access_aud": _clean_text(access_aud),
        "created_at": now_cst_str(),
    }
    normalized_posts = [
        {
            "call_id": payload["call_id"],
            "post_uid": _clean_text(row.get("post_uid")),
            "source_kind": _clean_text(row.get("source_kind")),
            "title": _clean_text(row.get("title")),
            "author": _clean_text(row.get("author")),
            "created_at": _clean_text(row.get("created_at")),
            "url": _clean_text(row.get("url")),
            "match_reason": _clean_text(row.get("match_reason")),
            "preview": _clean_text(row.get("preview")),
        }
        for row in posts
        if _clean_text(row.get("post_uid")) and _clean_text(row.get("source_kind"))
    ]

    def _write(conn: PostgresConnection) -> None:
        conn.execute(
            insert_mcp_call_history_sql(MCP_CALL_HISTORY_TABLE),
            payload,
        )
        if normalized_posts:
            conn.execute(
                insert_mcp_call_history_post_rows(MCP_CALL_HISTORY_POSTS_TABLE),
                normalized_posts,
            )

    run_postgres_transaction(engine_or_conn, _write)


def list_mcp_call_history(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    limit: int = 100,
) -> list[McpCallHistoryRow]:
    rows = run_postgres_transaction(
        engine_or_conn,
        lambda conn: conn.execute(
            select_mcp_call_history_rows(MCP_CALL_HISTORY_TABLE),
            {"limit": max(1, int(limit or 0))},
        )
        .mappings()
        .all(),
    )
    return [_coerce_row(dict(row)) for row in rows]


def get_mcp_call_history_detail(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    call_id: str,
) -> McpCallHistoryDetail:
    resolved_call_id = _clean_text(call_id)

    def _read(conn: PostgresConnection) -> McpCallHistoryDetail:
        row = (
            conn.execute(
                select_mcp_call_history_by_call_id(MCP_CALL_HISTORY_TABLE),
                {"call_id": resolved_call_id},
            )
            .mappings()
            .first()
        )
        post_rows = (
            conn.execute(
                select_mcp_call_history_posts_by_call_id(MCP_CALL_HISTORY_POSTS_TABLE),
                {"call_id": resolved_call_id},
            )
            .mappings()
            .all()
        )
        return {
            "row": _coerce_row(dict(row)) if row else None,
            "posts": [_coerce_post_row(dict(post)) for post in post_rows],
        }

    return run_postgres_transaction(engine_or_conn, _read)


__all__ = [
    "McpCallHistoryDetail",
    "McpCallHistoryPostRow",
    "McpCallHistoryRow",
    "get_mcp_call_history_detail",
    "list_mcp_call_history",
    "record_mcp_call_history",
]
