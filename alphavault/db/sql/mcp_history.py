from __future__ import annotations


def insert_mcp_call_history(table: str) -> str:
    return f"""
INSERT INTO {table}(
    call_id,
    trace_id,
    tool_name,
    status,
    auth_mode,
    request_path,
    input_json,
    resolved_stock_key,
    result_count,
    error_text,
    duration_ms,
    cf_ray,
    access_subject,
    access_email,
    access_aud,
    created_at
)
VALUES (
    :call_id,
    :trace_id,
    :tool_name,
    :status,
    :auth_mode,
    :request_path,
    :input_json,
    :resolved_stock_key,
    :result_count,
    :error_text,
    :duration_ms,
    :cf_ray,
    :access_subject,
    :access_email,
    :access_aud,
    :created_at
)
"""


def insert_mcp_call_history_post_rows(table: str) -> str:
    return f"""
INSERT INTO {table}(
    call_id,
    post_uid,
    source_kind,
    title,
    author,
    created_at,
    url,
    match_reason,
    preview
)
VALUES (
    :call_id,
    :post_uid,
    :source_kind,
    :title,
    :author,
    :created_at,
    :url,
    :match_reason,
    :preview
)
ON CONFLICT(call_id, post_uid, source_kind) DO UPDATE SET
    title = excluded.title,
    author = excluded.author,
    created_at = excluded.created_at,
    url = excluded.url,
    match_reason = excluded.match_reason,
    preview = excluded.preview
"""


def select_mcp_call_history_rows(table: str) -> str:
    return f"""
SELECT call_id,
       trace_id,
       tool_name,
       status,
       auth_mode,
       request_path,
       input_json,
       resolved_stock_key,
       result_count,
       error_text,
       duration_ms,
       cf_ray,
       access_subject,
       access_email,
       access_aud,
       created_at
FROM {table}
ORDER BY created_at DESC, call_id DESC
LIMIT :limit
"""


def select_mcp_call_history_by_call_id(table: str) -> str:
    return f"""
SELECT call_id,
       trace_id,
       tool_name,
       status,
       auth_mode,
       request_path,
       input_json,
       resolved_stock_key,
       result_count,
       error_text,
       duration_ms,
       cf_ray,
       access_subject,
       access_email,
       access_aud,
       created_at
FROM {table}
WHERE call_id = :call_id
LIMIT 1
"""


def select_mcp_call_history_posts_by_call_id(table: str) -> str:
    return f"""
SELECT call_id,
       post_uid,
       source_kind,
       title,
       author,
       created_at,
       url,
       match_reason,
       preview
FROM {table}
WHERE call_id = :call_id
ORDER BY created_at DESC, post_uid ASC, source_kind ASC
"""


__all__ = [
    "insert_mcp_call_history",
    "insert_mcp_call_history_post_rows",
    "select_mcp_call_history_by_call_id",
    "select_mcp_call_history_posts_by_call_id",
    "select_mcp_call_history_rows",
]
