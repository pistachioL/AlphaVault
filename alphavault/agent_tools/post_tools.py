from __future__ import annotations

from typing_extensions import TypedDict

from alphavault.capabilities.post_detail import PostDetailResult, get_post_detail
from alphavault.capabilities.post_search import (
    DEFAULT_POST_SEARCH_LIMIT,
    search_posts,
)

DEFAULT_AGENT_POST_SEARCH_LIMIT = 10


class AgentPostSearchRow(TypedDict):
    post_uid: str
    source: str
    source_label: str
    author: str
    created_at: str
    url: str
    title: str
    preview: str
    match_reason: str


class AgentPostSearchResult(TypedDict):
    rows: list[AgentPostSearchRow]
    next_cursor: str
    has_more: bool
    error: str


class AgentPostDetailResult(TypedDict):
    post_uid: str
    raw_text: str
    tree_text: str
    message: str
    load_error: str


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _trim_post_search_rows(rows: object) -> list[AgentPostSearchRow]:
    if not isinstance(rows, list):
        return []
    out: list[AgentPostSearchRow] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "post_uid": _clean_text(row.get("post_uid")),
                "source": _clean_text(row.get("source")),
                "source_label": _clean_text(row.get("source_label")),
                "author": _clean_text(row.get("author")),
                "created_at": _clean_text(row.get("created_at")),
                "url": _clean_text(row.get("url")),
                "title": _clean_text(row.get("title")),
                "preview": _clean_text(row.get("preview")),
                "match_reason": _clean_text(row.get("match_reason")),
            }
        )
    return out


def ai_search_posts(
    query: str,
    *,
    limit: int = DEFAULT_AGENT_POST_SEARCH_LIMIT,
    cursor: str = "",
) -> AgentPostSearchResult:
    result = search_posts(query, limit=limit, cursor=cursor)
    return {
        "rows": _trim_post_search_rows(result.get("rows")),
        "next_cursor": _clean_text(result.get("next_cursor")),
        "has_more": bool(result.get("has_more")),
        "error": _clean_text(result.get("error")),
    }


def ai_get_post_detail(post_uid: str) -> AgentPostDetailResult:
    detail: PostDetailResult = get_post_detail(post_uid)
    return {
        "post_uid": _clean_text(detail.get("post_uid")),
        "raw_text": _clean_text(detail.get("raw_text")),
        "tree_text": _clean_text(detail.get("tree_text")),
        "message": _clean_text(detail.get("message")),
        "load_error": _clean_text(detail.get("load_error")),
    }


__all__ = [
    "AgentPostDetailResult",
    "AgentPostSearchResult",
    "AgentPostSearchRow",
    "DEFAULT_AGENT_POST_SEARCH_LIMIT",
    "DEFAULT_POST_SEARCH_LIMIT",
    "ai_get_post_detail",
    "ai_search_posts",
]
