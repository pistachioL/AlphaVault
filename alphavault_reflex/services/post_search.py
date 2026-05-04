from __future__ import annotations

from alphavault.capabilities.post_search import (
    DEFAULT_POST_SEARCH_LIMIT,
    PostSearchResult,
    PostSearchRow,
    search_posts,
    validate_post_search_query,
)
from urllib.parse import quote

POST_SEARCH_ROUTE = "/search/posts"


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def build_post_search_route(query: str) -> str:
    clean_query = _clean_text(query)
    if not clean_query:
        return POST_SEARCH_ROUTE
    return f"{POST_SEARCH_ROUTE}?q={quote(clean_query)}"


def search_posts_from_env(
    query: str,
    *,
    limit: int = DEFAULT_POST_SEARCH_LIMIT,
    cursor: str = "",
) -> PostSearchResult:
    return search_posts(query, limit=limit, cursor=cursor)


__all__ = [
    "DEFAULT_POST_SEARCH_LIMIT",
    "PostSearchResult",
    "PostSearchRow",
    "POST_SEARCH_ROUTE",
    "build_post_search_route",
    "search_posts_from_env",
    "validate_post_search_query",
]
