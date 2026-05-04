from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Iterable

from alphavault.domains.content.models import Post


def _coerce_text(value: object) -> str:
    return str(value or "").strip()


def _coerce_float(value: object) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    text = _coerce_text(value)
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _coerce_datetime_or_text(value: object) -> datetime | str:
    if isinstance(value, datetime):
        return value
    return _coerce_text(value)


def post_from_row(row: dict[str, object]) -> Post:
    return Post(
        post_uid=_coerce_text(row.get("post_uid")),
        platform=_coerce_text(row.get("platform")),
        platform_post_id=_coerce_text(row.get("platform_post_id")),
        author=_coerce_text(row.get("author")),
        created_at=_coerce_datetime_or_text(row.get("created_at")),
        url=_coerce_text(row.get("url")),
        raw_text=_coerce_text(row.get("raw_text")),
        status=_coerce_text(row.get("status")),
        invest_score=_coerce_float(row.get("invest_score")),
        processed_at=_coerce_datetime_or_text(row.get("processed_at")),
        source=_coerce_text(row.get("source")),
    )


def map_posts(rows: Iterable[dict[str, object]]) -> list[Post]:
    return [post_from_row(row) for row in rows]


def post_to_row(post: Post) -> dict[str, object]:
    return asdict(post)


def index_posts_by_uid(posts: Iterable[Post]) -> dict[str, Post]:
    return {post.post_uid: post for post in posts if str(post.post_uid or "").strip()}


__all__ = ["index_posts_by_uid", "map_posts", "post_from_row", "post_to_row"]
