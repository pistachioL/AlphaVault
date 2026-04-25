from __future__ import annotations

from typing import TypedDict

from alphavault.db.postgres_db import (
    PostgresEngine,
    ensure_postgres_engine,
    postgres_connect_autocommit,
    qualify_postgres_table,
    require_postgres_schema_name,
)
from alphavault.db.postgres_env import (
    infer_platform_from_post_uid,
    require_postgres_source_from_env,
)
from alphavault.db.source_queue import load_cloud_post
from alphavault.db.sql_rows import read_sql_rows
from alphavault.env import load_dotenv_if_present

_POSTS_TABLE_NAME = "posts"
DEFAULT_ALIAS_HISTORY_HIT_LIMIT = 5
ALIAS_HISTORY_DIALOGUE_CHARS = 500


class AliasHistoryHit(TypedDict):
    post_uid: str
    created_at: str
    author: str
    dialogue_text: str


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _clip_text(value: object, *, limit: int) -> str:
    text = _clean_text(value)
    if not text or limit <= 0 or len(text) <= limit:
        return text
    return text[:limit]


def _escape_ilike_pattern(text: str) -> str:
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _source_posts_table(engine_or_conn: object) -> str:
    return qualify_postgres_table(
        require_postgres_schema_name(engine_or_conn),
        _POSTS_TABLE_NAME,
    )


def _build_source_engine(sample_post_uid: str) -> PostgresEngine:
    platform = infer_platform_from_post_uid(sample_post_uid)
    if not platform:
        raise RuntimeError(f"unknown_source_platform:{sample_post_uid}")
    load_dotenv_if_present()
    source = require_postgres_source_from_env(platform)
    return ensure_postgres_engine(source.dsn, schema_name=source.schema)


def load_alias_history_hits(
    *,
    keyword_text: str,
    sample_post_uid: str,
    limit: int = DEFAULT_ALIAS_HISTORY_HIT_LIMIT,
) -> list[AliasHistoryHit]:
    keyword = _clean_text(keyword_text)
    post_uid = _clean_text(sample_post_uid)
    fetch_limit = max(1, int(limit))
    if not keyword or not post_uid:
        return []

    engine = _build_source_engine(post_uid)
    sample_post = load_cloud_post(engine, post_uid)
    author = _clean_text(sample_post.author)
    if not author:
        return []

    escaped_keyword = _escape_ilike_pattern(keyword)
    query = f"""
SELECT post_uid, created_at, author, raw_text
FROM {_source_posts_table(engine)}
WHERE processed_at IS NOT NULL
  AND TRIM(processed_at) <> ''
  AND author = :author
  AND post_uid <> :sample_post_uid
  AND raw_text ILIKE :keyword_pattern ESCAPE '\\'
ORDER BY created_at DESC, post_uid DESC
LIMIT :limit
""".strip()
    with postgres_connect_autocommit(engine) as conn:
        rows = read_sql_rows(
            conn,
            query,
            params={
                "author": author,
                "sample_post_uid": post_uid,
                "keyword_pattern": f"%{escaped_keyword}%",
                "limit": fetch_limit,
            },
        )
    return [
        AliasHistoryHit(
            post_uid=_clean_text(row.get("post_uid")),
            created_at=_clean_text(row.get("created_at")),
            author=_clean_text(row.get("author")),
            dialogue_text=_clip_text(
                row.get("raw_text"),
                limit=ALIAS_HISTORY_DIALOGUE_CHARS,
            ),
        )
        for row in rows
        if _clean_text(row.get("post_uid")) and _clean_text(row.get("raw_text"))
    ]


__all__ = [
    "ALIAS_HISTORY_DIALOGUE_CHARS",
    "DEFAULT_ALIAS_HISTORY_HIT_LIMIT",
    "AliasHistoryHit",
    "load_alias_history_hits",
]
