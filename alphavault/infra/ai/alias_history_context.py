from __future__ import annotations

from collections.abc import Iterable
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
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql_rows import read_sql_rows
from alphavault.env import load_dotenv_if_present

_POSTS_TABLE_NAME = "posts"
DEFAULT_ALIAS_HISTORY_HIT_LIMIT = 5
_ALIAS_HISTORY_BATCH_QUERY_SIZE = 50


class AliasHistoryHit(TypedDict):
    post_uid: str
    created_at: str
    author: str
    dialogue_text: str


class AliasHistoryLookupRequest(TypedDict):
    sample_post_uid: str
    keyword_text: str


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _escape_ilike_pattern(text: str) -> str:
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _source_posts_table(engine_or_conn: object) -> str:
    return qualify_postgres_table(
        require_postgres_schema_name(engine_or_conn),
        _POSTS_TABLE_NAME,
    )


def _platform_from_post_uid(sample_post_uid: str) -> str:
    return _clean_text(infer_platform_from_post_uid(sample_post_uid))


def _build_source_engine_for_platform(platform: str) -> PostgresEngine:
    platform_name = _clean_text(platform)
    if not platform_name:
        raise RuntimeError("unknown_source_platform")
    load_dotenv_if_present()
    source = require_postgres_source_from_env(platform_name)
    return ensure_postgres_engine(source.dsn, schema_name=source.schema)


def _normalize_lookup_requests(
    requests: Iterable[AliasHistoryLookupRequest],
) -> list[AliasHistoryLookupRequest]:
    out: list[AliasHistoryLookupRequest] = []
    seen: set[tuple[str, str]] = set()
    for item in requests:
        sample_post_uid = _clean_text(item.get("sample_post_uid"))
        keyword_text = _clean_text(item.get("keyword_text"))
        if not sample_post_uid or not keyword_text:
            continue
        request_key = (sample_post_uid, keyword_text)
        if request_key in seen:
            continue
        seen.add(request_key)
        out.append(
            AliasHistoryLookupRequest(
                sample_post_uid=sample_post_uid,
                keyword_text=keyword_text,
            )
        )
    return out


def _iter_request_chunks(
    requests: list[AliasHistoryLookupRequest],
    *,
    chunk_size: int,
) -> Iterable[list[AliasHistoryLookupRequest]]:
    size = max(1, int(chunk_size))
    for start in range(0, len(requests), size):
        yield requests[start : start + size]


def _load_source_post_authors(
    engine: PostgresEngine,
    post_uids: list[str],
) -> dict[str, str]:
    cleaned = [
        uid for uid in dict.fromkeys(_clean_text(uid) for uid in post_uids) if uid
    ]
    if not cleaned:
        return {}
    placeholders = make_in_placeholders(prefix="uid", count=len(cleaned))
    params = make_in_params(prefix="uid", values=cleaned)
    query = f"""
SELECT post_uid, author
FROM {_source_posts_table(engine)}
WHERE post_uid IN ({placeholders})
""".strip()
    with postgres_connect_autocommit(engine) as conn:
        rows = read_sql_rows(conn, query, params=params)
    out: dict[str, str] = {}
    for row in rows:
        post_uid = _clean_text(row.get("post_uid"))
        author = _clean_text(row.get("author"))
        if post_uid and author:
            out[post_uid] = author
    return out


def _build_history_request_values(
    requests: list[AliasHistoryLookupRequest],
) -> tuple[str, dict[str, object]]:
    params: dict[str, object] = {}
    value_rows: list[str] = []
    for idx, item in enumerate(requests):
        sample_post_uid_name = f"sample_post_uid_{idx}"
        keyword_text_name = f"keyword_text_{idx}"
        escaped_keyword_name = f"escaped_keyword_{idx}"
        value_rows.append(
            f"(:{sample_post_uid_name}, :{keyword_text_name}, :{escaped_keyword_name})"
        )
        sample_post_uid = _clean_text(item.get("sample_post_uid"))
        keyword_text = _clean_text(item.get("keyword_text"))
        params[sample_post_uid_name] = sample_post_uid
        params[keyword_text_name] = keyword_text
        params[escaped_keyword_name] = _escape_ilike_pattern(keyword_text)
    return ", ".join(value_rows), params


def _load_alias_history_hits_for_author_requests(
    *,
    engine: PostgresEngine,
    author: str,
    requests: list[AliasHistoryLookupRequest],
    limit: int,
) -> dict[tuple[str, str], list[AliasHistoryHit]]:
    if not requests:
        return {}
    values_sql, params = _build_history_request_values(requests)
    params["author"] = _clean_text(author)
    params["limit"] = max(1, int(limit))
    query = f"""
WITH wanted(sample_post_uid, keyword_text, escaped_keyword) AS (
  VALUES {values_sql}
),
ranked AS (
  SELECT
    wanted.sample_post_uid,
    wanted.keyword_text,
    p.post_uid,
    p.created_at,
    p.author,
    p.raw_text,
    ROW_NUMBER() OVER (
      PARTITION BY wanted.sample_post_uid, wanted.keyword_text
      ORDER BY p.created_at DESC, p.post_uid DESC
    ) AS row_num
  FROM wanted
  JOIN {_source_posts_table(engine)} AS p
    ON p.author = :author
  WHERE p.processed_at IS NOT NULL
    AND TRIM(p.processed_at) <> ''
    AND p.post_uid <> wanted.sample_post_uid
    AND p.raw_text ILIKE ('%' || wanted.escaped_keyword || '%') ESCAPE '\\'
)
SELECT sample_post_uid, keyword_text, post_uid, created_at, author, raw_text
FROM ranked
WHERE row_num <= :limit
ORDER BY sample_post_uid, keyword_text, created_at DESC, post_uid DESC
""".strip()
    with postgres_connect_autocommit(engine) as conn:
        rows = read_sql_rows(conn, query, params=params)
    out: dict[tuple[str, str], list[AliasHistoryHit]] = {}
    for row in rows:
        sample_post_uid = _clean_text(row.get("sample_post_uid"))
        keyword_text = _clean_text(row.get("keyword_text"))
        post_uid = _clean_text(row.get("post_uid"))
        dialogue_text = _clean_text(row.get("raw_text"))
        if not sample_post_uid or not keyword_text or not post_uid or not dialogue_text:
            continue
        request_key = (sample_post_uid, keyword_text)
        out.setdefault(request_key, []).append(
            AliasHistoryHit(
                post_uid=post_uid,
                created_at=_clean_text(row.get("created_at")),
                author=_clean_text(row.get("author")),
                dialogue_text=dialogue_text,
            )
        )
    return out


def load_alias_history_hits_batch(
    *,
    requests: Iterable[AliasHistoryLookupRequest],
    limit: int = DEFAULT_ALIAS_HISTORY_HIT_LIMIT,
) -> dict[tuple[str, str], list[AliasHistoryHit]]:
    normalized_requests = _normalize_lookup_requests(requests)
    if not normalized_requests:
        return {}
    fetch_limit = max(1, int(limit))
    requests_by_platform: dict[str, list[AliasHistoryLookupRequest]] = {}
    for item in normalized_requests:
        sample_post_uid = _clean_text(item.get("sample_post_uid"))
        platform = _platform_from_post_uid(sample_post_uid)
        if not platform:
            continue
        requests_by_platform.setdefault(platform, []).append(item)

    out: dict[tuple[str, str], list[AliasHistoryHit]] = {}
    for platform, platform_requests in requests_by_platform.items():
        engine = _build_source_engine_for_platform(platform)
        authors_by_post_uid = _load_source_post_authors(
            engine,
            [_clean_text(item.get("sample_post_uid")) for item in platform_requests],
        )
        requests_by_author: dict[str, list[AliasHistoryLookupRequest]] = {}
        for item in platform_requests:
            sample_post_uid = _clean_text(item.get("sample_post_uid"))
            author = _clean_text(authors_by_post_uid.get(sample_post_uid))
            if not author:
                continue
            requests_by_author.setdefault(author, []).append(item)
        for author, author_requests in requests_by_author.items():
            for chunk in _iter_request_chunks(
                author_requests,
                chunk_size=_ALIAS_HISTORY_BATCH_QUERY_SIZE,
            ):
                out.update(
                    _load_alias_history_hits_for_author_requests(
                        engine=engine,
                        author=author,
                        requests=chunk,
                        limit=fetch_limit,
                    )
                )
    return out


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
    hits_by_request = load_alias_history_hits_batch(
        requests=[
            AliasHistoryLookupRequest(
                sample_post_uid=post_uid,
                keyword_text=keyword,
            )
        ],
        limit=fetch_limit,
    )
    return list(hits_by_request.get((post_uid, keyword), []))


__all__ = [
    "DEFAULT_ALIAS_HISTORY_HIT_LIMIT",
    "AliasHistoryHit",
    "AliasHistoryLookupRequest",
    "load_alias_history_hits",
    "load_alias_history_hits_batch",
]
