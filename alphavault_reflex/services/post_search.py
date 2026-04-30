from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import TypedDict
from urllib.parse import quote

from alphavault.constants import (
    ENV_POSTGRES_DSN,
    PLATFORM_WEIBO,
    PLATFORM_XUEQIU,
    SCHEMA_WEIBO,
    SCHEMA_XUEQIU,
)
from alphavault.db.postgres_db import (
    ensure_postgres_engine,
    is_fatal_base_exception,
    postgres_connect_autocommit,
    qualify_postgres_table,
)
from alphavault.db.postgres_env import PostgresSource
from alphavault.db.postgres_env import load_configured_postgres_sources_from_env
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql_rows import read_sql_rows
from alphavault.domains.common.assertion_entities import coerce_stock_code_entity_key
from alphavault.search_text import (
    build_exact_search_variants,
    is_single_cjk_query,
    to_simplified_text,
    to_traditional_text,
)

POST_SEARCH_ROUTE = "/search/posts"
DEFAULT_POST_SEARCH_LIMIT = 20
ASSERTION_MENTION_WEIGHT = 12
ASSERTION_ENTITY_WEIGHT = 9
CONTEXT_ENTITY_WEIGHT = 7
BODY_HIT_WEIGHT = 3
MIN_QUERY_LIMIT = 1
EXTRA_FETCH_COUNT = 1
SOURCE_SCHEMA_NAMES = frozenset((SCHEMA_WEIBO, SCHEMA_XUEQIU))
ENTITY_KEY_PREFIXES = ("keyword", "industry", "commodity", "index")
PREVIEW_CHAR_LIMIT = 220
SEARCH_EXTENSION_ERROR_TEXT = "全文搜索依赖 PGroonga 扩展，当前数据库还没装好。"
EMPTY_QUERY_ERROR_TEXT = "请输入关键字。"
SINGLE_CJK_QUERY_ERROR_TEXT = "全文搜索至少输入 2 个汉字。"
MULTI_DSN_ERROR_TEXT = "全文搜索当前要求 source schema 共用同一个 Postgres。"
DEFAULT_DETAIL_TITLE = "帖子详情"
_WHITESPACE_RE = re.compile(r"\s+")


class PostSearchRow(TypedDict):
    post_uid: str
    source: str
    source_label: str
    author: str
    created_at: str
    url: str
    title: str
    preview: str
    raw_text: str
    match_reason: str


class PostSearchResult(TypedDict):
    rows: list[PostSearchRow]
    next_cursor: str
    has_more: bool
    error: str


@dataclass(frozen=True)
class SearchQuerySpec:
    exact_variants: tuple[str, ...]
    body_queries: tuple[str, ...]
    entity_keys: tuple[str, ...]


@dataclass(frozen=True)
class SearchCursor:
    total_score: int
    body_score: float
    created_at: str
    post_uid: str


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = _clean_text(value)
    if not text:
        return 0
    return int(text)


def _coerce_float(value: object) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    text = _clean_text(value)
    if not text:
        return 0.0
    return float(text)


def _dedupe_texts(values: list[str]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return tuple(out)


def validate_post_search_query(query: str) -> str:
    clean_query = _clean_text(query)
    if not clean_query:
        return EMPTY_QUERY_ERROR_TEXT
    if is_single_cjk_query(clean_query):
        return SINGLE_CJK_QUERY_ERROR_TEXT
    return ""


def build_post_search_route(query: str) -> str:
    clean_query = _clean_text(query)
    if not clean_query:
        return POST_SEARCH_ROUTE
    return f"{POST_SEARCH_ROUTE}?q={quote(clean_query)}"


def _platform_label(source_name: str) -> str:
    if source_name == PLATFORM_WEIBO:
        return "微博"
    if source_name == PLATFORM_XUEQIU:
        return "雪球"
    return source_name or "来源"


def _clean_limit(limit: int) -> int:
    return max(MIN_QUERY_LIMIT, int(limit))


def _build_query_spec(query: str) -> SearchQuerySpec:
    exact_variants = _dedupe_texts(build_exact_search_variants(query))
    body_queries = _dedupe_texts(
        [
            _clean_text(query),
            to_simplified_text(query),
            to_traditional_text(query),
        ]
    )
    entity_keys = _build_entity_keys(exact_variants)
    return SearchQuerySpec(
        exact_variants=exact_variants,
        body_queries=body_queries,
        entity_keys=entity_keys,
    )


def _build_entity_keys(exact_variants: tuple[str, ...]) -> tuple[str, ...]:
    out: list[str] = []
    for variant in exact_variants:
        stock_key = coerce_stock_code_entity_key(variant)
        if stock_key:
            out.append(stock_key)
        for prefix in ENTITY_KEY_PREFIXES:
            out.append(f"{prefix}:{variant}")
    return _dedupe_texts(out)


def _build_in_clause(
    *,
    column_expr: str,
    values: tuple[str, ...],
    prefix: str,
    params: dict[str, object],
) -> str:
    if not values:
        return ""
    placeholders = make_in_placeholders(prefix=prefix, count=len(values))
    params.update(make_in_params(prefix=prefix, values=values))
    return f"{column_expr} IN ({placeholders})"


def _empty_score_cte(*, name: str, score_column: str, score_literal: str) -> str:
    return f"""
{name} AS (
    SELECT
        '' AS post_uid,
        {score_literal} AS {score_column}
    WHERE FALSE
)
""".strip()


def _mention_hits_cte(
    *,
    assertion_mentions_table: str,
    assertions_table: str,
    exact_variants: tuple[str, ...],
    params: dict[str, object],
) -> str:
    norm_clause = _build_in_clause(
        column_expr="am.mention_norm",
        values=exact_variants,
        prefix="mention_norm",
        params=params,
    )
    text_clause = _build_in_clause(
        column_expr="LOWER(am.mention_text)",
        values=exact_variants,
        prefix="mention_text",
        params=params,
    )
    where_clauses: list[str] = []
    if norm_clause:
        where_clauses.append(norm_clause)
    if text_clause:
        where_clauses.append(
            f"(TRIM(COALESCE(am.mention_norm, '')) = '' AND {text_clause})"
        )
    if not where_clauses:
        return _empty_score_cte(
            name="mention_hits",
            score_column="mention_score",
            score_literal="0",
        )
    return f"""
mention_hits AS (
    SELECT
        a.post_uid,
        MAX({ASSERTION_MENTION_WEIGHT}) AS mention_score
    FROM {assertion_mentions_table} am
    JOIN {assertions_table} a
      ON a.assertion_id = am.assertion_id
    WHERE {" OR ".join(where_clauses)}
    GROUP BY a.post_uid
)
""".strip()


def _assertion_entity_hits_cte(
    *,
    assertion_entities_table: str,
    assertions_table: str,
    entity_keys: tuple[str, ...],
    params: dict[str, object],
) -> str:
    match_clause = _build_in_clause(
        column_expr="ae.entity_key",
        values=entity_keys,
        prefix="assertion_entity",
        params=params,
    )
    if not match_clause:
        return _empty_score_cte(
            name="assertion_entity_hits",
            score_column="entity_score",
            score_literal="0",
        )
    return f"""
assertion_entity_hits AS (
    SELECT
        a.post_uid,
        MAX({ASSERTION_ENTITY_WEIGHT}) AS entity_score
    FROM {assertion_entities_table} ae
    JOIN {assertions_table} a
      ON a.assertion_id = ae.assertion_id
    WHERE {match_clause}
    GROUP BY a.post_uid
)
""".strip()


def _context_entity_hits_cte(
    *,
    post_context_entities_table: str,
    entity_keys: tuple[str, ...],
    params: dict[str, object],
) -> str:
    match_clause = _build_in_clause(
        column_expr="pce.entity_key",
        values=entity_keys,
        prefix="context_entity",
        params=params,
    )
    if not match_clause:
        return _empty_score_cte(
            name="context_entity_hits",
            score_column="entity_score",
            score_literal="0",
        )
    return f"""
context_entity_hits AS (
    SELECT
        pce.post_uid,
        MAX({CONTEXT_ENTITY_WEIGHT}) AS entity_score
    FROM {post_context_entities_table} pce
    WHERE {match_clause}
    GROUP BY pce.post_uid
)
""".strip()


def _body_hits_cte(
    *,
    posts_table: str,
    body_queries: tuple[str, ...],
    params: dict[str, object],
) -> str:
    if not body_queries:
        return _empty_score_cte(
            name="body_hits",
            score_column="body_score",
            score_literal="0.0",
        )
    search_expr = "COALESCE(NULLIF(p.raw_text_search_norm, ''), p.raw_text)"
    body_clauses: list[str] = []
    for idx, query in enumerate(body_queries):
        key = f"body_query_{idx}"
        params[key] = query
        body_clauses.append(f"{search_expr} &@~ :{key}")
    return f"""
body_hits AS (
    SELECT
        p.post_uid,
        MAX(pgroonga_score(p.tableoid, p.ctid)) AS body_score
    FROM {posts_table} p
    WHERE p.processed_at IS NOT NULL
      AND ({" OR ".join(body_clauses)})
    GROUP BY p.post_uid
)
""".strip()


def _build_source_branch_sql(
    source: PostgresSource,
    *,
    spec: SearchQuerySpec,
    params: dict[str, object],
) -> str:
    posts_table = qualify_postgres_table(source.schema, "posts")
    assertions_table = qualify_postgres_table(source.schema, "assertions")
    assertion_mentions_table = qualify_postgres_table(
        source.schema,
        "assertion_mentions",
    )
    assertion_entities_table = qualify_postgres_table(
        source.schema,
        "assertion_entities",
    )
    post_context_entities_table = qualify_postgres_table(
        source.schema,
        "post_context_entities",
    )
    ctes = (
        _mention_hits_cte(
            assertion_mentions_table=assertion_mentions_table,
            assertions_table=assertions_table,
            exact_variants=spec.exact_variants,
            params=params,
        ),
        _assertion_entity_hits_cte(
            assertion_entities_table=assertion_entities_table,
            assertions_table=assertions_table,
            entity_keys=spec.entity_keys,
            params=params,
        ),
        _context_entity_hits_cte(
            post_context_entities_table=post_context_entities_table,
            entity_keys=spec.entity_keys,
            params=params,
        ),
        _body_hits_cte(
            posts_table=posts_table,
            body_queries=spec.body_queries,
            params=params,
        ),
    )
    joined_ctes = ",\n    ".join(ctes)
    return f"""
SELECT * FROM (
    WITH
    {joined_ctes},
    matched_posts AS (
        SELECT
            p.post_uid,
            p.platform,
            p.author,
            p.created_at,
            p.url,
            p.raw_text,
            COALESCE(mh.mention_score, 0) AS mention_score,
            COALESCE(aeh.entity_score, 0) AS assertion_entity_score,
            COALESCE(ceh.entity_score, 0) AS context_entity_score,
            COALESCE(bh.body_score, 0.0) AS body_score
        FROM {posts_table} p
        LEFT JOIN mention_hits mh
          ON mh.post_uid = p.post_uid
        LEFT JOIN assertion_entity_hits aeh
          ON aeh.post_uid = p.post_uid
        LEFT JOIN context_entity_hits ceh
          ON ceh.post_uid = p.post_uid
        LEFT JOIN body_hits bh
          ON bh.post_uid = p.post_uid
        WHERE p.processed_at IS NOT NULL
          AND (
            mh.post_uid IS NOT NULL
            OR aeh.post_uid IS NOT NULL
            OR ceh.post_uid IS NOT NULL
            OR bh.post_uid IS NOT NULL
          )
    )
    SELECT
        post_uid,
        platform,
        author,
        created_at,
        url,
        raw_text,
        mention_score,
        assertion_entity_score,
        context_entity_score,
        CASE WHEN body_score > 0 THEN 1 ELSE 0 END AS body_hit,
        body_score,
        (
            mention_score
            + assertion_entity_score
            + context_entity_score
            + CASE WHEN body_score > 0 THEN {BODY_HIT_WEIGHT} ELSE 0 END
        ) AS total_score
    FROM matched_posts
) AS source_branch
""".strip()


def _encode_cursor(row: dict[str, object]) -> str:
    payload = {
        "total_score": _coerce_int(row.get("total_score")),
        "body_score": _coerce_float(row.get("body_score")),
        "created_at": _clean_text(row.get("created_at")),
        "post_uid": _clean_text(row.get("post_uid")),
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _decode_cursor(cursor: str) -> SearchCursor:
    text = _clean_text(cursor)
    if not text:
        return SearchCursor(
            total_score=0,
            body_score=0.0,
            created_at="",
            post_uid="",
        )
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("invalid_search_cursor") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("invalid_search_cursor")
    return SearchCursor(
        total_score=int(payload.get("total_score") or 0),
        body_score=float(payload.get("body_score") or 0.0),
        created_at=_clean_text(payload.get("created_at")),
        post_uid=_clean_text(payload.get("post_uid")),
    )


def _cursor_where_clause(
    *,
    cursor: str,
    params: dict[str, object],
) -> str:
    cursor_key = _decode_cursor(cursor)
    if not cursor_key.post_uid:
        return ""
    params.update(
        {
            "cursor_total_score": int(cursor_key.total_score),
            "cursor_body_score": float(cursor_key.body_score),
            "cursor_created_at": cursor_key.created_at,
            "cursor_post_uid": cursor_key.post_uid,
        }
    )
    return """
WHERE (
    total_score < :cursor_total_score
    OR (
        total_score = :cursor_total_score
        AND body_score < :cursor_body_score
    )
    OR (
        total_score = :cursor_total_score
        AND body_score = :cursor_body_score
        AND created_at < :cursor_created_at
    )
    OR (
        total_score = :cursor_total_score
        AND body_score = :cursor_body_score
        AND created_at = :cursor_created_at
        AND post_uid < :cursor_post_uid
    )
)
""".strip()


def _load_search_sources_from_env() -> list[PostgresSource]:
    sources = [
        source
        for source in load_configured_postgres_sources_from_env()
        if source.schema in SOURCE_SCHEMA_NAMES
    ]
    if not sources:
        raise RuntimeError(f"缺少 {ENV_POSTGRES_DSN}")
    dsn_values = {str(source.url or "").strip() for source in sources}
    if len(dsn_values) != 1:
        raise RuntimeError(MULTI_DSN_ERROR_TEXT)
    return sources


def _build_search_sql(
    *,
    sources: list[PostgresSource],
    spec: SearchQuerySpec,
    limit: int,
    cursor: str,
) -> tuple[str, dict[str, object]]:
    params: dict[str, object] = {"fetch_limit": _clean_limit(limit) + EXTRA_FETCH_COUNT}
    branches = [
        _build_source_branch_sql(source, spec=spec, params=params) for source in sources
    ]
    cursor_clause = _cursor_where_clause(cursor=cursor, params=params)
    union_sql = "\n    UNION ALL\n    ".join(branches)
    sql = f"""
WITH merged_posts AS (
    {union_sql}
)
SELECT
    post_uid,
    platform,
    author,
    created_at,
    url,
    raw_text,
    mention_score,
    assertion_entity_score,
    context_entity_score,
    body_hit,
    body_score,
    total_score
FROM merged_posts
{cursor_clause}
ORDER BY total_score DESC, body_score DESC, created_at DESC, post_uid DESC
LIMIT :fetch_limit
"""
    return sql, params


def _load_search_rows(
    *,
    spec: SearchQuerySpec,
    limit: int,
    cursor: str,
) -> list[dict[str, object]]:
    sources = _load_search_sources_from_env()
    sql, params = _build_search_sql(
        sources=sources,
        spec=spec,
        limit=limit,
        cursor=cursor,
    )
    engine = ensure_postgres_engine(sources[0].url)
    with postgres_connect_autocommit(engine) as conn:
        return read_sql_rows(conn, sql, params=params)


def _search_error_message(exc: BaseException) -> str:
    text = _clean_text(exc)
    lowered = text.lower()
    if "pgroonga" in lowered or "&@~" in lowered:
        return SEARCH_EXTENSION_ERROR_TEXT
    if not text:
        return type(exc).__name__
    return text


def _normalize_preview(raw_text: str) -> str:
    compact_text = _WHITESPACE_RE.sub(" ", raw_text).strip()
    if len(compact_text) <= PREVIEW_CHAR_LIMIT:
        return compact_text
    return compact_text[:PREVIEW_CHAR_LIMIT].rstrip() + "…"


def _detail_title(source_name: str, *, author: str, created_at: str) -> str:
    parts = [_platform_label(source_name)]
    if author:
        parts.append(author)
    if created_at:
        parts.append(created_at)
    title = " · ".join(parts)
    return title or DEFAULT_DETAIL_TITLE


def _match_reason(row: dict[str, object]) -> str:
    reasons: list[str] = []
    if _coerce_int(row.get("mention_score")) > 0:
        reasons.append("提及词")
    if _coerce_int(row.get("assertion_entity_score")) > 0:
        reasons.append("观点实体")
    if _coerce_int(row.get("context_entity_score")) > 0:
        reasons.append("对话实体")
    if _coerce_int(row.get("body_hit")) > 0:
        reasons.append("正文")
    if not reasons:
        return "正文命中"
    return "、".join(reasons) + "命中"


def _format_row(row: dict[str, object]) -> PostSearchRow:
    post_uid = _clean_text(row.get("post_uid"))
    source_name = _clean_text(row.get("platform"))
    author = _clean_text(row.get("author"))
    created_at = _clean_text(row.get("created_at"))
    raw_text = _clean_text(row.get("raw_text"))
    return {
        "post_uid": post_uid,
        "source": source_name,
        "source_label": _platform_label(source_name),
        "author": author,
        "created_at": created_at,
        "url": _clean_text(row.get("url")),
        "title": _detail_title(
            source_name,
            author=author,
            created_at=created_at,
        ),
        "preview": _normalize_preview(raw_text),
        "raw_text": raw_text,
        "match_reason": _match_reason(row),
    }


def _slice_page_rows(
    rows: list[dict[str, object]],
    *,
    limit: int,
) -> tuple[list[dict[str, object]], str, bool]:
    clean_limit = _clean_limit(limit)
    if len(rows) <= clean_limit:
        return rows, "", False
    page_rows = rows[:clean_limit]
    return page_rows, _encode_cursor(page_rows[-1]), True


def search_posts_from_env(
    query: str,
    *,
    limit: int = DEFAULT_POST_SEARCH_LIMIT,
    cursor: str = "",
) -> PostSearchResult:
    query_error = validate_post_search_query(query)
    if query_error:
        return {
            "rows": [],
            "next_cursor": "",
            "has_more": False,
            "error": query_error,
        }
    spec = _build_query_spec(query)
    try:
        rows = _load_search_rows(spec=spec, limit=limit, cursor=cursor)
    except BaseException as exc:
        if is_fatal_base_exception(exc):
            raise
        return {
            "rows": [],
            "next_cursor": "",
            "has_more": False,
            "error": _search_error_message(exc),
        }
    page_rows, next_cursor, has_more = _slice_page_rows(rows, limit=limit)
    return {
        "rows": [_format_row(row) for row in page_rows],
        "next_cursor": next_cursor,
        "has_more": has_more,
        "error": "",
    }


__all__ = [
    "DEFAULT_POST_SEARCH_LIMIT",
    "PostSearchResult",
    "PostSearchRow",
    "POST_SEARCH_ROUTE",
    "build_post_search_route",
    "search_posts_from_env",
    "validate_post_search_query",
]
