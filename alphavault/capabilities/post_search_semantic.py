from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json

from alphavault.ai.embedding import embed_texts_with_openai
from alphavault.ai.reranker import rerank_texts
from alphavault.capabilities.post_search import (
    DEFAULT_POST_SEARCH_LIMIT,
    PostSearchResult,
    PostSearchRow,
    _clean_text,
    _detail_title,
    _load_search_sources_from_env,
    _normalize_preview,
    _platform_label,
    validate_post_search_query,
)
from alphavault.constants import (
    DEFAULT_EMBEDDING_TIMEOUT_SECONDS,
    DEFAULT_RERANKER_TIMEOUT_SECONDS,
    DEFAULT_SEMANTIC_DOC_EMBEDDING_DIMENSIONS,
)
from alphavault.db.postgres_db import (
    ensure_postgres_engine,
    is_fatal_base_exception,
    postgres_connect_autocommit,
    qualify_postgres_table,
)
from alphavault.db.postgres_env import PostgresSource
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql_rows import read_sql_rows
from alphavault.db.zilliz_client import (
    semantic_search_in_zilliz,
    should_write_to_postgres,
)
from alphavault.infra.ai.embedding_runtime_config import (
    EMBEDDING_TASK_SEMANTIC_QUERY,
    EmbeddingRuntimeConfig,
    embedding_task_runtime_config_from_env,
    embedding_task_runtime_config_is_configured,
)
from alphavault.infra.ai.reranker_runtime_config import (
    RERANKER_TASK_SEMANTIC_QUERY,
    RerankerRuntimeConfig,
    reranker_task_runtime_config_from_env,
    reranker_task_runtime_config_is_configured,
)
from alphavault.rss.utils import RateLimiter

DEFAULT_SEMANTIC_POST_CANDIDATE_LIMIT = 40
ZILLIZ_SOURCE_OVERFETCH_MULTIPLIER = 4
MIN_QUERY_LIMIT = 1
EXTRA_FETCH_COUNT = 1
RERANK_MIN_OVERFETCH = EXTRA_FETCH_COUNT
DEFAULT_DETAIL_TITLE = "帖子详情"
DOC_KIND_ASSERTION = "assertion"
DOC_KIND_RAW_TAIL = "raw_tail"
SEMANTIC_DOC_KIND_LABELS = {
    DOC_KIND_ASSERTION: "观点",
    DOC_KIND_RAW_TAIL: "原文尾段",
}
SEMANTIC_SEARCH_EXTENSION_ERROR_TEXT = "语义搜索依赖向量检索服务，当前环境还没准备好。"


@dataclass(frozen=True)
class SemanticQueryEmbeddingRuntime:
    config: EmbeddingRuntimeConfig
    limiter: RateLimiter


@dataclass(frozen=True)
class SemanticQueryRerankerRuntime:
    config: RerankerRuntimeConfig
    limiter: RateLimiter


@dataclass(frozen=True)
class SemanticSearchCursor:
    primary_score: float
    semantic_score: float
    created_at: str
    post_uid: str


def _coerce_float(value: object) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    text = _clean_text(value)
    if not text:
        return 0.0
    return float(text)


def _clean_limit(limit: int) -> int:
    return max(MIN_QUERY_LIMIT, int(limit))


def _clean_candidate_limit(candidate_limit: int, *, limit: int) -> int:
    return max(_clean_limit(limit) + EXTRA_FETCH_COUNT, int(candidate_limit))


def _should_rerank_rows(*, row_count: int, limit: int) -> bool:
    return row_count > _clean_limit(limit) + RERANK_MIN_OVERFETCH


def _embedding_literal(vector: list[float]) -> str:
    return json.dumps(vector, ensure_ascii=False, separators=(",", ":"))


def _semantic_docs_table(source: PostgresSource) -> str:
    return qualify_postgres_table(source.schema, "semantic_docs")


def _posts_table(source: PostgresSource) -> str:
    return qualify_postgres_table(source.schema, "posts")


def _build_source_branch_sql(source: PostgresSource) -> str:
    semantic_docs_table = _semantic_docs_table(source)
    posts_table = _posts_table(source)
    return f"""
SELECT
    post_uid,
    platform,
    author,
    created_at,
    url,
    raw_text,
    doc_kind,
    match_doc_text,
    semantic_score
FROM (
    SELECT
        sd.post_uid,
        p.platform,
        p.author,
        p.created_at,
        p.url,
        p.raw_text,
        sd.doc_kind,
        sd.doc_text AS match_doc_text,
        (1 - (sd.embedding <=> CAST(:query_embedding AS halfvec({DEFAULT_SEMANTIC_DOC_EMBEDDING_DIMENSIONS})))) AS semantic_score,
        ROW_NUMBER() OVER (
            PARTITION BY sd.post_uid
            ORDER BY
                sd.embedding <=> CAST(:query_embedding AS halfvec({DEFAULT_SEMANTIC_DOC_EMBEDDING_DIMENSIONS})) ASC,
                sd.created_at_ts DESC NULLS LAST,
                sd.doc_id DESC
        ) AS post_rank
    FROM {semantic_docs_table} sd
    JOIN {posts_table} p
      ON p.post_uid = sd.post_uid
    WHERE p.processed_at IS NOT NULL
) AS ranked_docs
WHERE post_rank = 1
ORDER BY semantic_score DESC, created_at DESC, post_uid DESC
LIMIT :source_candidate_limit
""".strip()


def _build_search_sql(
    *, sources: list[PostgresSource]
) -> tuple[str, dict[str, object]]:
    params = {"source_candidate_limit": 0, "query_embedding": ""}
    branches = [f"({_build_source_branch_sql(source)})" for source in sources]
    union_sql = "\nUNION ALL\n".join(branches)
    sql = f"""
WITH semantic_candidates AS (
{union_sql}
)
SELECT
    post_uid,
    platform,
    author,
    created_at,
    url,
    raw_text,
    doc_kind,
    match_doc_text,
    semantic_score
FROM semantic_candidates
ORDER BY semantic_score DESC, created_at DESC, post_uid DESC
"""
    return sql, params


def _should_load_candidates_from_postgres() -> bool:
    return should_write_to_postgres()


def _posts_metadata_sql(
    *,
    posts_table: str,
    post_uid_placeholders: str,
) -> str:
    return f"""
SELECT
    post_uid,
    platform,
    author,
    created_at,
    url,
    raw_text
FROM {posts_table}
WHERE processed_at IS NOT NULL
  AND post_uid IN ({post_uid_placeholders})
""".strip()


def _semantic_score_from_zilliz_row(row: dict[str, object]) -> float:
    return _coerce_float(row.get("distance"))


def _zilliz_search_limit(*, candidate_limit: int) -> int:
    return max(
        int(candidate_limit),
        int(candidate_limit) * ZILLIZ_SOURCE_OVERFETCH_MULTIPLIER,
    )


def _load_posts_metadata(
    *,
    conn: object,
    source: PostgresSource,
    post_uids: list[str],
) -> dict[str, dict[str, object]]:
    resolved_post_uids = [_clean_text(post_uid) for post_uid in post_uids]
    resolved_post_uids = [post_uid for post_uid in resolved_post_uids if post_uid]
    if not resolved_post_uids:
        return {}
    placeholders = make_in_placeholders(
        prefix="post_uid",
        count=len(resolved_post_uids),
    )
    rows = read_sql_rows(
        conn,
        _posts_metadata_sql(
            posts_table=_posts_table(source),
            post_uid_placeholders=placeholders,
        ),
        params=make_in_params(prefix="post_uid", values=tuple(resolved_post_uids)),
    )
    out: dict[str, dict[str, object]] = {}
    for row in rows:
        post_uid = _clean_text(row.get("post_uid"))
        if not post_uid:
            continue
        out[post_uid] = row
    return out


def _pick_better_semantic_row(
    current: dict[str, object] | None,
    candidate: dict[str, object],
) -> dict[str, object]:
    if current is None:
        return candidate
    current_score = _coerce_float(current.get("semantic_score"))
    candidate_score = _coerce_float(candidate.get("semantic_score"))
    if candidate_score > current_score:
        return candidate
    if candidate_score < current_score:
        return current
    current_created_at = _clean_text(current.get("created_at"))
    candidate_created_at = _clean_text(candidate.get("created_at"))
    if candidate_created_at > current_created_at:
        return candidate
    if candidate_created_at < current_created_at:
        return current
    if _clean_text(candidate.get("doc_id")) > _clean_text(current.get("doc_id")):
        return candidate
    return current


def _load_source_candidate_rows_from_zilliz(
    *,
    conn: object,
    source: PostgresSource,
    query_embedding: list[float],
    candidate_limit: int,
) -> list[dict[str, object]]:
    raw_rows = semantic_search_in_zilliz(
        source.schema,
        query_vector=query_embedding,
        limit=_zilliz_search_limit(candidate_limit=candidate_limit),
        output_fields=[
            "doc_id",
            "post_uid",
            "doc_kind",
            "platform",
            "author",
            "created_at",
            "doc_text",
        ],
    )
    post_uids = [_clean_text(row.get("post_uid")) for row in raw_rows]
    posts_by_uid = _load_posts_metadata(
        conn=conn,
        source=source,
        post_uids=post_uids,
    )
    best_row_by_post_uid: dict[str, dict[str, object]] = {}
    for row in raw_rows:
        post_uid = _clean_text(row.get("post_uid"))
        post_row = posts_by_uid.get(post_uid)
        if post_row is None:
            continue
        candidate = {
            "doc_id": _clean_text(row.get("doc_id")),
            "post_uid": post_uid,
            "platform": _clean_text(post_row.get("platform"))
            or _clean_text(row.get("platform")),
            "author": _clean_text(post_row.get("author"))
            or _clean_text(row.get("author")),
            "created_at": _clean_text(post_row.get("created_at"))
            or _clean_text(row.get("created_at")),
            "url": _clean_text(post_row.get("url")),
            "raw_text": _clean_text(post_row.get("raw_text")),
            "doc_kind": _clean_text(row.get("doc_kind")),
            "match_doc_text": _clean_text(row.get("doc_text")),
            "semantic_score": _semantic_score_from_zilliz_row(row),
        }
        best_row_by_post_uid[post_uid] = _pick_better_semantic_row(
            best_row_by_post_uid.get(post_uid),
            candidate,
        )
    out = list(best_row_by_post_uid.values())
    out.sort(
        key=lambda row: (
            _coerce_float(row.get("semantic_score")),
            _clean_text(row.get("created_at")),
            _clean_text(row.get("post_uid")),
        ),
        reverse=True,
    )
    return out[: int(candidate_limit)]


@lru_cache(maxsize=1)
def semantic_query_embedding_runtime_from_env() -> SemanticQueryEmbeddingRuntime:
    config = embedding_task_runtime_config_from_env(
        task_key=EMBEDDING_TASK_SEMANTIC_QUERY,
        timeout_seconds_default=DEFAULT_EMBEDDING_TIMEOUT_SECONDS,
    )
    return SemanticQueryEmbeddingRuntime(
        config=config,
        limiter=RateLimiter(config.rpm),
    )


def _semantic_query_reranker_runtime_from_env() -> SemanticQueryRerankerRuntime | None:
    configured, _ = reranker_task_runtime_config_is_configured(
        task_key=RERANKER_TASK_SEMANTIC_QUERY,
        timeout_seconds_default=DEFAULT_RERANKER_TIMEOUT_SECONDS,
    )
    if not configured:
        return None
    config = reranker_task_runtime_config_from_env(
        task_key=RERANKER_TASK_SEMANTIC_QUERY,
        timeout_seconds_default=DEFAULT_RERANKER_TIMEOUT_SECONDS,
    )
    return SemanticQueryRerankerRuntime(
        config=config,
        limiter=RateLimiter(config.rpm),
    )


def _embed_query(
    *,
    query: str,
    runtime: SemanticQueryEmbeddingRuntime,
) -> list[float]:
    embeddings = embed_texts_with_openai(
        texts=[query],
        model_name=runtime.config.model,
        dimensions=runtime.config.dimensions,
        base_url=runtime.config.base_url,
        api_key=runtime.config.api_key,
        timeout_seconds=runtime.config.timeout_seconds,
        retry_count=runtime.config.retries,
        request_gate=runtime.limiter.wait,
    )
    if len(embeddings) != 1:
        raise RuntimeError("semantic_query_embedding_missing")
    return embeddings[0]


def _load_candidate_rows(
    *,
    query_embedding: list[float],
    candidate_limit: int,
) -> list[dict[str, object]]:
    sources = _load_search_sources_from_env()
    engine = ensure_postgres_engine(sources[0].url)
    with postgres_connect_autocommit(engine) as conn:
        if _should_load_candidates_from_postgres():
            sql, params = _build_search_sql(sources=sources)
            params["source_candidate_limit"] = int(candidate_limit)
            params["query_embedding"] = _embedding_literal(query_embedding)
            return read_sql_rows(conn, sql, params=params)
        rows: list[dict[str, object]] = []
        for source in sources:
            rows.extend(
                _load_source_candidate_rows_from_zilliz(
                    conn=conn,
                    source=source,
                    query_embedding=query_embedding,
                    candidate_limit=candidate_limit,
                )
            )
        rows.sort(
            key=lambda row: (
                _coerce_float(row.get("semantic_score")),
                _clean_text(row.get("created_at")),
                _clean_text(row.get("post_uid")),
            ),
            reverse=True,
        )
        return rows[: int(candidate_limit)]


def _semantic_match_reason(*, doc_kind: str, reranked: bool) -> str:
    label = SEMANTIC_DOC_KIND_LABELS.get(_clean_text(doc_kind), "内容")
    if reranked:
        return f"语义重排（{label}）"
    return f"语义命中（{label}）"


def _format_row(row: dict[str, object], *, reranked: bool) -> PostSearchRow:
    source_name = _clean_text(row.get("platform"))
    author = _clean_text(row.get("author"))
    created_at = _clean_text(row.get("created_at"))
    match_doc_text = _clean_text(row.get("match_doc_text"))
    raw_text = _clean_text(row.get("raw_text"))
    title = _detail_title(source_name, author=author, created_at=created_at)
    if not title:
        title = DEFAULT_DETAIL_TITLE
    return {
        "post_uid": _clean_text(row.get("post_uid")),
        "source": source_name,
        "source_label": _platform_label(source_name),
        "author": author,
        "created_at": created_at,
        "url": _clean_text(row.get("url")),
        "title": title,
        "preview": _normalize_preview(match_doc_text or raw_text),
        "raw_text": raw_text,
        "match_reason": _semantic_match_reason(
            doc_kind=_clean_text(row.get("doc_kind")),
            reranked=reranked,
        ),
    }


def _encode_cursor(row: dict[str, object]) -> str:
    payload = {
        "primary_score": _coerce_float(row.get("primary_score")),
        "semantic_score": _coerce_float(row.get("semantic_score")),
        "created_at": _clean_text(row.get("created_at")),
        "post_uid": _clean_text(row.get("post_uid")),
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _decode_cursor(cursor: str) -> SemanticSearchCursor:
    text = _clean_text(cursor)
    if not text:
        return SemanticSearchCursor(
            primary_score=0.0,
            semantic_score=0.0,
            created_at="",
            post_uid="",
        )
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("invalid_semantic_search_cursor") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("invalid_semantic_search_cursor")
    return SemanticSearchCursor(
        primary_score=_coerce_float(payload.get("primary_score")),
        semantic_score=_coerce_float(payload.get("semantic_score")),
        created_at=_clean_text(payload.get("created_at")),
        post_uid=_clean_text(payload.get("post_uid")),
    )


def _is_after_cursor(row: dict[str, object], cursor: SemanticSearchCursor) -> bool:
    if not cursor.post_uid:
        return True
    row_primary_score = _coerce_float(row.get("primary_score"))
    row_semantic_score = _coerce_float(row.get("semantic_score"))
    row_created_at = _clean_text(row.get("created_at"))
    row_post_uid = _clean_text(row.get("post_uid"))
    if row_primary_score < cursor.primary_score:
        return True
    if row_primary_score > cursor.primary_score:
        return False
    if row_semantic_score < cursor.semantic_score:
        return True
    if row_semantic_score > cursor.semantic_score:
        return False
    if row_created_at < cursor.created_at:
        return True
    if row_created_at > cursor.created_at:
        return False
    return row_post_uid < cursor.post_uid


def _apply_cursor(
    rows: list[dict[str, object]],
    *,
    cursor: str,
) -> list[dict[str, object]]:
    decoded = _decode_cursor(cursor)
    if not decoded.post_uid:
        return rows
    return [row for row in rows if _is_after_cursor(row, decoded)]


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


def _sort_key(row: dict[str, object]) -> tuple[float, float, str, str]:
    return (
        _coerce_float(row.get("primary_score")),
        _coerce_float(row.get("semantic_score")),
        _clean_text(row.get("created_at")),
        _clean_text(row.get("post_uid")),
    )


def _rerank_rows(
    *,
    query: str,
    rows: list[dict[str, object]],
    reranker_runtime: SemanticQueryRerankerRuntime | None,
    limit: int,
) -> tuple[list[dict[str, object]], bool]:
    if (
        reranker_runtime is None
        or not rows
        or not _should_rerank_rows(row_count=len(rows), limit=limit)
    ):
        out = [
            dict(row, primary_score=_coerce_float(row.get("semantic_score")))
            for row in rows
        ]
        out.sort(key=_sort_key, reverse=True)
        return out, False
    rerank_top_n = min(
        len(rows),
        max(
            _clean_limit(limit) + EXTRA_FETCH_COUNT, int(reranker_runtime.config.top_n)
        ),
    )
    rerank_results = rerank_texts(
        query=query,
        documents=[_clean_text(row.get("match_doc_text")) for row in rows],
        model_name=reranker_runtime.config.model,
        base_url=reranker_runtime.config.base_url,
        api_key=reranker_runtime.config.api_key,
        timeout_seconds=reranker_runtime.config.timeout_seconds,
        retry_count=reranker_runtime.config.retries,
        top_n=rerank_top_n,
        request_gate=reranker_runtime.limiter.wait,
    )
    ranked_rows: list[dict[str, object]] = []
    for item in rerank_results:
        if item.index < 0 or item.index >= len(rows):
            continue
        row = dict(rows[item.index])
        row["rerank_score"] = float(item.score)
        row["primary_score"] = float(item.score)
        ranked_rows.append(row)
    ranked_rows.sort(key=_sort_key, reverse=True)
    return ranked_rows, True


def _search_error_message(exc: BaseException) -> str:
    text = _clean_text(exc)
    lowered = text.lower()
    if "pgvector" in lowered or "halfvec" in lowered or "<=>" in lowered:
        return SEMANTIC_SEARCH_EXTENSION_ERROR_TEXT
    if not text:
        return type(exc).__name__
    return text


def search_posts_semantic(
    query: str,
    *,
    limit: int = DEFAULT_POST_SEARCH_LIMIT,
    cursor: str = "",
    candidate_limit: int = DEFAULT_SEMANTIC_POST_CANDIDATE_LIMIT,
) -> PostSearchResult:
    query_error = validate_post_search_query(query)
    if query_error:
        return {
            "rows": [],
            "next_cursor": "",
            "has_more": False,
            "error": query_error,
        }
    configured, message = embedding_task_runtime_config_is_configured(
        task_key=EMBEDDING_TASK_SEMANTIC_QUERY,
        timeout_seconds_default=DEFAULT_EMBEDDING_TIMEOUT_SECONDS,
    )
    if not configured:
        return {
            "rows": [],
            "next_cursor": "",
            "has_more": False,
            "error": message,
        }
    try:
        embedding_runtime = semantic_query_embedding_runtime_from_env()
        query_embedding = _embed_query(query=query, runtime=embedding_runtime)
        rows = _load_candidate_rows(
            query_embedding=query_embedding,
            candidate_limit=_clean_candidate_limit(candidate_limit, limit=limit),
        )
        reranked_rows, reranked = _rerank_rows(
            query=query,
            rows=rows,
            reranker_runtime=_semantic_query_reranker_runtime_from_env(),
            limit=limit,
        )
        visible_rows = _apply_cursor(reranked_rows, cursor=cursor)
    except BaseException as exc:
        if is_fatal_base_exception(exc):
            raise
        return {
            "rows": [],
            "next_cursor": "",
            "has_more": False,
            "error": _search_error_message(exc),
        }
    page_rows, next_cursor, has_more = _slice_page_rows(visible_rows, limit=limit)
    return {
        "rows": [_format_row(row, reranked=reranked) for row in page_rows],
        "next_cursor": next_cursor,
        "has_more": has_more,
        "error": "",
    }


__all__ = [
    "DEFAULT_SEMANTIC_POST_CANDIDATE_LIMIT",
    "search_posts_semantic",
]
