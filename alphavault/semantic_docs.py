from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
import hashlib
import json
from typing import NamedTuple

from alphavault.ai.embedding import embed_texts_with_openai
from alphavault.constants import (
    DEFAULT_EMBEDDING_TIMEOUT_SECONDS,
    PLATFORM_WEIBO,
    PLATFORM_XUEQIU,
)
from alphavault.db.postgres_db import PostgresConnection, PostgresEngine
from alphavault.db.semantic_docs import (
    SemanticAssertionSourceRow,
    delete_semantic_docs_by_post_uid,
    load_assertion_semantic_rows,
    load_stored_semantic_doc_embeddings,
    parse_vector_text,
    replace_semantic_docs,
)
from alphavault.db.source_queue import CloudPost, load_cloud_post
from alphavault.domains.signal.aggregator import coerce_signal_timestamp
from alphavault.domains.thread_tree.parse import parse_thread_segments
from alphavault.infra.ai.embedding_runtime_config import (
    EMBEDDING_TASK_SEMANTIC_DOC_SYNC,
    EmbeddingRuntimeConfig,
    embedding_task_runtime_config_is_configured,
    embedding_task_runtime_config_from_env,
)
from alphavault.rss.utils import RateLimiter, split_xueqiu_context_segments
from alphavault.timeutil import now_cst_str
from alphavault.weibo.thread_text import SEGMENT_SEPARATOR, strip_image_label_lines

DOC_KIND_ASSERTION = "assertion"
DOC_KIND_RAW_TAIL = "raw_tail"
RAW_TAIL_MIN_CHARS = 80


@dataclass(frozen=True)
class SemanticDoc:
    doc_id: str
    post_uid: str
    assertion_id: str
    doc_kind: str
    chunk_seq: int
    platform: str
    author: str
    created_at: str
    created_at_ts: datetime | None
    action: str
    action_strength: int
    mention_texts: tuple[str, ...]
    entity_keys: tuple[str, ...]
    doc_text: str
    content_hash: str


@dataclass(frozen=True)
class SemanticDocEmbeddingRuntime:
    config: EmbeddingRuntimeConfig
    limiter: RateLimiter


SemanticDocSyncResult = NamedTuple(
    "SemanticDocSyncResult",
    [
        ("post_uid", str),
        ("doc_count", int),
        ("embedded_count", int),
        ("reused_count", int),
        ("deleted_count", int),
        ("applied", bool),
    ],
)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _dedupe_texts(values: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return tuple(out)


def _hash_text(*parts: str) -> str:
    joined = "\n".join(str(part or "") for part in parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def _build_assertion_doc_text(
    *,
    post: CloudPost,
    row: SemanticAssertionSourceRow,
) -> str:
    lines = [
        f"作者：{_clean_text(post.author)}",
        f"时间：{_clean_text(post.created_at)}",
        f"动作：{_clean_text(row.action)}",
        f"强度：{int(row.action_strength)}",
        f"摘要：{_clean_text(row.summary)}",
        f"证据：{_clean_text(row.evidence)}",
    ]
    mention_texts = _dedupe_texts(list(row.mention_texts))
    if mention_texts:
        lines.append(f"提及：{'，'.join(mention_texts)}")
    return "\n".join(line for line in lines if _clean_text(line))


def _normalize_fallback_segments(raw_text: str) -> list[str]:
    cleaned_text = strip_image_label_lines(raw_text)
    if not cleaned_text:
        return []
    if SEGMENT_SEPARATOR not in cleaned_text:
        return [_clean_text(cleaned_text)] if _clean_text(cleaned_text) else []
    return [
        segment
        for segment in (
            _clean_text(item) for item in cleaned_text.split(SEGMENT_SEPARATOR)
        )
        if segment
    ]


def _raw_text_segments(post: CloudPost) -> list[str]:
    platform = _clean_text(post.platform).lower()
    raw_text = str(post.raw_text or "")
    if platform == PLATFORM_WEIBO:
        return [
            segment
            for segment in parse_thread_segments(raw_text)
            if _clean_text(segment)
        ]
    if platform == PLATFORM_XUEQIU:
        return [
            _clean_text(strip_image_label_lines(segment))
            for segment in split_xueqiu_context_segments(raw_text)
            if _clean_text(strip_image_label_lines(segment))
        ]
    return _normalize_fallback_segments(raw_text)


def _build_raw_tail_text(post: CloudPost) -> str:
    segments = _raw_text_segments(post)
    if not segments:
        return ""
    tail_segments = [segments[-1]]
    if len(tail_segments[0]) < RAW_TAIL_MIN_CHARS and len(segments) >= 2:
        tail_segments.insert(0, segments[-2])
    tail_text = SEGMENT_SEPARATOR.join(tail_segments).strip()
    if not tail_text:
        return ""
    return "\n".join(
        [
            f"作者：{_clean_text(post.author)}",
            f"时间：{_clean_text(post.created_at)}",
            "帖子原文尾段：",
            tail_text,
        ]
    ).strip()


def build_semantic_docs(
    *,
    post: CloudPost,
    assertion_rows: list[SemanticAssertionSourceRow],
) -> list[SemanticDoc]:
    resolved_post_uid = _clean_text(post.post_uid)
    created_at = _clean_text(post.created_at)
    created_at_ts = coerce_signal_timestamp(created_at)
    docs: list[SemanticDoc] = []
    for row in assertion_rows:
        doc_text = _build_assertion_doc_text(post=post, row=row)
        if not doc_text:
            continue
        docs.append(
            SemanticDoc(
                doc_id=f"{resolved_post_uid}:assertion:{row.assertion_id}",
                post_uid=resolved_post_uid,
                assertion_id=row.assertion_id,
                doc_kind=DOC_KIND_ASSERTION,
                chunk_seq=max(1, int(row.idx)),
                platform=_clean_text(post.platform),
                author=_clean_text(post.author),
                created_at=created_at,
                created_at_ts=created_at_ts,
                action=_clean_text(row.action),
                action_strength=max(0, int(row.action_strength)),
                mention_texts=_dedupe_texts(list(row.mention_texts)),
                entity_keys=_dedupe_texts(list(row.entity_keys)),
                doc_text=doc_text,
                content_hash=_hash_text(DOC_KIND_ASSERTION, row.assertion_id, doc_text),
            )
        )
    raw_tail_text = _build_raw_tail_text(post)
    if raw_tail_text:
        docs.append(
            SemanticDoc(
                doc_id=f"{resolved_post_uid}:raw_tail:1",
                post_uid=resolved_post_uid,
                assertion_id="",
                doc_kind=DOC_KIND_RAW_TAIL,
                chunk_seq=1,
                platform=_clean_text(post.platform),
                author=_clean_text(post.author),
                created_at=created_at,
                created_at_ts=created_at_ts,
                action="",
                action_strength=0,
                mention_texts=(),
                entity_keys=(),
                doc_text=raw_tail_text,
                content_hash=_hash_text(DOC_KIND_RAW_TAIL, raw_tail_text),
            )
        )
    return docs


def _embedding_literal(vector: list[float]) -> str:
    return json.dumps(vector, ensure_ascii=False, separators=(",", ":"))


def _stored_embedding_map(
    rows: list[dict[str, object]],
) -> dict[str, tuple[str, str, list[float]]]:
    out: dict[str, tuple[str, str, list[float]]] = {}
    for row in rows:
        doc_id = _clean_text(row.get("doc_id"))
        if not doc_id:
            continue
        try:
            out[doc_id] = (
                _clean_text(row.get("content_hash")),
                _clean_text(row.get("embedding_model")),
                parse_vector_text(row.get("embedding_text")),
            )
        except Exception:
            continue
    return out


def _embedding_model_signature(*, model_name: str, dimensions: int) -> str:
    return f"{_clean_text(model_name)}@{int(dimensions)}"


def _stored_docs_match_current_docs(
    *,
    docs: list[SemanticDoc],
    stored_by_doc_id: dict[str, tuple[str, str, list[float]]],
    embedding_model_signature: str,
    embedding_dimensions: int,
) -> bool:
    if len(docs) != len(stored_by_doc_id):
        return False
    for doc in docs:
        stored = stored_by_doc_id.get(doc.doc_id)
        if stored is None:
            return False
        stored_content_hash, stored_model, stored_embedding = stored
        if stored_content_hash != doc.content_hash:
            return False
        if stored_model != embedding_model_signature:
            return False
        if len(stored_embedding) != int(embedding_dimensions):
            return False
    return True


def _assert_embedding_dimensions(
    embeddings: list[list[float]],
    *,
    expected_dimensions: int,
) -> None:
    for vector in embeddings:
        if len(vector) != int(expected_dimensions):
            raise RuntimeError(
                f"semantic_doc_embedding_dimension_mismatch:{len(vector)}"
            )


def _embed_docs(
    *,
    docs: list[SemanticDoc],
    runtime: SemanticDocEmbeddingRuntime,
) -> list[list[float]]:
    if not docs:
        return []
    batch_size = max(1, int(runtime.config.batch_size))
    all_embeddings: list[list[float]] = []
    for start_idx in range(0, len(docs), batch_size):
        batch_docs = docs[start_idx : start_idx + batch_size]
        batch_embeddings = embed_texts_with_openai(
            texts=[doc.doc_text for doc in batch_docs],
            model_name=runtime.config.model,
            dimensions=runtime.config.dimensions,
            base_url=runtime.config.base_url,
            api_key=runtime.config.api_key,
            timeout_seconds=runtime.config.timeout_seconds,
            retry_count=runtime.config.retries,
            request_gate=runtime.limiter.wait,
        )
        all_embeddings.extend(batch_embeddings)
    _assert_embedding_dimensions(
        all_embeddings,
        expected_dimensions=runtime.config.dimensions,
    )
    return all_embeddings


@lru_cache(maxsize=1)
def semantic_doc_embedding_runtime_from_env() -> SemanticDocEmbeddingRuntime:
    config = embedding_task_runtime_config_from_env(
        task_key=EMBEDDING_TASK_SEMANTIC_DOC_SYNC,
        timeout_seconds_default=DEFAULT_EMBEDDING_TIMEOUT_SECONDS,
    )
    return SemanticDocEmbeddingRuntime(
        config=config,
        limiter=RateLimiter(config.rpm),
    )


@lru_cache(maxsize=1)
def semantic_doc_embedding_is_configured() -> tuple[bool, str]:
    return embedding_task_runtime_config_is_configured(
        task_key=EMBEDDING_TASK_SEMANTIC_DOC_SYNC,
        timeout_seconds_default=DEFAULT_EMBEDDING_TIMEOUT_SECONDS,
    )


def _build_insert_payload(
    *,
    doc: SemanticDoc,
    embedding_model: str,
    embedding: list[float],
    updated_at: str,
) -> dict[str, object]:
    return {
        "doc_id": doc.doc_id,
        "post_uid": doc.post_uid,
        "assertion_id": doc.assertion_id,
        "doc_kind": doc.doc_kind,
        "chunk_seq": doc.chunk_seq,
        "platform": doc.platform,
        "author": doc.author,
        "created_at": doc.created_at,
        "created_at_ts": doc.created_at_ts,
        "action": doc.action,
        "action_strength": doc.action_strength,
        "mention_texts": list(doc.mention_texts),
        "entity_keys": list(doc.entity_keys),
        "doc_text": doc.doc_text,
        "content_hash": doc.content_hash,
        "embedding_model": embedding_model,
        "embedding": _embedding_literal(embedding),
        "updated_at": updated_at,
    }


def sync_semantic_docs_for_post(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    post_uid: str,
    final_status: str = "",
    prefetched_post: CloudPost | None = None,
    prefetched_assertion_rows: list[SemanticAssertionSourceRow] | None = None,
    prefetched_stored_rows: list[dict[str, object]] | None = None,
    apply: bool = True,
    embedding_runtime: SemanticDocEmbeddingRuntime | None = None,
) -> SemanticDocSyncResult:
    resolved_post_uid = _clean_text(post_uid)
    resolved_final_status = _clean_text(final_status)
    if not resolved_post_uid:
        raise RuntimeError("semantic_doc_post_uid_missing")
    if resolved_final_status and resolved_final_status != "relevant":
        deleted_count = (
            delete_semantic_docs_by_post_uid(engine_or_conn, post_uid=resolved_post_uid)
            if apply
            else 0
        )
        return SemanticDocSyncResult(
            post_uid=resolved_post_uid,
            doc_count=0,
            embedded_count=0,
            reused_count=0,
            deleted_count=deleted_count,
            applied=apply,
        )
    post = (
        prefetched_post
        if prefetched_post is not None
        else load_cloud_post(
            engine_or_conn,
            resolved_post_uid,
        )
    )
    assertion_rows = (
        prefetched_assertion_rows
        if prefetched_assertion_rows is not None
        else load_assertion_semantic_rows(
            engine_or_conn,
            post_uid=resolved_post_uid,
        )
    )
    docs = build_semantic_docs(post=post, assertion_rows=assertion_rows)
    if not docs:
        deleted_count = (
            delete_semantic_docs_by_post_uid(engine_or_conn, post_uid=resolved_post_uid)
            if apply
            else 0
        )
        return SemanticDocSyncResult(
            post_uid=resolved_post_uid,
            doc_count=0,
            embedded_count=0,
            reused_count=0,
            deleted_count=deleted_count,
            applied=apply,
        )
    if not apply:
        return SemanticDocSyncResult(
            post_uid=resolved_post_uid,
            doc_count=len(docs),
            embedded_count=len(docs),
            reused_count=0,
            deleted_count=0,
            applied=False,
        )
    runtime = embedding_runtime or semantic_doc_embedding_runtime_from_env()
    stored_rows = (
        prefetched_stored_rows
        if prefetched_stored_rows is not None
        else load_stored_semantic_doc_embeddings(
            engine_or_conn,
            post_uid=resolved_post_uid,
        )
    )
    stored_by_doc_id = _stored_embedding_map(stored_rows)
    docs_needing_embedding: list[SemanticDoc] = []
    embeddings_by_doc_id: dict[str, list[float]] = {}
    reused_count = 0
    embedding_model_signature = _embedding_model_signature(
        model_name=runtime.config.model,
        dimensions=runtime.config.dimensions,
    )
    for doc in docs:
        stored = stored_by_doc_id.get(doc.doc_id)
        if stored is None:
            docs_needing_embedding.append(doc)
            continue
        stored_content_hash, stored_model, stored_embedding = stored
        if (
            stored_content_hash == doc.content_hash
            and stored_model == embedding_model_signature
            and stored_embedding
            and len(stored_embedding) == int(runtime.config.dimensions)
        ):
            embeddings_by_doc_id[doc.doc_id] = stored_embedding
            reused_count += 1
            continue
        docs_needing_embedding.append(doc)
    if docs_needing_embedding:
        embeddings = _embed_docs(docs=docs_needing_embedding, runtime=runtime)
        for doc, embedding in zip(docs_needing_embedding, embeddings, strict=True):
            embeddings_by_doc_id[doc.doc_id] = embedding
    if not docs_needing_embedding and _stored_docs_match_current_docs(
        docs=docs,
        stored_by_doc_id=stored_by_doc_id,
        embedding_model_signature=embedding_model_signature,
        embedding_dimensions=runtime.config.dimensions,
    ):
        return SemanticDocSyncResult(
            post_uid=resolved_post_uid,
            doc_count=len(docs),
            embedded_count=0,
            reused_count=reused_count,
            deleted_count=0,
            applied=True,
        )
    updated_at = now_cst_str()
    inserted_rows = [
        _build_insert_payload(
            doc=doc,
            embedding_model=embedding_model_signature,
            embedding=embeddings_by_doc_id[doc.doc_id],
            updated_at=updated_at,
        )
        for doc in docs
    ]
    replace_semantic_docs(
        engine_or_conn,
        post_uid=resolved_post_uid,
        rows=inserted_rows,
    )
    return SemanticDocSyncResult(
        post_uid=resolved_post_uid,
        doc_count=len(docs),
        embedded_count=len(docs_needing_embedding),
        reused_count=reused_count,
        deleted_count=0,
        applied=True,
    )


__all__ = [
    "DOC_KIND_ASSERTION",
    "DOC_KIND_RAW_TAIL",
    "SemanticDoc",
    "SemanticDocEmbeddingRuntime",
    "SemanticDocSyncResult",
    "build_semantic_docs",
    "semantic_doc_embedding_is_configured",
    "semantic_doc_embedding_runtime_from_env",
    "sync_semantic_docs_for_post",
]
