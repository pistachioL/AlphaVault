from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import json
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from redis import Redis

from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    postgres_connect_autocommit,
    qualify_postgres_table,
    require_postgres_schema_name,
    run_postgres_transaction,
)
from alphavault.db.sql import semantic_docs as semantic_docs_sql
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql_rows import read_sql_rows
from alphavault.db.zilliz_client import (
    delete_semantic_docs_by_post_uid_in_zilliz,
    replace_semantic_docs_in_zilliz,
    should_write_to_postgres,
    should_write_to_zilliz,
)
from alphavault.db.zilliz_retry_queue import enqueue_zilliz_retry
from alphavault.logging_config import get_logger

logger = get_logger(__name__)

_ASSERTIONS_TABLE_NAME = "assertions"
_ASSERTION_MENTIONS_TABLE_NAME = "assertion_mentions"
_ASSERTION_ENTITIES_TABLE_NAME = "assertion_entities"
_POSTS_TABLE_NAME = "posts"
_SEMANTIC_DOCS_TABLE_NAME = "semantic_docs"


@dataclass(frozen=True)
class SemanticAssertionSourceRow:
    assertion_id: str
    post_uid: str
    idx: int
    action: str
    action_strength: int
    summary: str
    evidence: str
    mention_texts: tuple[str, ...]
    entity_keys: tuple[str, ...]


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


def _coerce_text_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = _clean_text(item)
        if text:
            out.append(text)
    return out


@contextmanager
def _use_conn(
    engine_or_conn: PostgresEngine | PostgresConnection,
) -> Iterator[PostgresConnection]:
    if isinstance(engine_or_conn, PostgresConnection):
        yield engine_or_conn
        return
    with postgres_connect_autocommit(engine_or_conn) as conn:
        yield conn


def _source_table(engine_or_conn: object, table_name: str) -> str:
    return qualify_postgres_table(
        require_postgres_schema_name(engine_or_conn),
        table_name,
    )


def _posts_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, _POSTS_TABLE_NAME)


def _assertions_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, _ASSERTIONS_TABLE_NAME)


def _assertion_mentions_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, _ASSERTION_MENTIONS_TABLE_NAME)


def _assertion_entities_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, _ASSERTION_ENTITIES_TABLE_NAME)


def _semantic_docs_table(engine_or_conn: object) -> str:
    return _source_table(engine_or_conn, _SEMANTIC_DOCS_TABLE_NAME)


def load_assertion_semantic_rows(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    post_uid: str,
) -> list[SemanticAssertionSourceRow]:
    resolved_post_uid = _clean_text(post_uid)
    if not resolved_post_uid:
        return []
    with _use_conn(engine_or_conn) as conn:
        assertion_rows = read_sql_rows(
            conn,
            semantic_docs_sql.select_assertions_for_post_sql(_assertions_table(conn)),
            params={"post_uid": resolved_post_uid},
        )
        mention_rows = read_sql_rows(
            conn,
            semantic_docs_sql.select_assertion_mentions_for_post_sql(
                _assertion_mentions_table(conn),
                _assertions_table(conn),
            ),
            params={"post_uid": resolved_post_uid},
        )
        entity_rows = read_sql_rows(
            conn,
            semantic_docs_sql.select_assertion_entities_for_post_sql(
                _assertion_entities_table(conn),
                _assertions_table(conn),
            ),
            params={"post_uid": resolved_post_uid},
        )
    mention_texts_by_assertion_id: dict[str, list[str]] = {}
    for row in mention_rows:
        assertion_id = _clean_text(row.get("assertion_id"))
        mention_text = _clean_text(row.get("mention_text"))
        if not assertion_id or not mention_text:
            continue
        mention_texts_by_assertion_id.setdefault(assertion_id, []).append(mention_text)
    entity_keys_by_assertion_id: dict[str, list[str]] = {}
    for row in entity_rows:
        assertion_id = _clean_text(row.get("assertion_id"))
        entity_key = _clean_text(row.get("entity_key"))
        if not assertion_id or not entity_key:
            continue
        entity_keys_by_assertion_id.setdefault(assertion_id, []).append(entity_key)
    out: list[SemanticAssertionSourceRow] = []
    for row in assertion_rows:
        assertion_id = _clean_text(row.get("assertion_id"))
        if not assertion_id:
            continue
        out.append(
            SemanticAssertionSourceRow(
                assertion_id=assertion_id,
                post_uid=_clean_text(row.get("post_uid")),
                idx=max(1, _coerce_int(row.get("idx"))),
                action=_clean_text(row.get("action")),
                action_strength=max(0, _coerce_int(row.get("action_strength"))),
                summary=_clean_text(row.get("summary")),
                evidence=_clean_text(row.get("evidence")),
                mention_texts=_dedupe_texts(
                    mention_texts_by_assertion_id.get(assertion_id, [])
                ),
                entity_keys=_dedupe_texts(
                    entity_keys_by_assertion_id.get(assertion_id, [])
                ),
            )
        )
    return out


def load_assertion_semantic_rows_by_post_uids(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    post_uids: list[str],
) -> dict[str, list[SemanticAssertionSourceRow]]:
    resolved_post_uids = _dedupe_texts(
        [_clean_text(post_uid) for post_uid in post_uids]
    )
    if not resolved_post_uids:
        return {}
    out: dict[str, list[SemanticAssertionSourceRow]] = {
        post_uid: [] for post_uid in resolved_post_uids
    }
    placeholders = make_in_placeholders(
        prefix="post_uid",
        count=len(resolved_post_uids),
    )
    params = make_in_params(prefix="post_uid", values=resolved_post_uids)
    with _use_conn(engine_or_conn) as conn:
        assertions_table = _assertions_table(conn)
        assertion_rows = read_sql_rows(
            conn,
            semantic_docs_sql.select_assertions_for_posts_sql(
                assertions_table,
                post_uid_placeholders=placeholders,
            ),
            params=params,
        )
        mention_rows = read_sql_rows(
            conn,
            semantic_docs_sql.select_assertion_mentions_for_posts_sql(
                _assertion_mentions_table(conn),
                assertions_table,
                post_uid_placeholders=placeholders,
            ),
            params=params,
        )
        entity_rows = read_sql_rows(
            conn,
            semantic_docs_sql.select_assertion_entities_for_posts_sql(
                _assertion_entities_table(conn),
                assertions_table,
                post_uid_placeholders=placeholders,
            ),
            params=params,
        )
    mention_texts_by_post_assertion: dict[tuple[str, str], list[str]] = {}
    for row in mention_rows:
        post_uid = _clean_text(row.get("post_uid"))
        assertion_id = _clean_text(row.get("assertion_id"))
        mention_text = _clean_text(row.get("mention_text"))
        if not post_uid or not assertion_id or not mention_text:
            continue
        mention_texts_by_post_assertion.setdefault((post_uid, assertion_id), []).append(
            mention_text
        )
    entity_keys_by_post_assertion: dict[tuple[str, str], list[str]] = {}
    for row in entity_rows:
        post_uid = _clean_text(row.get("post_uid"))
        assertion_id = _clean_text(row.get("assertion_id"))
        entity_key = _clean_text(row.get("entity_key"))
        if not post_uid or not assertion_id or not entity_key:
            continue
        entity_keys_by_post_assertion.setdefault((post_uid, assertion_id), []).append(
            entity_key
        )
    for row in assertion_rows:
        post_uid = _clean_text(row.get("post_uid"))
        assertion_id = _clean_text(row.get("assertion_id"))
        if not post_uid or not assertion_id:
            continue
        out.setdefault(post_uid, []).append(
            SemanticAssertionSourceRow(
                assertion_id=assertion_id,
                post_uid=post_uid,
                idx=max(1, _coerce_int(row.get("idx"))),
                action=_clean_text(row.get("action")),
                action_strength=max(0, _coerce_int(row.get("action_strength"))),
                summary=_clean_text(row.get("summary")),
                evidence=_clean_text(row.get("evidence")),
                mention_texts=_dedupe_texts(
                    mention_texts_by_post_assertion.get((post_uid, assertion_id), [])
                ),
                entity_keys=_dedupe_texts(
                    entity_keys_by_post_assertion.get((post_uid, assertion_id), [])
                ),
            )
        )
    return out


def load_stored_semantic_doc_embeddings(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    post_uid: str,
) -> list[dict[str, object]]:
    resolved_post_uid = _clean_text(post_uid)
    if not resolved_post_uid:
        return []
    with _use_conn(engine_or_conn) as conn:
        return read_sql_rows(
            conn,
            semantic_docs_sql.select_stored_semantic_docs_for_post_sql(
                _semantic_docs_table(conn)
            ),
            params={"post_uid": resolved_post_uid},
        )


def load_stored_semantic_doc_embeddings_by_post_uids(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    post_uids: list[str],
) -> dict[str, list[dict[str, object]]]:
    resolved_post_uids = _dedupe_texts(
        [_clean_text(post_uid) for post_uid in post_uids]
    )
    if not resolved_post_uids:
        return {}
    placeholders = make_in_placeholders(
        prefix="post_uid",
        count=len(resolved_post_uids),
    )
    params = make_in_params(prefix="post_uid", values=resolved_post_uids)
    out: dict[str, list[dict[str, object]]] = {
        post_uid: [] for post_uid in resolved_post_uids
    }
    with _use_conn(engine_or_conn) as conn:
        rows = read_sql_rows(
            conn,
            semantic_docs_sql.select_stored_semantic_docs_for_posts_sql(
                _semantic_docs_table(conn),
                post_uid_placeholders=placeholders,
            ),
            params=params,
        )
    for row in rows:
        post_uid = _clean_text(row.get("post_uid"))
        if not post_uid:
            continue
        out.setdefault(post_uid, []).append(row)
    return out


def _normalize_insert_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        doc_id = _clean_text(row.get("doc_id"))
        post_uid = _clean_text(row.get("post_uid"))
        doc_kind = _clean_text(row.get("doc_kind"))
        embedding = _clean_text(row.get("embedding"))
        if not doc_id or not post_uid or not doc_kind or not embedding:
            continue
        normalized.append(
            {
                "doc_id": doc_id,
                "post_uid": post_uid,
                "assertion_id": _clean_text(row.get("assertion_id")),
                "doc_kind": doc_kind,
                "chunk_seq": max(1, _coerce_int(row.get("chunk_seq"))),
                "platform": _clean_text(row.get("platform")),
                "author": _clean_text(row.get("author")),
                "created_at": _clean_text(row.get("created_at")),
                "created_at_ts": row.get("created_at_ts"),
                "action": _clean_text(row.get("action")),
                "action_strength": max(0, _coerce_int(row.get("action_strength"))),
                "mention_texts": _coerce_text_list(row.get("mention_texts")),
                "entity_keys": _coerce_text_list(row.get("entity_keys")),
                "doc_text": _clean_text(row.get("doc_text")),
                "content_hash": _clean_text(row.get("content_hash")),
                "embedding_model": _clean_text(row.get("embedding_model")),
                "embedding": embedding,
                "updated_at": _clean_text(row.get("updated_at")),
            }
        )
    return normalized


def replace_semantic_docs(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    post_uid: str,
    rows: list[dict[str, object]],
    redis_client: Redis | Any = None,
) -> int:
    """
    替换指定 post_uid 的所有 semantic_docs

    Args:
        engine_or_conn: Postgres 连接或引擎
        post_uid: 帖子唯一标识
        rows: 要插入的记录列表
        redis_client: Redis 客户端，用于 Zilliz 写入失败时加入重试队列（可选）
                      如果为 None 且 Zilliz 写入失败，将只记录警告日志

    Returns:
        插入的记录数（dual_write 模式返回 Postgres 写入数）
    """
    resolved_post_uid = _clean_text(post_uid)
    normalized_rows = _normalize_insert_rows(rows)
    schema = require_postgres_schema_name(engine_or_conn)

    pg_count = 0
    zilliz_count = 0

    # 1. 写入 Postgres（根据迁移模式决定）
    if should_write_to_postgres():

        def _write(conn: PostgresConnection) -> int:
            conn.execute(
                semantic_docs_sql.delete_semantic_docs_by_post_uid_sql(
                    _semantic_docs_table(conn)
                ),
                {"post_uid": resolved_post_uid},
            )
            if not normalized_rows:
                return 0
            conn.execute(
                semantic_docs_sql.insert_semantic_doc_sql(_semantic_docs_table(conn)),
                normalized_rows,
            )
            return len(normalized_rows)

        pg_count = run_postgres_transaction(engine_or_conn, _write)

    # 2. 同步到 Zilliz Cloud（根据迁移模式决定）
    if should_write_to_zilliz():
        try:
            zilliz_count = replace_semantic_docs_in_zilliz(
                schema,
                post_uid=resolved_post_uid,
                rows=normalized_rows,
            )
        except Exception as e:
            # Zilliz 同步失败处理
            if should_write_to_postgres():
                # dual_write 模式：放入 Redis 重试队列
                logger.warning(
                    "zilliz_sync_failed post_uid=%s error=%s",
                    resolved_post_uid,
                    str(e)[:100],
                )

                # 加入重试队列
                if redis_client:
                    try:
                        enqueue_zilliz_retry(
                            redis_client,
                            operation="replace",
                            schema=schema,
                            post_uid=resolved_post_uid,
                            rows=normalized_rows,
                            retry_count=0,
                            error=str(e),
                        )
                    except Exception as retry_err:
                        logger.warning(
                            "zilliz_retry_enqueue_failed post_uid=%s: %s",
                            resolved_post_uid,
                            retry_err,
                        )
                else:
                    logger.warning(
                        "zilliz_retry_unavailable post_uid=%s (redis_client=None)",
                        resolved_post_uid,
                    )
            else:
                # zilliz_only 模式：抛出异常（主存储失败）
                raise RuntimeError(
                    f"Zilliz write failed (zilliz_only mode) for {resolved_post_uid}: {e}"
                ) from e

    return pg_count if should_write_to_postgres() else zilliz_count


def delete_semantic_docs_by_post_uid(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    post_uid: str,
    redis_client: Redis | Any = None,
) -> int:
    """
    删除指定 post_uid 的所有 semantic_docs

    Args:
        engine_or_conn: Postgres 连接或引擎
        post_uid: 帖子唯一标识
        redis_client: Redis 客户端，用于 Zilliz 删除失败时加入重试队列（可选）
                      如果为 None 且 Zilliz 删除失败，将只记录警告日志

    Returns:
        删除的记录数（dual_write 模式返回 Postgres 删除数）
    """
    resolved_post_uid = _clean_text(post_uid)
    if not resolved_post_uid:
        return 0

    schema = require_postgres_schema_name(engine_or_conn)
    pg_count = 0

    # 1. 删除 Postgres 记录（根据迁移模式决定）
    if should_write_to_postgres():

        def _write(conn: PostgresConnection) -> int:
            result = conn.execute(
                semantic_docs_sql.delete_semantic_docs_by_post_uid_sql(
                    _semantic_docs_table(conn)
                ),
                {"post_uid": resolved_post_uid},
            )
            return int(result.rowcount or 0)

        pg_count = run_postgres_transaction(engine_or_conn, _write)

    # 2. 同步删除 Zilliz 记录（根据迁移模式决定）
    zilliz_count = 0
    if should_write_to_zilliz():
        try:
            zilliz_count = delete_semantic_docs_by_post_uid_in_zilliz(
                schema,
                post_uid=resolved_post_uid,
            )
        except Exception as e:
            if should_write_to_postgres():
                logger.warning(
                    "zilliz_delete_failed post_uid=%s error=%s",
                    resolved_post_uid,
                    str(e)[:100],
                )

                if redis_client:
                    try:
                        enqueue_zilliz_retry(
                            redis_client,
                            operation="delete",
                            schema=schema,
                            post_uid=resolved_post_uid,
                            rows=[],
                            retry_count=0,
                            error=str(e),
                        )
                    except Exception as retry_err:
                        logger.warning(
                            "zilliz_delete_retry_enqueue_failed post_uid=%s: %s",
                            resolved_post_uid,
                            retry_err,
                        )
                else:
                    logger.warning(
                        "zilliz_delete_retry_unavailable post_uid=%s (redis_client=None)",
                        resolved_post_uid,
                    )
            else:
                raise RuntimeError(
                    f"Zilliz delete failed (zilliz_only mode) for {resolved_post_uid}: {e}"
                ) from e

    return pg_count if should_write_to_postgres() else zilliz_count


def scan_relevant_post_uids(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    after_post_uid: str,
    batch_size: int,
) -> list[str]:
    with _use_conn(engine_or_conn) as conn:
        rows = read_sql_rows(
            conn,
            semantic_docs_sql.select_relevant_post_uids_sql(_posts_table(conn)),
            params={
                "after_post_uid": _clean_text(after_post_uid),
                "limit": max(1, int(batch_size)),
            },
        )
    out: list[str] = []
    for row in rows:
        post_uid = _clean_text(row.get("post_uid"))
        if post_uid:
            out.append(post_uid)
    return out


def parse_vector_text(value: object) -> list[float]:
    text = _clean_text(value)
    if not text:
        return []
    parsed = json.loads(text)
    if not isinstance(parsed, list):
        raise RuntimeError("semantic_doc_vector_invalid")
    return [float(item) for item in parsed]


__all__ = [
    "SemanticAssertionSourceRow",
    "delete_semantic_docs_by_post_uid",
    "load_assertion_semantic_rows",
    "load_assertion_semantic_rows_by_post_uids",
    "load_stored_semantic_doc_embeddings",
    "load_stored_semantic_doc_embeddings_by_post_uids",
    "parse_vector_text",
    "replace_semantic_docs",
    "scan_relevant_post_uids",
]
