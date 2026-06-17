"""
Zilliz Cloud 向量数据库客户端

替代 Postgres pgvector 用于存储和检索 semantic_docs。

环境变量配置:
- ZILLIZ_CLOUD_URI: Zilliz Cloud 集群 URI
- ZILLIZ_CLOUD_TOKEN: Zilliz Cloud API Token
- ZILLIZ_MIGRATION_MODE: 迁移模式（dual_write/zilliz_only/postgres_only，默认 dual_write）
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from pymilvus import MilvusClient

from alphavault.constants import SCHEMA_WEIBO, SCHEMA_XUEQIU
from alphavault.db.zilliz_constants import (
    ZILLIZ_CLOUD_TOKEN,
    ZILLIZ_CLOUD_URI,
    ZILLIZ_MIGRATION_MODE,
)
from alphavault.logging_config import get_logger

logger = get_logger(__name__)

MigrationMode = Literal["dual_write", "zilliz_only", "postgres_only"]


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


def _parse_vector_text(value: object) -> list[float]:
    """解析 Postgres 向量文本格式 '[0.1,0.2,...]' 为 Python list"""
    text = _clean_text(value)
    if not text:
        return []
    # 去掉前后的 '[' 和 ']'
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    if not text:
        return []
    try:
        return [float(x.strip()) for x in text.split(",") if x.strip()]
    except ValueError:
        return []


def _escape_filter_string(value: str) -> str:
    """
    转义 Zilliz filter 表达式中的特殊字符，防止注入风险

    Args:
        value: 原始字符串

    Returns:
        转义后的字符串（可安全用于 filter 表达式）

    Example:
        >>> _escape_filter_string('post_123')
        'post_123'
        >>> _escape_filter_string('post_"123"')
        'post_\\\\"123\\\\"'
    """
    # 转义反斜杠（必须先转义，否则会影响后续转义）
    value = value.replace("\\", "\\\\")
    # 转义双引号
    value = value.replace('"', '\\"')
    # 转义单引号
    value = value.replace("'", "\\'")
    return value


def _is_zilliz_enabled() -> bool:
    """检查是否配置了 Zilliz Cloud"""
    uri = os.getenv(ZILLIZ_CLOUD_URI, "").strip()
    token = os.getenv(ZILLIZ_CLOUD_TOKEN, "").strip()
    return bool(uri and token)


@lru_cache(maxsize=1)
def get_migration_mode() -> MigrationMode:
    """
    获取迁移模式

    Returns:
        "dual_write": 同时写入 Postgres 和 Zilliz（默认）
        "zilliz_only": 只写入 Zilliz
        "postgres_only": 只写入 Postgres（禁用 Zilliz）
    """
    mode = os.getenv(ZILLIZ_MIGRATION_MODE, "dual_write").strip().lower()
    if mode in ("zilliz_only", "postgres_only"):
        return mode  # type: ignore
    return "dual_write"


def should_write_to_postgres() -> bool:
    """是否需要写入 Postgres"""
    mode = get_migration_mode()
    return mode in ("dual_write", "postgres_only")


def should_write_to_zilliz() -> bool:
    """是否需要写入 Zilliz"""
    mode = get_migration_mode()
    return mode in ("dual_write", "zilliz_only") and _is_zilliz_enabled()


@lru_cache(maxsize=1)
def get_zilliz_client() -> MilvusClient:
    """获取 Zilliz Cloud 客户端（单例）"""
    if not _is_zilliz_enabled():
        raise RuntimeError(
            "Zilliz Cloud not configured. Set ZILLIZ_CLOUD_URI and ZILLIZ_CLOUD_TOKEN in .env"
        )

    try:
        from pymilvus import MilvusClient
    except ImportError as e:
        raise RuntimeError(
            "pymilvus not installed. Run: uv pip install pymilvus"
        ) from e

    uri = os.getenv(ZILLIZ_CLOUD_URI)
    token = os.getenv(ZILLIZ_CLOUD_TOKEN)

    return MilvusClient(uri=uri, token=token)


def _get_collection_name(schema: str) -> str:
    """根据 schema 名称获取 Zilliz 集合名称"""
    normalized = _clean_text(schema).lower()
    if normalized == SCHEMA_XUEQIU:
        return "alphavault_xueqiu_semantic"
    if normalized == SCHEMA_WEIBO:
        return "alphavault_weibo_semantic"
    # 未知 schema，使用通用命名
    return f"alphavault_{normalized}_semantic"


def replace_semantic_docs_in_zilliz(
    schema: str,
    *,
    post_uid: str,
    rows: list[dict[str, Any]],
) -> int:
    """
    替换 Zilliz 中某个 post_uid 的所有 semantic_docs

    Args:
        schema: 数据库 schema 名称（xueqiu/weibo）
        post_uid: 帖子唯一标识
        rows: 要插入的记录列表（格式同 Postgres）

    Returns:
        插入的记录数
    """
    if not should_write_to_zilliz():
        return 0

    client = get_zilliz_client()
    collection_name = _get_collection_name(schema)
    resolved_post_uid = _clean_text(post_uid)
    safe_post_uid = _escape_filter_string(resolved_post_uid)

    # 1. 删除旧记录
    try:
        client.delete(
            collection_name=collection_name,
            filter=f'post_uid == "{safe_post_uid}"',
        )
    except Exception as e:
        # 集合可能不存在，忽略错误
        logger.warning(
            "zilliz_delete_old_records_failed collection=%s post_uid=%s error=%s",
            collection_name,
            resolved_post_uid,
            str(e)[:200],
        )

    # 2. 插入新记录
    if not rows:
        return 0

    # 准备批量插入数据（行格式：list of dicts）
    data: list[dict[str, Any]] = []

    for row in rows:
        try:
            # 解析 embedding（可能是字符串或已经是 list）
            embedding_raw = row.get("embedding")
            if isinstance(embedding_raw, str):
                embedding = _parse_vector_text(embedding_raw)
            elif isinstance(embedding_raw, list):
                embedding = embedding_raw
            else:
                logger.warning(
                    "zilliz_invalid_embedding_format doc_id=%s type=%s",
                    row.get("doc_id"),
                    type(embedding_raw).__name__,
                )
                continue

            if not embedding:
                logger.warning("zilliz_empty_embedding doc_id=%s", row.get("doc_id"))
                continue

            # 构造单条记录
            data.append(
                {
                    "doc_id": _clean_text(row.get("doc_id")),
                    "post_uid": _clean_text(row.get("post_uid")),
                    "assertion_id": _clean_text(row.get("assertion_id")),
                    "doc_kind": _clean_text(row.get("doc_kind")),
                    "chunk_seq": _coerce_int(row.get("chunk_seq")),
                    "platform": _clean_text(row.get("platform")),
                    "author": _clean_text(row.get("author"))[:255],
                    "created_at": _clean_text(row.get("created_at")),
                    "action": _clean_text(row.get("action")),
                    "action_strength": _coerce_int(row.get("action_strength")),
                    "doc_text": _clean_text(row.get("doc_text"))[:65535],
                    "content_hash": _clean_text(row.get("content_hash")),
                    "embedding_model": _clean_text(row.get("embedding_model")),
                    "embedding": embedding,
                }
            )

        except Exception as e:
            logger.warning(
                "zilliz_prepare_data_failed doc_id=%s error=%s",
                row.get("doc_id"),
                str(e)[:200],
            )

    # 批量插入
    if data:
        try:
            client.insert(collection_name=collection_name, data=data)
        except Exception as e:
            logger.error(
                "zilliz_batch_insert_failed collection=%s count=%d error=%s",
                collection_name,
                len(data),
                str(e)[:500],
            )
            raise

    return len(data)


def delete_semantic_docs_by_post_uid_in_zilliz(
    schema: str,
    *,
    post_uid: str,
) -> int:
    """
    删除 Zilliz 中某个 post_uid 的所有 semantic_docs

    Args:
        schema: 数据库 schema 名称（xueqiu/weibo）
        post_uid: 帖子唯一标识

    Returns:
        删除的记录数（Zilliz 不返回准确数字，返回 1 表示成功）
    """
    if not should_write_to_zilliz():
        return 0

    client = get_zilliz_client()
    collection_name = _get_collection_name(schema)
    resolved_post_uid = _clean_text(post_uid)
    safe_post_uid = _escape_filter_string(resolved_post_uid)

    try:
        client.delete(
            collection_name=collection_name,
            filter=f'post_uid == "{safe_post_uid}"',
        )
        return 1  # Zilliz 不返回准确删除数，返回 1 表示成功
    except Exception as e:
        logger.warning(
            "zilliz_delete_failed collection=%s post_uid=%s error=%s",
            collection_name,
            resolved_post_uid,
            str(e)[:200],
        )
        return 0


def semantic_search_in_zilliz(
    schema: str,
    *,
    query_vector: list[float],
    limit: int = 20,
    filter_expr: str = "",
    output_fields: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    在 Zilliz 中执行语义搜索

    Args:
        schema: 数据库 schema 名称（xueqiu/weibo）
        query_vector: 查询向量（embedding）
        limit: 返回结果数量
        filter_expr: 过滤表达式（例如: 'action == "trade.buy"'）
        output_fields: 返回的字段列表（默认返回所有）

    Returns:
        搜索结果列表，每个元素包含 doc_id, distance, 以及请求的字段
    """
    if not _is_zilliz_enabled():
        return []

    client = get_zilliz_client()
    collection_name = _get_collection_name(schema)

    if output_fields is None:
        output_fields = [
            "doc_id",
            "post_uid",
            "assertion_id",
            "doc_kind",
            "platform",
            "author",
            "created_at",
            "action",
            "action_strength",
            "doc_text",
        ]

    try:
        results = client.search(
            collection_name=collection_name,
            data=[query_vector],
            limit=limit,
            filter=filter_expr if filter_expr else None,
            output_fields=output_fields,
        )
        return results[0] if results else []
    except Exception as e:
        logger.error(
            "zilliz_search_failed collection=%s limit=%d error=%s",
            collection_name,
            limit,
            str(e)[:500],
        )
        return []


__all__ = [
    "get_zilliz_client",
    "get_migration_mode",
    "should_write_to_postgres",
    "should_write_to_zilliz",
    "replace_semantic_docs_in_zilliz",
    "delete_semantic_docs_by_post_uid_in_zilliz",
    "semantic_search_in_zilliz",
]
