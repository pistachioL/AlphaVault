#!/usr/bin/env python3
"""
迁移 Supabase semantic_docs 表到 Zilliz Cloud

功能：
1. 从 Postgres 读取 semantic_docs 表（xueqiu + weibo schemas）
2. 批量上传到 Zilliz Cloud 的两个集合
3. 支持断点续传（记录已迁移的 doc_id）
4. 验证迁移完整性

使用方法：
    # 1. 安装依赖
    uv pip install pymilvus

    # 2. 设置环境变量（添加到 .env）
    ZILLIZ_CLOUD_URI=your-cluster-uri  # 从 Zilliz Cloud 控制台获取
    ZILLIZ_CLOUD_TOKEN=your-api-key    # 从 Zilliz Cloud 控制台获取

    # 3. 运行迁移（先试运行）
    uv run python scripts/migrate_to_zilliz.py --dry-run

    # 4. 正式迁移
    uv run python scripts/migrate_to_zilliz.py

    # 5. 验证迁移结果
    uv run python scripts/migrate_to_zilliz.py --verify
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from alphavault.constants import SCHEMA_WEIBO, SCHEMA_XUEQIU
from alphavault.db.postgres_db import (
    PostgresEngine,
    ensure_postgres_engine,
    postgres_connect_autocommit,
)
from alphavault.db.postgres_env import require_postgres_source_from_env
from alphavault.db.sql_rows import read_sql_rows
from alphavault.env import load_dotenv_if_present


@dataclass
class MigrationConfig:
    """迁移配置"""

    zilliz_uri: str
    zilliz_token: str
    batch_size: int = 100
    vector_dim: int = 2048  # 从 schema 可知是 HALFVEC(2048)
    state_file: str = ".zilliz_migration_state.json"
    dry_run: bool = False


@dataclass
class MigrationStats:
    """迁移统计"""

    total_count: int = 0
    migrated_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    start_time: float = 0.0

    def report(self) -> str:
        elapsed = time.time() - self.start_time
        return (
            f"迁移统计:\n"
            f"  总记录数: {self.total_count}\n"
            f"  已迁移: {self.migrated_count}\n"
            f"  跳过: {self.skipped_count}\n"
            f"  错误: {self.error_count}\n"
            f"  耗时: {elapsed:.1f}秒"
        )


def parse_vector_text(value: object) -> list[float]:
    """解析 Postgres 向量文本格式 '[0.1,0.2,...]' 为 Python list"""
    text = str(value or "").strip()
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


def load_migration_state(state_file: str) -> dict[str, set[str]]:
    """加载迁移状态（已迁移的 doc_id）"""
    if not os.path.exists(state_file):
        return {"xueqiu": set(), "weibo": set()}
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {
                "xueqiu": set(data.get("xueqiu", [])),
                "weibo": set(data.get("weibo", [])),
            }
    except Exception as e:
        print(f"⚠️  读取状态文件失败: {e}，将从头开始迁移")
        return {"xueqiu": set(), "weibo": set()}


def save_migration_state(state_file: str, state: dict[str, set[str]]) -> None:
    """保存迁移状态"""
    try:
        data = {"xueqiu": list(state["xueqiu"]), "weibo": list(state["weibo"])}
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️  保存状态文件失败: {e}")


def fetch_semantic_docs_from_postgres(
    engine: PostgresEngine,
    schema: str,
    limit: int = 0,
    offset: int = 0,
    batch_size: int = 1000,
) -> list[dict[str, Any]]:
    """从 Postgres 分批读取 semantic_docs 表

    Args:
        engine: Postgres 引擎
        schema: 数据库 schema
        limit: 限制读取条数，0 表示读取全部（用于测试）
        offset: 起始偏移量
        batch_size: 批次大小
    """
    if limit > 0:
        limit_clause = f"LIMIT {limit} OFFSET {offset}"
    else:
        limit_clause = f"LIMIT {batch_size} OFFSET {offset}"

    sql = f"""
    SELECT
        doc_id,
        post_uid,
        assertion_id,
        doc_kind,
        chunk_seq,
        platform,
        author,
        created_at,
        created_at_ts,
        action,
        action_strength,
        mention_texts,
        entity_keys,
        doc_text,
        content_hash,
        embedding_model,
        CAST(embedding AS TEXT) AS embedding_text,
        updated_at
    FROM {schema}.semantic_docs
    ORDER BY created_at_ts DESC, doc_id ASC
    {limit_clause}
    """
    with postgres_connect_autocommit(engine) as conn:
        rows = read_sql_rows(conn, sql, params={})
    return rows


def create_zilliz_collection(
    client: Any, collection_name: str, vector_dim: int
) -> None:
    """创建 Zilliz 集合（如果不存在）"""
    from pymilvus import CollectionSchema, DataType, FieldSchema

    # 检查集合是否存在
    if client.has_collection(collection_name):
        print(f"✓ 集合 {collection_name} 已存在")
        return

    # 定义 schema
    fields = [
        FieldSchema(
            name="doc_id", dtype=DataType.VARCHAR, is_primary=True, max_length=255
        ),
        FieldSchema(name="post_uid", dtype=DataType.VARCHAR, max_length=255),
        FieldSchema(name="assertion_id", dtype=DataType.VARCHAR, max_length=255),
        FieldSchema(name="doc_kind", dtype=DataType.VARCHAR, max_length=50),
        FieldSchema(name="chunk_seq", dtype=DataType.INT32),
        FieldSchema(name="platform", dtype=DataType.VARCHAR, max_length=50),
        FieldSchema(name="author", dtype=DataType.VARCHAR, max_length=255),
        FieldSchema(name="created_at", dtype=DataType.VARCHAR, max_length=50),
        FieldSchema(name="action", dtype=DataType.VARCHAR, max_length=100),
        FieldSchema(name="action_strength", dtype=DataType.INT32),
        FieldSchema(name="doc_text", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="content_hash", dtype=DataType.VARCHAR, max_length=100),
        FieldSchema(name="embedding_model", dtype=DataType.VARCHAR, max_length=100),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=vector_dim),
    ]

    schema = CollectionSchema(
        fields=fields, description=f"AlphaVault semantic docs - {collection_name}"
    )

    # 创建集合
    client.create_collection(collection_name=collection_name, schema=schema)
    print(f"✓ 创建集合 {collection_name}")

    # 创建索引 - 需要先建立连接
    from pymilvus import connections, Collection

    # 从 client 获取连接信息
    conn_alias = f"create_index_{collection_name}"
    try:
        # 获取 MilvusClient 的连接信息
        uri = client._config.uri
        token = client._config.token

        connections.connect(alias=conn_alias, uri=uri, token=token)

        coll = Collection(name=collection_name, using=conn_alias)
        coll.create_index(
            field_name="embedding",
            index_params={
                "metric_type": "COSINE",
                "index_type": "HNSW",
                "params": {"M": 16, "efConstruction": 200},
            },
        )
        print(f"✓ 创建索引 {collection_name}.embedding (HNSW, COSINE)")
    finally:
        try:
            connections.disconnect(conn_alias)
        except Exception:
            pass


def migrate_batch_to_zilliz(
    client: Any,
    collection_name: str,
    rows: list[dict[str, Any]],
    vector_dim: int,
    dry_run: bool = False,
    max_retries: int = 3,
) -> tuple[int, int]:
    """批量迁移到 Zilliz（返回成功数和失败数，支持重试）"""
    if not rows:
        return 0, 0

    # 准备批量插入数据
    data: dict[str, list[Any]] = {
        "doc_id": [],
        "post_uid": [],
        "assertion_id": [],
        "doc_kind": [],
        "chunk_seq": [],
        "platform": [],
        "author": [],
        "created_at": [],
        "action": [],
        "action_strength": [],
        "doc_text": [],
        "content_hash": [],
        "embedding_model": [],
        "embedding": [],
    }

    success_count = 0
    error_count = 0

    for row in rows:
        try:
            # 解析 embedding
            embedding = parse_vector_text(row.get("embedding_text"))
            if len(embedding) != vector_dim:
                print(
                    f"⚠️  跳过 {row.get('doc_id')}: embedding 维度不匹配 "
                    f"(期望 {vector_dim}, 实际 {len(embedding)})"
                )
                error_count += 1
                continue

            # 填充数据
            data["doc_id"].append(str(row.get("doc_id") or ""))
            data["post_uid"].append(str(row.get("post_uid") or ""))
            data["assertion_id"].append(str(row.get("assertion_id") or ""))
            data["doc_kind"].append(str(row.get("doc_kind") or ""))
            data["chunk_seq"].append(int(row.get("chunk_seq") or 1))
            data["platform"].append(str(row.get("platform") or ""))
            data["author"].append(str(row.get("author") or "")[:255])  # 截断
            data["created_at"].append(str(row.get("created_at") or ""))
            data["action"].append(str(row.get("action") or ""))
            data["action_strength"].append(int(row.get("action_strength") or 0))
            # doc_text 截断到 65535 字符
            doc_text = str(row.get("doc_text") or "")[:65535]
            data["doc_text"].append(doc_text)
            data["content_hash"].append(str(row.get("content_hash") or ""))
            data["embedding_model"].append(str(row.get("embedding_model") or ""))
            data["embedding"].append(embedding)

            success_count += 1

        except Exception as e:
            print(f"⚠️  处理记录失败 {row.get('doc_id')}: {e}")
            error_count += 1

    # 批量插入（带重试）
    if success_count > 0 and not dry_run:
        # 转换为 list of dicts
        rows_to_insert = []
        for i in range(success_count):
            rows_to_insert.append(
                {
                    "doc_id": data["doc_id"][i],
                    "post_uid": data["post_uid"][i],
                    "assertion_id": data["assertion_id"][i],
                    "doc_kind": data["doc_kind"][i],
                    "chunk_seq": data["chunk_seq"][i],
                    "platform": data["platform"][i],
                    "author": data["author"][i],
                    "created_at": data["created_at"][i],
                    "action": data["action"][i],
                    "action_strength": data["action_strength"][i],
                    "doc_text": data["doc_text"][i],
                    "content_hash": data["content_hash"][i],
                    "embedding_model": data["embedding_model"][i],
                    "embedding": data["embedding"][i],
                }
            )

        # 使用 upsert 而不是 insert，避免重复插入（重试逻辑）
        for attempt in range(max_retries):
            try:
                client.upsert(collection_name=collection_name, data=rows_to_insert)
                break  # 成功则退出重试
            except Exception as e:
                if attempt < max_retries - 1:
                    import time

                    wait_time = 2**attempt  # 指数退避: 1s, 2s, 4s
                    print(
                        f"⚠️  upsert 失败 (尝试 {attempt + 1}/{max_retries}), {wait_time}秒后重试: {e}"
                    )
                    time.sleep(wait_time)
                else:
                    print(f"❌ 批量 upsert 最终失败: {e}")
                    error_count += success_count
                    success_count = 0

    return success_count, error_count


def migrate_schema_to_zilliz(
    client: Any,
    engine: PostgresEngine,
    schema: str,
    collection_name: str,
    config: MigrationConfig,
    migrated_doc_ids: set[str],
    stats: MigrationStats,
    limit: int = 0,
) -> None:
    """迁移单个 schema 的数据到 Zilliz（流式处理）"""
    print(f"\n{'=' * 60}")
    print(f"开始迁移 {schema}.semantic_docs → {collection_name}")
    print(f"{'=' * 60}")

    # 1. 创建集合
    if not config.dry_run:
        create_zilliz_collection(client, collection_name, config.vector_dim)

    # 2. 获取总记录数
    count_sql = f"SELECT COUNT(*) as cnt FROM {schema}.semantic_docs"
    print(f"正在查询 {schema}.semantic_docs 表...")
    with postgres_connect_autocommit(engine) as conn:
        count_rows = read_sql_rows(conn, count_sql, params={})
        total_count = count_rows[0].get("cnt", 0) if count_rows else 0

    if limit > 0:
        print(f"  表中共有 {total_count} 条记录，限制迁移前 {limit} 条用于测试...")
        total_to_process = min(limit, total_count)
    else:
        print(f"  表中共有 {total_count} 条记录，开始流式迁移...")
        total_to_process = total_count

    stats.total_count += total_to_process

    # 3. 流式读取并迁移
    pg_batch_size = 1000  # 每次从 Postgres 读取 1000 条
    zilliz_batch_size = config.batch_size  # 每次向 Zilliz 写入的批次
    offset = 0
    processed = 0

    print(f"开始流式读取，每批 {pg_batch_size} 条...")

    while processed < total_to_process:
        # 计算本次读取数量
        fetch_size = min(pg_batch_size, total_to_process - processed)

        # 从 Postgres 读取一批数据
        print(f"  [读取] offset={offset}, limit={fetch_size}...")
        rows = fetch_semantic_docs_from_postgres(
            engine, schema, limit=fetch_size, offset=offset, batch_size=pg_batch_size
        )
        print(f"  [完成] 读取到 {len(rows)} 条记录")

        if not rows:
            break

        # 过滤已迁移的记录
        rows_to_migrate = [
            row for row in rows if str(row.get("doc_id")) not in migrated_doc_ids
        ]
        skipped = len(rows) - len(rows_to_migrate)
        stats.skipped_count += skipped

        if skipped > 0:
            print(f"  [跳过] {skipped} 条已迁移的记录")

        if rows_to_migrate:
            # 分批次写入 Zilliz
            for i in range(0, len(rows_to_migrate), zilliz_batch_size):
                batch = rows_to_migrate[i : i + zilliz_batch_size]
                success, errors = migrate_batch_to_zilliz(
                    client, collection_name, batch, config.vector_dim, config.dry_run
                )
                stats.migrated_count += success
                stats.error_count += errors

                # 更新状态
                for row in batch[:success]:
                    migrated_doc_ids.add(str(row.get("doc_id")))

                # 显示进度
                already_migrated_before_start = (
                    len(migrated_doc_ids) - stats.migrated_count
                )
                total_now_migrated = len(migrated_doc_ids)
                progress_pct = (stats.migrated_count / total_to_process) * 100
                print(
                    f"  进度: {stats.migrated_count}/{total_to_process} ({progress_pct:.1f}%) "
                    f"- 本批: 成功 {success}, 失败 {errors} "
                    f"| 累计: {total_now_migrated} (含之前 {already_migrated_before_start})"
                )

                # 定期保存状态
                if (
                    stats.migrated_count % (zilliz_batch_size * 10) == 0
                    and not config.dry_run
                ):
                    save_migration_state(
                        config.state_file,
                        {"xueqiu": migrated_doc_ids, "weibo": migrated_doc_ids},
                    )

        processed += len(rows)
        offset += len(rows)

    print(f"✓ {schema} 迁移完成")


def verify_migration(
    client: Any, engine: PostgresEngine, config: MigrationConfig
) -> None:
    """验证迁移完整性"""
    print(f"\n{'=' * 60}")
    print("验证迁移结果")
    print(f"{'=' * 60}")

    for schema, collection_name in [
        (SCHEMA_XUEQIU, "alphavault_xueqiu_semantic"),
        (SCHEMA_WEIBO, "alphavault_weibo_semantic"),
    ]:
        # Postgres 记录数
        sql = f"SELECT COUNT(*) AS cnt FROM {schema}.semantic_docs"
        with postgres_connect_autocommit(engine) as conn:
            rows = read_sql_rows(conn, sql, params={})
        pg_count = rows[0].get("cnt", 0) if rows else 0

        # Zilliz 记录数（使用迭代器精确统计，避免 num_entities 缓存问题）
        try:
            # 确保集合存在
            if not client.has_collection(collection_name):
                print(f"⚠️  集合 {collection_name} 不存在")
                zilliz_count = 0
            else:
                print(f"  正在统计 {collection_name}...")
                iterator = client.query_iterator(
                    collection_name=collection_name,
                    filter="",
                    output_fields=["doc_id"],
                    batch_size=1000,
                )
                zilliz_count = 0
                while True:
                    batch = iterator.next()
                    if not batch:
                        break
                    zilliz_count += len(batch)
                iterator.close()
        except Exception as e:
            print(f"⚠️  查询 {collection_name} 失败: {e}")
            zilliz_count = 0

        # 对比
        match = "✓" if pg_count == zilliz_count else "❌"
        print(f"{match} {schema}: Postgres={pg_count}, Zilliz={zilliz_count}")


def main() -> None:
    parser = argparse.ArgumentParser(description="迁移 semantic_docs 到 Zilliz Cloud")
    parser.add_argument("--dry-run", action="store_true", help="试运行（不实际写入）")
    parser.add_argument("--verify", action="store_true", help="验证迁移结果")
    parser.add_argument("--batch-size", type=int, default=100, help="批量大小")
    parser.add_argument(
        "--limit", type=int, default=0, help="限制迁移条数（0=全部，用于测试）"
    )
    parser.add_argument(
        "--schema",
        type=str,
        choices=["xueqiu", "weibo", "all"],
        default="all",
        help="指定迁移的 schema：xueqiu/weibo/all（默认 all）",
    )
    args = parser.parse_args()

    # 0. 加载 .env 文件
    load_dotenv_if_present()

    # 1. 加载配置
    zilliz_uri = os.getenv("ZILLIZ_CLOUD_URI")
    zilliz_token = os.getenv("ZILLIZ_CLOUD_TOKEN")

    if not zilliz_uri or not zilliz_token:
        print("❌ 请设置环境变量: ZILLIZ_CLOUD_URI, ZILLIZ_CLOUD_TOKEN")
        print("\n获取方式:")
        print("  1. 登录 https://cloud.zilliz.com/")
        print("  2. 创建免费集群（Free Tier）")
        print("  3. 复制 Cluster URI 和 API Key")
        print("\n示例:")
        print(
            '  export ZILLIZ_CLOUD_URI="https://in03-xxx.api.gcp-us-west1.zillizcloud.com"'
        )
        print('  export ZILLIZ_CLOUD_TOKEN="your-api-key"')
        sys.exit(1)

    config = MigrationConfig(
        zilliz_uri=zilliz_uri,
        zilliz_token=zilliz_token,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )

    # 2. 连接数据库
    try:
        from pymilvus import MilvusClient
    except ImportError:
        print("❌ 请先安装依赖: uv pip install pymilvus")
        sys.exit(1)

    print(f"连接 Zilliz Cloud: {config.zilliz_uri}")
    try:
        client = MilvusClient(
            uri=config.zilliz_uri, token=config.zilliz_token, timeout=10
        )
        # 测试连接
        client.list_collections()
        print("✓ Zilliz Cloud 连接成功")
    except Exception as e:
        print(f"❌ 连接 Zilliz Cloud 失败: {e}")
        print("\n可能的原因:")
        print("  1. API Token 已过期或无效")
        print("  2. Cluster URI 不正确")
        print("  3. 集群已被删除或暂停")
        print("  4. 网络连接问题")
        print("\n解决方法:")
        print("  1. 登录 https://cloud.zilliz.com/ 检查集群状态")
        print("  2. 重新生成 API Key")
        print("  3. 确认 Cluster URI 格式正确（应该是 https://xxx.cloud.zilliz.com）")
        sys.exit(1)

    print("连接 Postgres...")
    # 获取 Postgres DSN（xueqiu 和 weibo 使用同一个数据库）
    source = require_postgres_source_from_env("xueqiu")
    engine = ensure_postgres_engine(source.dsn)

    # 3. 仅验证模式
    if args.verify:
        verify_migration(client, engine, config)
        return

    # 4. 迁移模式
    stats = MigrationStats(start_time=time.time())
    state = load_migration_state(config.state_file)

    try:
        # 根据参数决定迁移哪个 schema
        if args.schema in ("xueqiu", "all"):
            migrate_schema_to_zilliz(
                client,
                engine,
                SCHEMA_XUEQIU,
                "alphavault_xueqiu_semantic",
                config,
                state["xueqiu"],
                stats,
                limit=args.limit,
            )

        if args.schema in ("weibo", "all"):
            migrate_schema_to_zilliz(
                client,
                engine,
                SCHEMA_WEIBO,
                "alphavault_weibo_semantic",
                config,
                state["weibo"],
                stats,
                limit=args.limit,
            )

    finally:
        # 保存最终状态
        if not config.dry_run:
            save_migration_state(config.state_file, state)

    # 5. 输出统计
    print(f"\n{'=' * 60}")
    print(stats.report())
    print(f"{'=' * 60}")

    if not config.dry_run:
        print(f"\n✓ 迁移完成！状态已保存到 {config.state_file}")
        print("\n下一步:")
        print("  1. 运行验证: uv run python scripts/migrate_to_zilliz.py --verify")
        print("  2. 更新代码，将语义搜索从 Postgres 切换到 Zilliz")
        print("  3. 测试通过后，删除 Supabase 的 semantic_docs 表")


if __name__ == "__main__":
    main()
