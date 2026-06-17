#!/usr/bin/env python3
"""
监控 Zilliz Cloud 数据量和健康状态

用法:
    uv run python scripts/monitor_zilliz.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from alphavault.db.postgres_db import (
    ensure_postgres_engine,
    postgres_connect_autocommit,
)
from alphavault.db.postgres_env import require_postgres_source_from_env
from alphavault.db.zilliz_client import (
    get_migration_mode,
    get_zilliz_client,
    should_write_to_postgres,
    should_write_to_zilliz,
)
from alphavault.db.zilliz_retry_queue import get_zilliz_retry_queue_stats


def format_number(num: int) -> str:
    """格式化数字（添加千分位）"""
    return f"{num:,}"


def check_postgres_stats() -> dict[str, int]:
    """查询 Postgres semantic_docs 统计"""
    source = require_postgres_source_from_env("xueqiu")
    engine = ensure_postgres_engine(source.dsn)
    stats = {}

    with postgres_connect_autocommit(engine) as conn:
        for schema in ["xueqiu", "weibo"]:
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {schema}.semantic_docs")
                row = result.fetchone()
                count = row[0] if row else 0
                stats[schema] = count
            except Exception as e:
                print(f"⚠️  Postgres {schema} 查询失败: {e}")
                stats[schema] = -1

    return stats


def check_zilliz_stats() -> dict[str, dict[str, bool | int | str]]:
    """查询 Zilliz Cloud 统计"""
    try:
        client = get_zilliz_client()
    except Exception as e:
        print(f"❌ Zilliz 连接失败: {e}")
        return {}

    stats: dict[str, dict[str, bool | int | str]] = {}
    collections = {
        "xueqiu": "alphavault_xueqiu_semantic",
        "weibo": "alphavault_weibo_semantic",
    }

    for schema, collection_name in collections.items():
        try:
            # 查询集合是否存在
            if not client.has_collection(collection_name):
                print(f"⚠️  集合 {collection_name} 不存在")
                stats[schema] = {"exists": False, "row_count": 0}
                continue

            # 查询统计信息
            collection_stats = client.get_collection_stats(collection_name)
            stats[schema] = {
                "exists": True,
                "row_count": collection_stats.get("row_count", 0),
            }
        except Exception as e:
            print(f"⚠️  Zilliz {schema} 查询失败: {e}")
            stats[schema] = {"exists": False, "error": str(e), "row_count": 0}

    return stats


def calculate_storage_usage(row_count: int, vector_dim: int = 2048) -> str:
    """估算存储占用（MB）"""
    # 估算：每个 vector 约 4-5 KB（包含 embedding + 元数据）
    bytes_per_row = 4.5 * 1024
    total_mb = (row_count * bytes_per_row) / (1024 * 1024)
    return f"{total_mb:.1f} MB"


def main() -> None:
    print("=" * 70)
    print("Zilliz Cloud 监控报告")
    print("=" * 70)

    # 1. 显示当前模式
    mode = get_migration_mode()
    write_pg = should_write_to_postgres()
    write_zilliz = should_write_to_zilliz()

    print(f"\n📋 当前迁移模式: {mode}")
    print(f"   - 写入 Postgres: {'✓' if write_pg else '✗'}")
    print(f"   - 写入 Zilliz: {'✓' if write_zilliz else '✗'}")

    # 2. Postgres 统计
    if write_pg:
        print(f"\n{'=' * 70}")
        print("📊 Postgres 统计")
        print(f"{'=' * 70}")
        pg_stats = check_postgres_stats()
        total_pg = 0
        for schema, count in pg_stats.items():
            if count >= 0:
                print(
                    f"   {schema}.semantic_docs: {format_number(count)} 条 "
                    f"({calculate_storage_usage(count)})"
                )
                total_pg += count
            else:
                print(f"   {schema}.semantic_docs: 查询失败")
        if total_pg > 0:
            print(
                f"\n   总计: {format_number(total_pg)} 条 ({calculate_storage_usage(total_pg)})"
            )

    # 3. Zilliz 统计
    if write_zilliz:
        print(f"\n{'=' * 70}")
        print("☁️  Zilliz Cloud 统计")
        print(f"{'=' * 70}")
        zilliz_stats = check_zilliz_stats()
        total_zilliz = 0
        for schema, stats in zilliz_stats.items():
            collection_name = f"alphavault_{schema}_semantic"
            if not stats.get("exists"):
                print(f"   {collection_name}: 不存在或查询失败")
            else:
                count_raw = stats.get("row_count", 0)
                count = int(count_raw) if isinstance(count_raw, (int, str)) else 0
                print(
                    f"   {collection_name}: {format_number(count)} 条 "
                    f"({calculate_storage_usage(count)})"
                )
                total_zilliz += count

        if total_zilliz > 0:
            print(
                f"\n   总计: {format_number(total_zilliz)} 条 ({calculate_storage_usage(total_zilliz)})"
            )

            # 计算距离 5GB 上限的剩余空间
            total_mb = (total_zilliz * 4.5 * 1024) / (1024 * 1024)
            free_mb = 5 * 1024 - total_mb
            usage_percent = (total_mb / (5 * 1024)) * 100
            print(
                f"\n   免费额度使用: {usage_percent:.1f}% (剩余 {free_mb:.1f} MB / 5120 MB)"
            )

            if usage_percent > 80:
                print("   ⚠️  警告: 已使用超过 80%，建议配置定期清理")

    # 4. 一致性检查（dual_write 模式）
    if mode == "dual_write" and write_pg and write_zilliz:
        print(f"\n{'=' * 70}")
        print("🔍 一致性检查（Postgres vs Zilliz）")
        print(f"{'=' * 70}")
        pg_stats = check_postgres_stats()
        zilliz_stats = check_zilliz_stats()

        all_consistent = True
        for schema in ["xueqiu", "weibo"]:
            pg_count = pg_stats.get(schema, -1)
            zilliz_count_raw = zilliz_stats.get(schema, {}).get("row_count", -1)
            zilliz_count = (
                int(zilliz_count_raw)
                if isinstance(zilliz_count_raw, (int, str))
                else -1
            )

            if pg_count < 0 or zilliz_count < 0:
                print(f"   {schema}: 无法对比（查询失败）")
                continue

            diff = abs(pg_count - zilliz_count)
            if diff == 0:
                print(f"   {schema}: ✓ 一致 ({format_number(pg_count)} 条)")
            else:
                print(
                    f"   {schema}: ⚠️  不一致 "
                    f"(Postgres: {format_number(pg_count)}, "
                    f"Zilliz: {format_number(zilliz_count)}, "
                    f"差异: {format_number(diff)})"
                )
                all_consistent = False

        if all_consistent:
            print("\n   ✓ 所有 schema 数据一致")
        else:
            print("\n   ⚠️  发现数据不一致，建议检查日志")

    # 5. Zilliz 重试队列统计（新增）
    if write_zilliz:
        print(f"\n{'=' * 70}")
        print("🔄 Zilliz 重试队列")
        print(f"{'=' * 70}")

        redis_url = os.getenv("REDIS_URL")
        if redis_url:
            try:
                import redis

                redis_client = redis.from_url(redis_url, decode_responses=True)
                retry_stats = get_zilliz_retry_queue_stats(redis_client)

                retry_queue_len = retry_stats.get("retry_queue_len", 0)
                failed_queue_len = retry_stats.get("failed_queue_len", 0)

                print(f"   待重试: {format_number(retry_queue_len)} 个任务")
                print(f"   已失败: {format_number(failed_queue_len)} 个任务")

                if retry_queue_len > 100:
                    print(f"\n   ⚠️  警告: 重试队列积压 {retry_queue_len} 个任务")
                    print("       可能原因: Zilliz 持续故障或网络问题")

                if failed_queue_len > 0:
                    print(f"\n   ⚠️  注意: 有 {failed_queue_len} 个任务最终失败")
                    print(
                        "       运行修复脚本: uv run python scripts/fix_zilliz_failed_tasks.py --list"
                    )

            except Exception as e:
                print(f"   ⚠️  无法查询重试队列: {e}")
        else:
            print("   Redis 未配置，无法查询重试队列")

    print(f"\n{'=' * 70}")
    print("监控完成")
    print(f"{'=' * 70}\n")


if __name__ == "__main__":
    main()
