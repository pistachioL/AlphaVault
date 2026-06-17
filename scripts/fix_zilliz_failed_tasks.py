#!/usr/bin/env python3
"""
修复 Zilliz Cloud 失败队列中的任务

当 Zilliz 重试超过 10 次后，任务会进入失败队列。
本脚本用于查看和手动重试这些失败任务。

用法:
    # 查看失败任务列表
    uv run python scripts/fix_zilliz_failed_tasks.py --list

    # 查看详细信息
    uv run python scripts/fix_zilliz_failed_tasks.py --list --limit 50

    # 重试所有失败任务
    uv run python scripts/fix_zilliz_failed_tasks.py --retry-all

    # 重试单个任务
    uv run python scripts/fix_zilliz_failed_tasks.py --retry-one post_uid_123

    # 清空失败队列（已修复后）
    uv run python scripts/fix_zilliz_failed_tasks.py --clear
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from alphavault.db.zilliz_client import replace_semantic_docs_in_zilliz
from alphavault.db.zilliz_retry_queue import (
    clear_failed_queue,
    get_failed_tasks,
)


def list_failed_tasks(redis_client, limit: int = 10) -> None:
    """列出失败任务"""
    tasks = get_failed_tasks(redis_client, limit=limit)

    if not tasks:
        print("✓ 失败队列为空")
        return

    print(f"失败队列: {len(tasks)} 个任务\n")
    print("=" * 80)

    for i, task in enumerate(tasks, 1):
        print(f"\n{i}. post_uid: {task.get('post_uid')}")
        print(f"   schema: {task.get('schema')}")
        print(f"   重试次数: {task.get('retry_count', 0)}")
        print(f"   入队时间: {task.get('enqueued_at', 'N/A')}")
        print(f"   失败时间: {task.get('failed_at', 'N/A')}")
        print(f"   数据行数: {len(task.get('rows', []))}")
        final_error = task.get("final_error", "N/A")
        print(f"   最后错误: {final_error[:200]}...")

    print("\n" + "=" * 80)


def retry_one_task(redis_client, post_uid: str) -> bool:
    """重试单个任务"""
    tasks = get_failed_tasks(redis_client, limit=1000)

    for task in tasks:
        if task.get("post_uid") == post_uid:
            try:
                print(f"重试任务: {post_uid}")
                replace_semantic_docs_in_zilliz(
                    task["schema"], post_uid=task["post_uid"], rows=task["rows"]
                )
                print(f"✓ 重试成功: {post_uid}")
                return True
            except Exception as e:
                print(f"✗ 重试失败: {post_uid}")
                print(f"   错误: {e}")
                return False

    print(f"⚠️  未找到任务: {post_uid}")
    return False


def retry_all_tasks(redis_client) -> None:
    """重试所有失败任务"""
    tasks = get_failed_tasks(redis_client, limit=1000)

    if not tasks:
        print("✓ 失败队列为空，无需重试")
        return

    print(f"开始重试 {len(tasks)} 个失败任务...\n")

    success = 0
    failed = 0

    for task in tasks:
        post_uid = task.get("post_uid")
        if not post_uid:
            print("⚠️  跳过无效任务（缺少 post_uid）")
            failed += 1
            continue
        try:
            replace_semantic_docs_in_zilliz(
                task["schema"], post_uid=str(post_uid), rows=task["rows"]
            )
            success += 1
            print(f"✓ {post_uid}")
        except Exception as e:
            failed += 1
            print(f"✗ {post_uid}: {str(e)[:100]}")

    print(f"\n重试结果: 成功 {success}, 失败 {failed}")

    if success > 0 and failed == 0:
        print("\n所有任务重试成功！")
        print("是否清空失败队列？(y/n): ", end="")
        if input().lower() == "y":
            count = clear_failed_queue(redis_client)
            print(f"已清空失败队列: {count} 个任务")
    elif success > 0:
        print(f"\n部分任务重试成功（{success}/{len(tasks)}）")
        print("建议手动检查失败任务")


def main() -> None:
    parser = argparse.ArgumentParser(description="修复 Zilliz 失败队列")
    parser.add_argument("--list", action="store_true", help="列出失败任务")
    parser.add_argument("--limit", type=int, default=10, help="列出的最大任务数")
    parser.add_argument("--retry-all", action="store_true", help="重试所有失败任务")
    parser.add_argument("--retry-one", type=str, help="重试单个任务（指定 post_uid）")
    parser.add_argument("--clear", action="store_true", help="清空失败队列")
    args = parser.parse_args()

    # 获取 Redis 客户端
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        print("❌ Redis not configured (REDIS_URL not set)")
        print("\n请在 .env 文件中配置:")
        print("REDIS_URL=redis://localhost:6379")
        sys.exit(1)

    try:
        import redis

        redis_client = redis.from_url(redis_url, decode_responses=True)
        # 测试连接
        redis_client.ping()
    except Exception as e:
        print(f"❌ Redis 连接失败: {e}")
        sys.exit(1)

    # 执行操作
    if args.list:
        list_failed_tasks(redis_client, limit=args.limit)

    elif args.retry_all:
        retry_all_tasks(redis_client)

    elif args.retry_one:
        retry_one_task(redis_client, args.retry_one)

    elif args.clear:
        print("⚠️  确认清空失败队列？所有失败任务将被删除。(y/n): ", end="")
        if input().lower() == "y":
            count = clear_failed_queue(redis_client)
            print(f"已清空失败队列: {count} 个任务")
        else:
            print("已取消")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
