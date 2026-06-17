"""
Zilliz Cloud 写入重试队列（使用 Redis Sorted Set）

当 Zilliz 写入/删除失败时，将任务放入 Redis Sorted Set 延迟重试队列。
集成到 worker 主循环，每次调度时自动处理到期任务。

架构对齐：
- 与现有 AI 队列重试机制一致
- 使用 Sorted Set 实现延迟重试 + 指数退避
- 集成到 scheduler.py，无需独立 cron job
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from redis import Redis

from alphavault.db.zilliz_client import replace_semantic_docs_in_zilliz
from alphavault.logging_config import get_logger

logger = get_logger(__name__)

# Redis 队列 key
ZILLIZ_RETRY_SORTED_SET = "alphavault:zilliz_retry"  # Sorted Set (score = 重试时间戳)
ZILLIZ_FAILED_LIST = "alphavault:zilliz_failed"  # List (最终失败队列)

# 重试配置
ZILLIZ_RETRY_MAX_COUNT = 10  # 最多重试 10 次
ZILLIZ_RETRY_MIN_BACKOFF = 60  # 最小退避 60 秒
ZILLIZ_RETRY_MAX_BACKOFF = 3600  # 最大退避 1 小时


def _calculate_backoff_seconds(retry_count: int) -> int:
    """计算指数退避时间（秒）"""
    # 指数退避：60, 120, 240, 480, 960, 1920, 3600, 3600, ...
    backoff = ZILLIZ_RETRY_MIN_BACKOFF * (2**retry_count)
    return min(backoff, ZILLIZ_RETRY_MAX_BACKOFF)


ZillizOperation = Literal["replace", "delete"]


def enqueue_zilliz_retry(
    redis_client: Redis | Any,
    *,
    operation: ZillizOperation,
    schema: str,
    post_uid: str,
    rows: list[dict[str, Any]],
    retry_count: int = 0,
    error: str = "",
) -> None:
    """
    将失败的 Zilliz 写入/删除任务放入 Redis Sorted Set 延迟重试队列

    Args:
        redis_client: Redis 客户端
        operation: 操作类型（"replace" 或 "delete"）
        schema: 数据库 schema (xueqiu/weibo)
        post_uid: 帖子 UID
        rows: 要写入的数据行（delete 操作传空列表）
        retry_count: 当前重试次数
        error: 失败原因
    """
    if not redis_client:
        logger.warning("zilliz_retry redis_unavailable post_uid=%s", post_uid)
        return

    task = {
        "operation": operation,
        "schema": schema,
        "post_uid": post_uid,
        "rows": rows,
        "retry_count": retry_count,
        "enqueued_at": time.time(),
        "last_error": str(error)[:500],
    }

    try:
        # 计算下次重试时间（指数退避）
        backoff_seconds = _calculate_backoff_seconds(retry_count)
        next_retry_at = int(time.time()) + backoff_seconds

        # 放入 Sorted Set（score = 重试时间戳）
        redis_client.zadd(
            ZILLIZ_RETRY_SORTED_SET,
            {json.dumps(task, ensure_ascii=False): next_retry_at},
        )
        logger.info(
            "zilliz_retry enqueued post_uid=%s retry_count=%d next_retry_seconds=%d",
            post_uid,
            retry_count,
            backoff_seconds,
        )
    except Exception as e:
        logger.warning("zilliz_retry enqueue_error post_uid=%s: %s", post_uid, e)


def process_zilliz_retry_queue(
    redis_client: Redis | Any, *, max_batch_size: int = 50
) -> dict[str, int]:
    """
    处理 Zilliz 重试队列中到期的任务

    由 worker 主循环调用，每次调度 AI 任务前执行。
    通过任务的 operation 字段区分写入（"replace"）和删除（"delete"）。

    Args:
        redis_client: Redis 客户端
        max_batch_size: 每次最多处理的任务数

    Returns:
        统计信息 {"processed": 已处理, "success": 成功, "retry": 重试, "failed": 失败}
    """
    if not redis_client:
        return {"processed": 0, "success": 0, "retry": 0, "failed": 0}

    stats = {"processed": 0, "success": 0, "retry": 0, "failed": 0}

    try:
        now = int(time.time())

        # 1. 获取所有到期任务（score <= now）
        due_tasks = redis_client.zrangebyscore(
            ZILLIZ_RETRY_SORTED_SET, 0, now, start=0, num=max_batch_size
        )

        if not due_tasks:
            return stats

        logger.info(
            "zilliz_retry processing queue_len=%d batch_size=%d",
            len(due_tasks),
            max_batch_size,
        )

        # 2. 批量处理到期任务
        for task_json in due_tasks:
            stats["processed"] += 1

            try:
                task = json.loads(task_json)
                schema = task["schema"]
                post_uid = task["post_uid"]
                rows = task["rows"]
                retry_count = task.get("retry_count", 0)

                # 重试写入或删除 Zilliz
                operation = task.get("operation", "replace" if rows else "delete")
                try:
                    if operation == "replace":
                        replace_semantic_docs_in_zilliz(
                            schema, post_uid=post_uid, rows=rows
                        )
                    else:
                        from alphavault.db.zilliz_client import (
                            delete_semantic_docs_by_post_uid_in_zilliz,
                        )

                        delete_semantic_docs_by_post_uid_in_zilliz(
                            schema, post_uid=post_uid
                        )

                    # 成功：从 Sorted Set 删除
                    redis_client.zrem(ZILLIZ_RETRY_SORTED_SET, task_json)
                    stats["success"] += 1
                    logger.info(
                        "zilliz_retry success operation=%s post_uid=%s retry_count=%d",
                        operation,
                        post_uid,
                        retry_count,
                    )

                except Exception as e:
                    # 重试失败：增加重试次数
                    task["retry_count"] = retry_count + 1
                    task["last_error"] = str(e)[:500]

                    if task["retry_count"] < ZILLIZ_RETRY_MAX_COUNT:
                        # 未超过最大次数：更新时间戳，继续重试
                        stats["retry"] += 1
                        backoff_seconds = _calculate_backoff_seconds(
                            task["retry_count"]
                        )
                        next_retry_at = int(time.time()) + backoff_seconds

                        # 删除旧的，添加新的（更新 score）
                        redis_client.zrem(ZILLIZ_RETRY_SORTED_SET, task_json)
                        redis_client.zadd(
                            ZILLIZ_RETRY_SORTED_SET,
                            {json.dumps(task, ensure_ascii=False): next_retry_at},
                        )
                        logger.warning(
                            "zilliz_retry retry_later post_uid=%s retry_count=%d/%d next_retry_seconds=%d error=%s",
                            post_uid,
                            task["retry_count"],
                            ZILLIZ_RETRY_MAX_COUNT,
                            backoff_seconds,
                            str(e)[:100],
                        )
                    else:
                        # 超过最大次数：移入失败队列
                        stats["failed"] += 1
                        task["failed_at"] = time.time()
                        task["final_error"] = task["last_error"]

                        redis_client.zrem(ZILLIZ_RETRY_SORTED_SET, task_json)
                        redis_client.rpush(
                            ZILLIZ_FAILED_LIST, json.dumps(task, ensure_ascii=False)
                        )
                        logger.error(
                            "zilliz_retry final_failure post_uid=%s retry_count=%d error=%s",
                            post_uid,
                            task["retry_count"],
                            task["final_error"][:100],
                        )

            except Exception as e:
                logger.warning("zilliz_retry process_task_error: %s", e)
                stats["failed"] += 1

        if stats["processed"] > 0:
            logger.info(
                "zilliz_retry batch_done processed=%d success=%d retry=%d failed=%d",
                stats["processed"],
                stats["success"],
                stats["retry"],
                stats["failed"],
            )

    except Exception as e:
        logger.error("zilliz_retry queue_processing_error: %s", e)

    return stats


def get_zilliz_retry_queue_stats(redis_client: Redis | Any) -> dict[str, int]:
    """
    获取 Zilliz 重试队列统计信息

    Args:
        redis_client: Redis 客户端

    Returns:
        {"retry_queue_len": 待重试数量, "failed_queue_len": 失败数量}
    """
    if not redis_client:
        return {"retry_queue_len": 0, "failed_queue_len": 0}

    try:
        return {
            "retry_queue_len": redis_client.zcard(ZILLIZ_RETRY_SORTED_SET),
            "failed_queue_len": redis_client.llen(ZILLIZ_FAILED_LIST),
        }
    except Exception as e:
        logger.warning("zilliz_retry get_stats_error: %s", e)
        return {"retry_queue_len": 0, "failed_queue_len": 0}


def get_failed_tasks(
    redis_client: Redis | Any, limit: int = 10
) -> list[dict[str, Any]]:
    """
    获取失败的任务列表（用于手动修复）

    Args:
        redis_client: Redis 客户端
        limit: 返回数量限制

    Returns:
        失败任务列表
    """
    if not redis_client:
        return []

    try:
        failed_tasks = []
        for i in range(min(limit, redis_client.llen(ZILLIZ_FAILED_LIST))):
            task_json = redis_client.lindex(ZILLIZ_FAILED_LIST, i)
            if task_json:
                failed_tasks.append(json.loads(task_json))
        return failed_tasks
    except Exception as e:
        logger.warning("zilliz_retry get_failed_tasks_error: %s", e)
        return []


def clear_failed_queue(redis_client: Any) -> int:
    """
    清空失败队列（手动修复后调用）

    Returns:
        清空的任务数量
    """
    if not redis_client:
        return 0

    try:
        count = redis_client.llen(ZILLIZ_FAILED_LIST)
        redis_client.delete(ZILLIZ_FAILED_LIST)
        logger.info("zilliz_retry failed_queue_cleared count=%d", count)
        return count
    except Exception as e:
        logger.warning("zilliz_retry clear_failed_queue_error: %s", e)
        return 0


__all__ = [
    "enqueue_zilliz_retry",
    "process_zilliz_retry_queue",
    "get_zilliz_retry_queue_stats",
    "get_failed_tasks",
    "clear_failed_queue",
    "ZILLIZ_RETRY_SORTED_SET",
    "ZILLIZ_FAILED_LIST",
]
