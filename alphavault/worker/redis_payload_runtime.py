from __future__ import annotations

import time
from typing import Any

from alphavault.db.turso_queue import (
    CloudPost,
    mark_post_failed,
)
from alphavault.db.postgres_db import PostgresEngine
from alphavault.rss.utils import RateLimiter, now_str
from alphavault.worker import ai_processor
from alphavault.worker.backoff import backoff_seconds
from alphavault.worker.post_processor import process_one_post_uid
from alphavault.worker.redis_stream_queue import (
    redis_ai_ack,
    redis_ai_ack_and_clear_dedup,
    redis_ai_ack_and_push_retry,
    redis_ai_push_retry,
)
from alphavault.worker.runtime_models import (
    LLMConfig,
    _parse_int_or_default,
)


_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def process_one_redis_payload(
    *,
    engine: PostgresEngine,
    payload: dict[str, object],
    message_id: str,
    redis_client: Any,
    redis_queue_key: str,
    source_name: str = "",
    config: LLMConfig,
    limiter: RateLimiter,
    verbose: bool,
    max_retry_count: int,
) -> None:
    def _payload_retry_count(inner_payload: dict[str, object]) -> int:
        return ai_processor.payload_retry_count(
            inner_payload,
            parse_int_or_default_fn=_parse_int_or_default,
        )

    def _mark_post_failed(**kwargs) -> None:  # type: ignore[no-untyped-def]
        mark_post_failed(
            kwargs["engine"],
            post_uid=str(kwargs["post_uid"] or "").strip(),
            model=str(kwargs["model"] or "").strip(),
            prompt_version=str(kwargs["prompt_version"] or "").strip(),
            processed_at=now_str(),
            archived_at=now_str(),
            prefetched_post=kwargs.get("prefetched_post"),
            prefetched_ingested_at=int(time.time()),
        )

    def _payload_to_cloud_post(inner_payload: dict[str, object]) -> CloudPost | None:
        cloud_post = ai_processor.payload_to_cloud_post(
            inner_payload,
            cloud_post_cls=CloudPost,
            now_str_fn=now_str,
            payload_retry_count_fn=_payload_retry_count,
        )
        if cloud_post is None:
            return None
        return cloud_post

    ai_processor.process_one_redis_payload(
        engine=engine,
        payload=payload,
        message_id=message_id,
        redis_client=redis_client,
        redis_queue_key=redis_queue_key,
        source_name=str(source_name or "").strip(),
        config=config,
        limiter=limiter,
        verbose=bool(verbose),
        payload_to_cloud_post_fn=_payload_to_cloud_post,
        process_one_post_uid_fn=process_one_post_uid,
        mark_post_failed_fn=_mark_post_failed,
        redis_ai_push_retry_fn=redis_ai_push_retry,
        redis_ai_ack_fn=redis_ai_ack,
        redis_ai_ack_and_clear_dedup_fn=redis_ai_ack_and_clear_dedup,
        redis_ai_ack_and_push_retry_fn=redis_ai_ack_and_push_retry,
        payload_retry_count_fn=_payload_retry_count,
        backoff_seconds_fn=backoff_seconds,
        now_epoch_fn=lambda: int(time.time()),
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        max_retry_count=max(0, int(max_retry_count)),
    )


__all__ = [
    "process_one_redis_payload",
]
