from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from alphavault.db.turso_queue import (
    CloudPost,
    load_recent_posts_by_author,
    try_mark_ai_running,
)
from alphavault.db.turso_db import TursoEngine
from alphavault.rss.utils import RateLimiter, now_str
from alphavault.worker import ai_processor
from alphavault.worker.backoff import backoff_seconds
from alphavault.worker.post_processor import process_one_post_uid
from alphavault.worker.redis_queue import (
    redis_ai_ack_and_cleanup,
    redis_ai_ack_processing,
    redis_ai_push_delayed,
)
from alphavault.worker.runtime_cache import AuthorRecentLocalCache
from alphavault.worker.runtime_models import (
    LLMConfig,
    _parse_int_or_default,
)


_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
AUTHOR_RECENT_CONTEXT_LIMIT = 200

_author_recent_local_cache = AuthorRecentLocalCache()


def process_one_redis_payload(
    *,
    engine: TursoEngine,
    payload: dict[str, object],
    processing_msg: str,
    redis_client: Any,
    redis_queue_key: str,
    source_name: str = "",
    spool_dir: Path,
    config: LLMConfig,
    limiter: RateLimiter,
    verbose: bool,
) -> None:
    def _payload_retry_count(inner_payload: dict[str, object]) -> int:
        return ai_processor.payload_retry_count(
            inner_payload,
            parse_int_or_default_fn=_parse_int_or_default,
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
        processing_msg=processing_msg,
        redis_client=redis_client,
        redis_queue_key=redis_queue_key,
        source_name=str(source_name or "").strip(),
        spool_dir=spool_dir,
        config=config,
        limiter=limiter,
        verbose=bool(verbose),
        payload_to_cloud_post_fn=_payload_to_cloud_post,
        author_recent_local_cache_get_fn=_author_recent_local_cache.get,
        author_recent_local_cache_set_fn=_author_recent_local_cache.set,
        load_recent_posts_by_author_fn=load_recent_posts_by_author,
        try_mark_ai_running_fn=try_mark_ai_running,
        process_one_post_uid_fn=process_one_post_uid,
        redis_ai_ack_and_cleanup_fn=redis_ai_ack_and_cleanup,
        redis_ai_push_delayed_fn=redis_ai_push_delayed,
        redis_ai_ack_processing_fn=redis_ai_ack_processing,
        payload_retry_count_fn=_payload_retry_count,
        build_author_recent_payload_fn=ai_processor.build_author_recent_payload,
        backoff_seconds_fn=backoff_seconds,
        now_epoch_fn=lambda: int(time.time()),
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        author_recent_context_limit=int(AUTHOR_RECENT_CONTEXT_LIMIT),
    )


__all__ = [
    "AUTHOR_RECENT_CONTEXT_LIMIT",
    "process_one_redis_payload",
]
