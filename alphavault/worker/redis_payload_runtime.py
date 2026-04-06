from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from alphavault.db.turso_queue import (
    CloudPost,
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
    redis_ai_release_lease,
    redis_ai_try_claim_lease,
)
from alphavault.worker.runtime_models import (
    LLMConfig,
    _parse_int_or_default,
)


_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


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
    lease_seconds: int,
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
        redis_ai_try_claim_lease_fn=redis_ai_try_claim_lease,
        process_one_post_uid_fn=process_one_post_uid,
        redis_ai_release_lease_fn=redis_ai_release_lease,
        redis_ai_ack_and_cleanup_fn=redis_ai_ack_and_cleanup,
        redis_ai_push_delayed_fn=redis_ai_push_delayed,
        redis_ai_ack_processing_fn=redis_ai_ack_processing,
        payload_retry_count_fn=_payload_retry_count,
        backoff_seconds_fn=backoff_seconds,
        now_epoch_fn=lambda: int(time.time()),
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        lease_seconds=max(1, int(lease_seconds)),
    )


__all__ = [
    "process_one_redis_payload",
]
