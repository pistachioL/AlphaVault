from __future__ import annotations

from dataclasses import replace
import time

from alphavault.ai.analyze import (
    format_llm_error_one_line,
)
from alphavault.ai.topic_prompt_v4 import TOPIC_PROMPT_VERSION
from alphavault.db.turso_db import TursoEngine
from alphavault.db.turso_queue import (
    CloudPost,
    mark_ai_error,
)
from alphavault.rss.utils import RateLimiter, now_str
from alphavault.worker.backoff import backoff_seconds
from alphavault.worker.post_processor_topic_prompt_v4 import (
    process_one_post_uid_topic_prompt_v4,
)
from alphavault.worker.runtime_models import LLMConfig


def process_one_post_uid(
    *,
    engine: TursoEngine,
    post_uid: str,
    config: LLMConfig,
    limiter: RateLimiter,
    prefetched_post: CloudPost | None = None,
    prefetched_recent: list[dict[str, object]] | None = None,
    source_name: str = "",
    outbox_source: str = "",
) -> bool:
    resolved_config = config
    if str(config.prompt_version or "").strip() != TOPIC_PROMPT_VERSION:
        resolved_config = replace(config, prompt_version=TOPIC_PROMPT_VERSION)
    try:
        return process_one_post_uid_topic_prompt_v4(
            engine=engine,
            post_uid=post_uid,
            config=resolved_config,
            limiter=limiter,
            prefetched_post=prefetched_post,
            prefetched_recent=prefetched_recent,
            source_name=str(source_name or "").strip(),
            outbox_source=outbox_source,
        )
    except Exception as err:
        base_url_for_log = (resolved_config.base_url or "").strip()
        if base_url_for_log:
            base_url_for_log = base_url_for_log.split("?", 1)[0].split("#", 1)[0]
            base_url_for_log = base_url_for_log[:220]
        ctx = (
            f" cfg_model={resolved_config.model}"
            f" api_mode={resolved_config.api_mode}"
            f" stream={1 if resolved_config.ai_stream else 0}"
            f" base_url={base_url_for_log or '(empty)'}"
        )
        msg = f"ai:{format_llm_error_one_line(err, limit=700)}{ctx}"
        now_epoch = int(time.time())
        retry_count = int(getattr(prefetched_post, "ai_retry_count", 1) or 1)
        next_retry = now_epoch + backoff_seconds(retry_count)
        try:
            mark_ai_error(
                engine,
                post_uid=post_uid,
                error=msg,
                next_retry_at=next_retry,
                archived_at=now_str(),
            )
        except Exception as mark_err:
            if config.verbose:
                print(
                    f"[llm] mark_error_failed {post_uid} {type(mark_err).__name__}: {mark_err}",
                    flush=True,
                )
        print(f"[llm] error {post_uid} {msg}", flush=True)
        return False


__all__ = [
    "backoff_seconds",
    "process_one_post_uid",
    "process_one_post_uid_topic_prompt_v4",
]
