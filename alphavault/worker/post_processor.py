from __future__ import annotations

from dataclasses import replace

from alphavault.logging_config import get_logger
from alphavault.ai.analyze import (
    format_llm_error_one_line,
)
from alphavault.ai.topic_prompt_v4 import TOPIC_PROMPT_VERSION
from alphavault.db.postgres_db import PostgresEngine
from alphavault.db.turso_queue import CloudPost
from alphavault.rss.utils import RateLimiter
from alphavault.worker.post_processor_topic_prompt_v4 import (
    process_one_post_uid_topic_prompt_v4,
)
from alphavault.worker.runtime_models import LLMConfig

logger = get_logger(__name__)


def process_one_post_uid(
    *,
    engine: PostgresEngine,
    post_uid: str,
    config: LLMConfig,
    limiter: RateLimiter,
    prefetched_post: CloudPost | None = None,
    prefetched_recent: list[dict[str, object]] | None = None,
    source_name: str = "",
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
        logger.error("[llm] error %s %s", post_uid, msg)
        return False


__all__ = [
    "process_one_post_uid",
    "process_one_post_uid_topic_prompt_v4",
]
