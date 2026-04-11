from __future__ import annotations

import logging
import threading
import time
from typing import Any

from alphavault.db.source_queue import (
    CloudPost,
    is_post_already_processed_success,
    mark_post_failed,
)
from alphavault.db.postgres_db import PostgresEngine
from alphavault.rss.utils import RateLimiter, now_str
from alphavault.worker import ai_processor
from alphavault.worker.backoff import backoff_seconds
from alphavault.worker.post_processor import process_one_post_uid
from alphavault.worker.post_processor_topic_prompt_v4 import (
    clear_topic_prompt_trace_context,
    set_topic_prompt_trace_context,
)
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
from alphavault.logging_config import get_logger


_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
AI_TRACE_LOG_PREFIX = "[ai_trace]"
PROCESS_STUCK_LOG_INTERVAL_SECONDS = 30.0
logger = get_logger(__name__)


def _trace_log_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    return " ".join(str(value or "").split()).strip()


def _append_trace_field(parts: list[str], key: str, value: object) -> None:
    text = _trace_log_value(value)
    if not text:
        return
    parts.append(f"{key}={text}")


def _build_ai_trace_id(*, consumer_name: str, message_id: str, post_uid: str) -> str:
    parts = [
        _trace_log_value(consumer_name),
        _trace_log_value(message_id),
        _trace_log_value(post_uid),
    ]
    return "|".join(part for part in parts if part)


def _build_ai_trace_log_line(
    *,
    stage: str,
    trace_id: str = "",
    consumer: str = "",
    queue: str = "",
    source: str = "",
    message_id: str = "",
    post_uid: str = "",
    retry_count: int | None = None,
    next_retry_at: int | None = None,
    success: bool | None = None,
    acked: int | None = None,
    deleted: int | None = None,
    retry_added: int | None = None,
    step: str = "",
    elapsed_seconds: int | None = None,
    reason: str = "",
) -> str:
    parts = [AI_TRACE_LOG_PREFIX, f"stage={_trace_log_value(stage)}"]
    _append_trace_field(parts, "trace_id", trace_id)
    _append_trace_field(parts, "consumer", consumer)
    _append_trace_field(parts, "queue", queue)
    _append_trace_field(parts, "source", source)
    _append_trace_field(parts, "message_id", message_id)
    _append_trace_field(parts, "post_uid", post_uid)
    _append_trace_field(parts, "step", step)
    _append_trace_field(parts, "reason", reason)
    if retry_count is not None:
        _append_trace_field(parts, "retry_count", int(retry_count))
    if next_retry_at is not None:
        _append_trace_field(parts, "next_retry_at", int(next_retry_at))
    if success is not None:
        _append_trace_field(parts, "success", bool(success))
    if acked is not None:
        _append_trace_field(parts, "acked", int(acked))
    if deleted is not None:
        _append_trace_field(parts, "deleted", int(deleted))
    if retry_added is not None:
        _append_trace_field(parts, "retry_added", int(retry_added))
    if elapsed_seconds is not None:
        _append_trace_field(parts, "elapsed_seconds", int(elapsed_seconds))
    return " ".join(parts)


def process_one_redis_payload(
    *,
    engine: PostgresEngine,
    payload: dict[str, object],
    message_id: str,
    redis_client: Any,
    redis_queue_key: str,
    consumer_name: str = "",
    source_name: str = "",
    config: LLMConfig,
    limiter: RateLimiter,
    trace_id: str = "",
    max_retry_count: int,
) -> None:
    resolved_consumer_name = str(consumer_name or "").strip()
    resolved_source_name = str(source_name or "").strip()
    resolved_queue_key = str(redis_queue_key or "").strip()
    resolved_message_id = str(message_id or "").strip()
    payload_post_uid = str(payload.get("post_uid") or "").strip()
    resolved_trace_id = str(trace_id or "").strip() or _build_ai_trace_id(
        consumer_name=resolved_consumer_name,
        message_id=resolved_message_id,
        post_uid=payload_post_uid,
    )
    trace_state = {
        "ack_reason": "",
        "post_uid": payload_post_uid,
        "step": "process_start",
    }
    process_started_at = time.monotonic()
    process_done_event = threading.Event()
    debug_enabled = logger.isEnabledFor(logging.DEBUG)

    def _log_trace(
        *,
        stage: str,
        post_uid: str = "",
        retry_count: int | None = None,
        next_retry_at: int | None = None,
        success: bool | None = None,
        acked: int | None = None,
        deleted: int | None = None,
        retry_added: int | None = None,
        step: str = "",
        elapsed_seconds: int | None = None,
        reason: str = "",
        update_step: bool = True,
    ) -> None:
        if update_step:
            trace_state["step"] = str(step or stage or "").strip()
        if not debug_enabled:
            return
        logger.debug(
            _build_ai_trace_log_line(
                stage=stage,
                trace_id=resolved_trace_id,
                consumer=resolved_consumer_name,
                queue=resolved_queue_key,
                source=resolved_source_name,
                message_id=resolved_message_id,
                post_uid=post_uid or str(trace_state.get("post_uid") or ""),
                retry_count=retry_count,
                next_retry_at=next_retry_at,
                success=success,
                acked=acked,
                deleted=deleted,
                retry_added=retry_added,
                step=str(step or trace_state.get("step") or ""),
                elapsed_seconds=elapsed_seconds,
                reason=reason,
            )
        )

    def _watch_long_running_process() -> None:
        while not process_done_event.wait(
            timeout=float(PROCESS_STUCK_LOG_INTERVAL_SECONDS)
        ):
            _log_trace(
                stage="still_running",
                step=str(trace_state.get("step") or ""),
                elapsed_seconds=int(time.monotonic() - process_started_at),
                update_step=False,
            )

    _log_trace(stage="process_start")
    if debug_enabled:
        threading.Thread(
            target=_watch_long_running_process,
            name="redis-payload-watchdog",
            daemon=True,
        ).start()

    def _payload_retry_count(inner_payload: dict[str, object]) -> int:
        return ai_processor.payload_retry_count(
            inner_payload,
            parse_int_or_default_fn=_parse_int_or_default,
        )

    def _mark_post_failed(**kwargs) -> None:  # type: ignore[no-untyped-def]
        resolved_post_uid = str(kwargs["post_uid"] or "").strip()
        trace_state["post_uid"] = resolved_post_uid
        _log_trace(stage="mark_failed_start", post_uid=resolved_post_uid)
        mark_post_failed(
            kwargs["engine"],
            post_uid=resolved_post_uid,
            model=str(kwargs["model"] or "").strip(),
            prompt_version=str(kwargs["prompt_version"] or "").strip(),
            processed_at=now_str(),
            archived_at=now_str(),
            prefetched_post=kwargs.get("prefetched_post"),
            prefetched_ingested_at=int(time.time()),
        )
        _log_trace(stage="mark_failed_done", post_uid=resolved_post_uid)

    def _payload_to_cloud_post(inner_payload: dict[str, object]) -> CloudPost | None:
        cloud_post = ai_processor.payload_to_cloud_post(
            inner_payload,
            cloud_post_cls=CloudPost,
            now_str_fn=now_str,
            payload_retry_count_fn=_payload_retry_count,
        )
        if cloud_post is None:
            trace_state["ack_reason"] = "payload_invalid"
            _log_trace(stage="payload_invalid", reason="payload_invalid")
            return None
        trace_state["post_uid"] = str(getattr(cloud_post, "post_uid", "") or "").strip()
        return cloud_post

    def _is_post_already_processed_success(*args, **kwargs) -> bool:  # type: ignore[no-untyped-def]
        is_done = is_post_already_processed_success(*args, **kwargs)
        if is_done:
            resolved_post_uid = str(
                kwargs.get("post_uid") or trace_state.get("post_uid") or ""
            ).strip()
            trace_state["post_uid"] = resolved_post_uid
            trace_state["ack_reason"] = "skip_db_processed"
            _log_trace(
                stage="skip_db_processed",
                post_uid=resolved_post_uid,
                reason="skip_db_processed",
            )
        return bool(is_done)

    def _redis_ai_ack_with_trace(client, queue_key: str, ack_message_id: str) -> int:
        reason = str(trace_state.get("ack_reason") or "").strip() or "ack"
        _log_trace(stage="ack_start", reason=reason)
        acked = redis_ai_ack(client, queue_key, ack_message_id)
        _log_trace(
            stage="ack_result",
            reason=reason,
            success=bool(acked),
            acked=int(acked),
        )
        return int(acked)

    def _redis_ai_ack_and_clear_dedup_with_trace(
        client,
        queue_key: str,
        *,
        message_id: str,
        post_uid: str,
    ) -> tuple[int, int]:
        resolved_post_uid = str(post_uid or "").strip()
        trace_state["post_uid"] = resolved_post_uid
        _log_trace(stage="final_failed_ack_start", post_uid=resolved_post_uid)
        result = redis_ai_ack_and_clear_dedup(
            client,
            queue_key,
            message_id=message_id,
            post_uid=resolved_post_uid,
        )
        acked, deleted = result
        _log_trace(
            stage="final_failed_ack_result",
            post_uid=resolved_post_uid,
            success=bool(acked),
            acked=int(acked),
            deleted=int(deleted),
        )
        return result

    def _redis_ai_ack_and_push_retry_with_trace(
        client,
        queue_key: str,
        *,
        message_id: str,
        payload: dict[str, Any],
        next_retry_at: int,
    ) -> tuple[int, int]:
        retry_count = _payload_retry_count(payload)
        resolved_post_uid = str(
            payload.get("post_uid") or trace_state.get("post_uid") or ""
        ).strip()
        trace_state["post_uid"] = resolved_post_uid
        _log_trace(
            stage="retry_handoff_start",
            post_uid=resolved_post_uid,
            retry_count=retry_count,
            next_retry_at=int(next_retry_at),
        )
        result = redis_ai_ack_and_push_retry(
            client,
            queue_key,
            message_id=message_id,
            payload=payload,
            next_retry_at=int(next_retry_at),
        )
        retry_added, acked = result
        _log_trace(
            stage="retry_handoff_result",
            post_uid=resolved_post_uid,
            retry_count=retry_count,
            next_retry_at=int(next_retry_at),
            success=bool(acked),
            retry_added=int(retry_added),
            acked=int(acked),
        )
        return result

    def _process_one_post_uid_with_trace(**kwargs) -> bool:  # type: ignore[no-untyped-def]
        resolved_post_uid = str(
            kwargs.get("post_uid") or trace_state.get("post_uid") or ""
        ).strip()
        trace_state["post_uid"] = resolved_post_uid
        set_topic_prompt_trace_context(
            trace_id=resolved_trace_id,
            consumer_name=resolved_consumer_name,
            message_id=resolved_message_id,
            redis_queue_key=resolved_queue_key,
            source_name=resolved_source_name,
        )
        _log_trace(stage="post_process_start", post_uid=resolved_post_uid)
        try:
            success = process_one_post_uid(**kwargs)
        finally:
            clear_topic_prompt_trace_context()
        if success:
            trace_state["ack_reason"] = "success"
        _log_trace(
            stage="post_process_done",
            post_uid=resolved_post_uid,
            success=bool(success),
        )
        return bool(success)

    try:
        ai_processor.process_one_redis_payload(
            engine=engine,
            payload=payload,
            message_id=message_id,
            redis_client=redis_client,
            redis_queue_key=redis_queue_key,
            source_name=str(source_name or "").strip(),
            config=config,
            limiter=limiter,
            payload_to_cloud_post_fn=_payload_to_cloud_post,
            process_one_post_uid_fn=_process_one_post_uid_with_trace,
            mark_post_failed_fn=_mark_post_failed,
            redis_ai_push_retry_fn=redis_ai_push_retry,
            redis_ai_ack_fn=_redis_ai_ack_with_trace,
            redis_ai_ack_and_clear_dedup_fn=_redis_ai_ack_and_clear_dedup_with_trace,
            redis_ai_ack_and_push_retry_fn=_redis_ai_ack_and_push_retry_with_trace,
            payload_retry_count_fn=_payload_retry_count,
            is_post_already_processed_success_fn=_is_post_already_processed_success,
            backoff_seconds_fn=backoff_seconds,
            now_epoch_fn=lambda: int(time.time()),
            fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
            max_retry_count=max(0, int(max_retry_count)),
        )
        _log_trace(
            stage="process_finish",
            elapsed_seconds=int(time.monotonic() - process_started_at),
            update_step=False,
        )
    except BaseException as err:
        if not isinstance(err, _FATAL_BASE_EXCEPTIONS):
            message = _build_ai_trace_log_line(
                stage="process_crash",
                trace_id=resolved_trace_id,
                consumer=resolved_consumer_name,
                queue=resolved_queue_key,
                source=resolved_source_name,
                message_id=resolved_message_id,
                post_uid=str(trace_state.get("post_uid") or ""),
                step=str(trace_state.get("step") or ""),
                elapsed_seconds=int(time.monotonic() - process_started_at),
                reason=f"{type(err).__name__}: {err}",
            )
            logger.warning(message)
        raise
    finally:
        process_done_event.set()


__all__ = [
    "process_one_redis_payload",
]
