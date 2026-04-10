from __future__ import annotations

from typing import Any, Callable


def payload_retry_count(
    payload: dict[str, object],
    *,
    parse_int_or_default_fn: Callable[[object, int], int],
) -> int:
    raw_retry_count = payload.get("retry_count")
    return max(0, int(parse_int_or_default_fn(raw_retry_count, 0)))


def payload_to_cloud_post(
    payload: dict[str, object],
    *,
    cloud_post_cls: Any,
    now_str_fn: Callable[[], str],
    payload_retry_count_fn: Callable[[dict[str, object]], int],
) -> Any | None:
    post_uid = str(payload.get("post_uid") or "").strip()
    if not post_uid:
        return None
    return cloud_post_cls(
        post_uid=post_uid,
        platform=str(payload.get("platform") or "weibo").strip() or "weibo",
        platform_post_id=str(payload.get("platform_post_id") or "").strip(),
        author=str(payload.get("author") or "").strip(),
        created_at=str(payload.get("created_at") or "").strip() or now_str_fn(),
        url=str(payload.get("url") or "").strip(),
        raw_text=str(payload.get("raw_text") or ""),
        ai_retry_count=max(1, int(payload_retry_count_fn(payload) or 1)),
    )


def process_one_redis_payload(
    *,
    engine: Any,
    payload: dict[str, object],
    message_id: str,
    redis_client: Any,
    redis_queue_key: str,
    source_name: str = "",
    config: Any,
    limiter: Any,
    verbose: bool,
    payload_to_cloud_post_fn: Callable[[dict[str, object]], Any | None],
    process_one_post_uid_fn: Callable[..., bool],
    mark_post_failed_fn: Callable[..., None],
    redis_ai_push_retry_fn: Callable[..., None],
    redis_ai_ack_fn: Callable[..., object],
    redis_ai_ack_and_clear_dedup_fn: Callable[..., object],
    redis_ai_ack_and_push_retry_fn: Callable[..., object],
    payload_retry_count_fn: Callable[[dict[str, object]], int],
    backoff_seconds_fn: Callable[[int], int],
    now_epoch_fn: Callable[[], int],
    fatal_exceptions: tuple[type[BaseException], ...],
    max_retry_count: int,
) -> None:
    cloud_post = payload_to_cloud_post_fn(payload)
    if cloud_post is None:
        try:
            redis_ai_ack_fn(redis_client, redis_queue_key, message_id)
        except Exception:
            return
        return

    resolved_post_uid = str(getattr(cloud_post, "post_uid", "") or "").strip()

    prefetched_recent: list[dict[str, object]] = []

    success = process_one_post_uid_fn(
        engine=engine,
        post_uid=resolved_post_uid,
        config=config,
        limiter=limiter,
        prefetched_post=cloud_post,
        prefetched_recent=prefetched_recent,
        source_name=str(source_name or "").strip(),
    )
    if success:
        redis_ai_ack_and_clear_dedup_fn(
            redis_client,
            redis_queue_key,
            message_id=message_id,
            post_uid=resolved_post_uid,
        )
        return

    retry_count = max(1, int(payload_retry_count_fn(payload)) + 1)
    if retry_count > max(0, int(max_retry_count)):
        try:
            mark_post_failed_fn(
                engine=engine,
                post_uid=resolved_post_uid,
                model=str(getattr(config, "model", "") or "").strip(),
                prompt_version=str(getattr(config, "prompt_version", "") or "").strip(),
                prefetched_post=cloud_post,
            )
        except BaseException as err:
            if isinstance(err, fatal_exceptions):
                raise
            if verbose:
                print(
                    f"[ai] mark_failed_error post_uid={resolved_post_uid} "
                    f"{type(err).__name__}: {err}",
                    flush=True,
                )
            return
        try:
            redis_ai_ack_and_clear_dedup_fn(
                redis_client,
                redis_queue_key,
                message_id=message_id,
                post_uid=resolved_post_uid,
            )
        except Exception as err:
            if verbose:
                print(
                    f"[ai] redis_final_ack_error post_uid={resolved_post_uid} "
                    f"{type(err).__name__}: {err}",
                    flush=True,
                )
        return

    next_retry_at = int(now_epoch_fn()) + int(backoff_seconds_fn(retry_count))
    retry_payload = dict(payload)
    retry_payload["retry_count"] = int(retry_count)
    retry_payload["next_retry_at"] = int(next_retry_at)
    try:
        redis_ai_ack_and_push_retry_fn(
            redis_client,
            redis_queue_key,
            message_id=message_id,
            payload=retry_payload,
            next_retry_at=int(next_retry_at),
        )
    except Exception as err:
        if verbose:
            print(
                f"[ai] redis_retry_handoff_error post_uid={resolved_post_uid} "
                f"{type(err).__name__}: {err}",
                flush=True,
            )
        return
    del redis_ai_push_retry_fn
