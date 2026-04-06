from __future__ import annotations

from pathlib import Path
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
    processing_msg: str,
    redis_client: Any,
    redis_queue_key: str,
    source_name: str = "",
    spool_dir: Path,
    config: Any,
    limiter: Any,
    verbose: bool,
    payload_to_cloud_post_fn: Callable[[dict[str, object]], Any | None],
    redis_ai_try_claim_lease_fn: Callable[..., str],
    process_one_post_uid_fn: Callable[..., bool],
    redis_ai_release_lease_fn: Callable[..., bool],
    redis_ai_ack_and_cleanup_fn: Callable[..., bool],
    redis_ai_push_delayed_fn: Callable[..., None],
    redis_ai_ack_processing_fn: Callable[..., None],
    payload_retry_count_fn: Callable[[dict[str, object]], int],
    backoff_seconds_fn: Callable[[int], int],
    now_epoch_fn: Callable[[], int],
    fatal_exceptions: tuple[type[BaseException], ...],
    lease_seconds: int,
) -> None:
    cloud_post = payload_to_cloud_post_fn(payload)
    if cloud_post is None:
        try:
            redis_ai_ack_processing_fn(redis_client, redis_queue_key, processing_msg)
        except Exception:
            return
        return

    resolved_post_uid = str(getattr(cloud_post, "post_uid", "") or "").strip()
    try:
        lease_token = str(
            redis_ai_try_claim_lease_fn(
                redis_client,
                redis_queue_key,
                post_uid=resolved_post_uid,
                lease_seconds=max(1, int(lease_seconds)),
            )
            or ""
        ).strip()
    except BaseException as err:
        if isinstance(err, fatal_exceptions):
            raise
        if verbose:
            print(
                f"[ai] redis_lease_claim_error post_uid={resolved_post_uid} "
                f"{type(err).__name__}: {err}",
                flush=True,
            )
        return
    if not lease_token:
        try:
            redis_ai_ack_processing_fn(redis_client, redis_queue_key, processing_msg)
        except Exception:
            pass
        return

    try:
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
            redis_ai_ack_and_cleanup_fn(
                redis_client,
                redis_queue_key,
                msg=processing_msg,
                post_uid=resolved_post_uid,
                spool_dir=spool_dir,
                lease_token=lease_token,
                verbose=bool(verbose),
            )
            return

        retry_count = max(1, int(payload_retry_count_fn(payload)) + 1)
        next_retry_at = int(now_epoch_fn()) + int(backoff_seconds_fn(retry_count))
        retry_payload = dict(payload)
        retry_payload["retry_count"] = int(retry_count)
        retry_payload["next_retry_at"] = int(next_retry_at)
        try:
            redis_ai_push_delayed_fn(
                redis_client,
                redis_queue_key,
                payload=retry_payload,
                next_retry_at=int(next_retry_at),
            )
        except Exception as err:
            if verbose:
                print(
                    f"[ai] redis_delay_push_error post_uid={getattr(cloud_post, 'post_uid', '')} "
                    f"{type(err).__name__}: {err}",
                    flush=True,
                )
            return
        try:
            redis_ai_ack_processing_fn(redis_client, redis_queue_key, processing_msg)
        except Exception as err:
            if verbose:
                print(
                    f"[ai] redis_ack_error post_uid={getattr(cloud_post, 'post_uid', '')} "
                    f"{type(err).__name__}: {err}",
                    flush=True,
                )
    finally:
        if lease_token:
            redis_ai_release_lease_fn(
                redis_client,
                redis_queue_key,
                post_uid=resolved_post_uid,
                lease_token=lease_token,
                verbose=bool(verbose),
            )
