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
        display_md=str(payload.get("display_md") or ""),
        ai_retry_count=max(1, int(payload_retry_count_fn(payload) or 1)),
    )


def build_author_recent_payload(
    *,
    post: Any,
    ai_status: str,
    ai_retry_count: int,
) -> dict[str, object]:
    return {
        "post_uid": str(getattr(post, "post_uid", "") or "").strip(),
        "platform_post_id": str(getattr(post, "platform_post_id", "") or "").strip(),
        "author": str(getattr(post, "author", "") or "").strip(),
        "created_at": str(getattr(post, "created_at", "") or "").strip(),
        "url": str(getattr(post, "url", "") or "").strip(),
        "raw_text": str(getattr(post, "raw_text", "") or ""),
        "display_md": str(getattr(post, "display_md", "") or ""),
        "processed_at": "",
        "ai_status": str(ai_status or "").strip(),
        "ai_retry_count": max(0, int(ai_retry_count)),
    }


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
    author_recent_local_cache_get_fn: Callable[
        ..., tuple[list[dict[str, Any]], bool, bool]
    ],
    author_recent_local_cache_set_fn: Callable[..., None],
    load_recent_posts_by_author_fn: Callable[..., list[dict[str, Any]]],
    try_mark_ai_running_fn: Callable[..., bool],
    process_one_post_uid_fn: Callable[..., bool],
    redis_ai_ack_and_cleanup_fn: Callable[..., bool],
    redis_ai_push_delayed_fn: Callable[..., None],
    redis_ai_ack_processing_fn: Callable[..., None],
    payload_retry_count_fn: Callable[[dict[str, object]], int],
    build_author_recent_payload_fn: Callable[..., dict[str, object]],
    backoff_seconds_fn: Callable[[int], int],
    now_epoch_fn: Callable[[], int],
    fatal_exceptions: tuple[type[BaseException], ...],
    author_recent_context_limit: int,
) -> None:
    cloud_post = payload_to_cloud_post_fn(payload)
    if cloud_post is None:
        try:
            redis_ai_ack_processing_fn(redis_client, redis_queue_key, processing_msg)
        except Exception:
            return
        return

    resolved_author = str(getattr(cloud_post, "author", "") or "").strip()
    prefetched_recent: list[dict[str, object]] = []
    has_recent_cache = False
    local_rows, local_marked_empty, local_hit = author_recent_local_cache_get_fn(
        queue_key=str(redis_queue_key or "").strip(),
        author=resolved_author,
    )
    if local_hit:
        prefetched_recent = list(local_rows)
        has_recent_cache = bool(prefetched_recent) or bool(local_marked_empty)
    if not has_recent_cache:
        try:
            prefetched_recent = load_recent_posts_by_author_fn(
                engine,
                author=resolved_author,
                limit=int(author_recent_context_limit),
            )
            author_recent_local_cache_set_fn(
                queue_key=str(redis_queue_key or "").strip(),
                author=resolved_author,
                rows=list(prefetched_recent),
                marked_empty=not bool(prefetched_recent),
            )
        except BaseException as err:
            if isinstance(err, fatal_exceptions):
                raise
            prefetched_recent = []

    ok = try_mark_ai_running_fn(
        engine,
        post_uid=str(getattr(cloud_post, "post_uid", "") or ""),
        now_epoch=int(now_epoch_fn()),
    )
    if not ok:
        try:
            redis_ai_ack_processing_fn(redis_client, redis_queue_key, processing_msg)
        except Exception:
            pass
        return

    success = process_one_post_uid_fn(
        engine=engine,
        post_uid=str(getattr(cloud_post, "post_uid", "") or ""),
        config=config,
        limiter=limiter,
        prefetched_post=cloud_post,
        prefetched_recent=prefetched_recent,
        source_name=str(source_name or "").strip(),
        outbox_source=str(redis_queue_key or "").strip(),
    )
    if success:
        done_payload = build_author_recent_payload_fn(
            post=cloud_post,
            ai_status="done",
            ai_retry_count=int(
                max(1, int(getattr(cloud_post, "ai_retry_count", 1) or 1))
            ),
        )
        author_recent_local_cache_set_fn(
            queue_key=str(redis_queue_key or "").strip(),
            author=resolved_author,
            rows=[done_payload],
            marked_empty=False,
        )
        redis_ai_ack_and_cleanup_fn(
            redis_client,
            redis_queue_key,
            msg=processing_msg,
            post_uid=str(getattr(cloud_post, "post_uid", "") or ""),
            spool_dir=spool_dir,
            verbose=bool(verbose),
        )
        return

    retry_count = max(1, int(payload_retry_count_fn(payload)) + 1)
    next_retry_at = int(now_epoch_fn()) + int(backoff_seconds_fn(retry_count))
    retry_payload = dict(payload)
    retry_payload["retry_count"] = int(retry_count)
    retry_payload["next_retry_at"] = int(next_retry_at)
    error_payload = build_author_recent_payload_fn(
        post=cloud_post,
        ai_status="error",
        ai_retry_count=int(retry_count),
    )
    author_recent_local_cache_set_fn(
        queue_key=str(redis_queue_key or "").strip(),
        author=resolved_author,
        rows=[error_payload],
        marked_empty=False,
    )
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
