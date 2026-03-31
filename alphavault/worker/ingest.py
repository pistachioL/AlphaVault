from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from alphavault.ai.analyze import clean_text
from alphavault.db.turso_db import (
    TursoEngine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
    turso_connect_autocommit,
)
from alphavault.db.turso_queue import upsert_pending_post
from alphavault.rss.utils import (
    build_ids,
    choose_author,
    fetch_feed,
    get_entry_content,
    infer_user_id_from_rss_url,
    now_str,
    parse_datetime,
    split_xueqiu_context_segments,
)
from alphavault.text.html import html_to_text
from alphavault.weibo.display import (
    extract_image_urls_from_html,
    format_weibo_display_md,
)
from alphavault.worker.redis_queue import (
    REDIS_PUSH_STATUS_DUPLICATE,
    REDIS_PUSH_STATUS_ERROR,
    REDIS_PUSH_STATUS_PUSHED,
    resolve_redis_dedup_ttl_seconds,
    redis_try_push_ai_dedup_status,
)
from alphavault.worker.spool import spool_delete, spool_write

RSS_LOG_PREFIX = "[rss]"
RSS_LOG_EVENT_ACCEPTED = "accepted"
RSS_LOG_EVENT_FEED_START = "feed_start"
RSS_LOG_EVENT_FEED_DONE = "feed_done"
RSS_LOG_EVENT_FEED_SLEEP = "feed_sleep"
RSS_LOG_EVENT_CYCLE_DONE = "cycle_done"
LOG_EMPTY_VALUE = "(empty)"
_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _build_raw_text(*, title: str, content_text: str) -> str:
    resolved_content = str(content_text or "").strip()
    if resolved_content:
        return resolved_content
    return str(title or "").strip()


def _build_post_texts(
    *,
    title: str,
    content_text: str,
    platform: str,
) -> tuple[str, str]:
    resolved_text = _build_raw_text(title=title, content_text=content_text)
    if str(platform or "").strip().lower() != "xueqiu":
        return resolved_text, resolved_text

    segments = split_xueqiu_context_segments(resolved_text)
    if not segments:
        return resolved_text, resolved_text

    display_text = "\n\n---\n\n".join(segments)
    return segments[-1], display_text


def _try_push_to_redis_status(
    redis_client,
    redis_queue_key: str,
    *,
    post_uid: str,
    payload: Dict[str, Any],
    verbose: bool,
) -> str:
    if not redis_client or not redis_queue_key or not post_uid:
        return REDIS_PUSH_STATUS_ERROR
    return redis_try_push_ai_dedup_status(
        redis_client,
        redis_queue_key,
        post_uid=post_uid,
        payload=payload,
        ttl_seconds=resolve_redis_dedup_ttl_seconds(),
        verbose=bool(verbose),
    )


def _clean_log_value(value: object) -> str:
    text = " ".join(str(value or "").split())
    return text if text else LOG_EMPTY_VALUE


def _format_progress(*, current: int, total: int) -> str:
    safe_current = max(0, int(current))
    safe_total = max(0, int(total))
    return f"{safe_current}/{safe_total}"


def _build_accepted_user_counter_key(
    *, feed_user_id: Optional[str], rss_url: str
) -> str:
    resolved_user_id = str(feed_user_id or "").strip()
    if resolved_user_id:
        return resolved_user_id
    resolved_rss_url = str(rss_url or "").strip()
    if resolved_rss_url:
        return resolved_rss_url
    return LOG_EMPTY_VALUE


def _build_rss_accepted_log_line(
    *,
    platform: str,
    post_uid: str,
    author: str,
    entry_index: int,
    entry_total: int,
    feed_index: int,
    feed_total: int,
    accepted_total: int,
) -> str:
    return " ".join(
        [
            f"{RSS_LOG_PREFIX} {RSS_LOG_EVENT_ACCEPTED}",
            f"platform={_clean_log_value(platform)}",
            f"post_uid={_clean_log_value(post_uid)}",
            f"author={_clean_log_value(author)}",
            f"progress={_format_progress(current=entry_index, total=entry_total)}",
            f"feed_progress={_format_progress(current=feed_index, total=feed_total)}",
            f"accepted_total={max(0, int(accepted_total))}",
        ]
    )


def _build_rss_feed_start_log_line(
    *,
    platform: str,
    feed_index: int,
    feed_total: int,
    rss_url: str,
) -> str:
    return " ".join(
        [
            f"{RSS_LOG_PREFIX} {RSS_LOG_EVENT_FEED_START}",
            f"platform={_clean_log_value(platform)}",
            f"feed_progress={_format_progress(current=feed_index, total=feed_total)}",
            f"url={_clean_log_value(rss_url)}",
        ]
    )


def _build_rss_feed_done_log_line(
    *,
    platform: str,
    feed_index: int,
    feed_total: int,
    entry_total: int,
    accepted_in_feed: int,
    source_error: bool,
) -> str:
    return " ".join(
        [
            f"{RSS_LOG_PREFIX} {RSS_LOG_EVENT_FEED_DONE}",
            f"platform={_clean_log_value(platform)}",
            f"feed_progress={_format_progress(current=feed_index, total=feed_total)}",
            f"entries={max(0, int(entry_total))}",
            f"accepted={max(0, int(accepted_in_feed))}",
            f"source_error={1 if source_error else 0}",
        ]
    )


def _build_rss_feed_sleep_log_line(
    *,
    platform: str,
    feed_index: int,
    feed_total: int,
    sleep_seconds: float,
) -> str:
    return " ".join(
        [
            f"{RSS_LOG_PREFIX} {RSS_LOG_EVENT_FEED_SLEEP}",
            f"platform={_clean_log_value(platform)}",
            f"feed_progress={_format_progress(current=feed_index, total=feed_total)}",
            f"sleep={float(sleep_seconds):.1f}s",
        ]
    )


def _build_rss_cycle_done_log_line(
    *,
    platform: str,
    feed_total: int,
    accepted_total: int,
    enqueue_error: bool,
) -> str:
    return " ".join(
        [
            f"{RSS_LOG_PREFIX} {RSS_LOG_EVENT_CYCLE_DONE}",
            f"platform={_clean_log_value(platform)}",
            f"feeds={max(0, int(feed_total))}",
            f"accepted_total={max(0, int(accepted_total))}",
            f"enqueue_error={1 if enqueue_error else 0}",
        ]
    )


def _coerce_nonnegative_float(value: object, *, default: float) -> float:
    try:
        return max(0.0, float(str(value).strip()))
    except Exception:
        return max(0.0, float(default))


def _maybe_dispose_turso_engine_on_transient_error(
    *, engine: TursoEngine, err: BaseException
) -> None:
    if not (is_turso_stream_not_found_error(err) or is_turso_libsql_panic_error(err)):
        return
    try:
        engine.dispose()
    except Exception:
        return


def ingest_rss_many_once(
    *,
    rss_urls: list[str],
    engine: Optional[TursoEngine],
    spool_dir: Path,
    redis_client,
    redis_queue_key: str,
    platform: str,
    author: str,
    user_id: Optional[str],
    limit: Optional[int],
    rss_timeout: float,
    rss_retries: int,
    verbose: bool,
    rss_feed_sleep_seconds: float = 0.0,
    on_item_ingested: Optional[Callable[[], None]] = None,
) -> Tuple[int, bool]:
    accepted = 0
    accepted_per_user: dict[str, int] = {}
    enqueue_error = False
    seen_post_uids: set[str] = set()
    seen_urls: set[str] = set()
    normalized_platform = str(platform or "weibo").strip().lower()
    feed_sleep_seconds = _coerce_nonnegative_float(rss_feed_sleep_seconds, default=0.0)
    feed_total = len(rss_urls)

    write_conn: Optional[Any] = None
    write_conn_context: Any = None

    def build_display_md(*, text: str, author_name: str, image_urls: list[str]) -> str:
        if normalized_platform == "weibo":
            return format_weibo_display_md(
                text, author=author_name, image_urls=image_urls
            )
        if not text and not image_urls:
            return ""
        img_lines = [f'<img class="ke_img" src="{url}" />' for url in image_urls]
        if not img_lines:
            return text
        if not text:
            return "\n".join(img_lines)
        return text.rstrip() + "\n" + "\n".join(img_lines)

    def _mark_item_accepted(
        *,
        post_uid: str,
        resolved_author: str,
        entry_index: int,
        entry_total: int,
        feed_index: int,
        feed_total: int,
        feed_counter_key: str,
    ) -> None:
        nonlocal accepted
        accepted += 1
        accepted_per_user[feed_counter_key] = (
            accepted_per_user.get(feed_counter_key, 0) + 1
        )
        if on_item_ingested is not None:
            try:
                on_item_ingested()
            except Exception:
                pass
        if verbose:
            print(
                _build_rss_accepted_log_line(
                    platform=normalized_platform,
                    post_uid=post_uid,
                    author=resolved_author,
                    entry_index=entry_index,
                    entry_total=entry_total,
                    feed_index=feed_index,
                    feed_total=feed_total,
                    accepted_total=accepted_per_user[feed_counter_key],
                ),
                flush=True,
            )

    def _close_write_conn(*, broken: bool = False) -> None:
        nonlocal write_conn, write_conn_context
        if write_conn is None:
            return
        try:
            close_fn = getattr(write_conn, "close", None)
            if callable(close_fn):
                close_fn(broken=bool(broken))
            elif write_conn_context is not None:
                write_conn_context.__exit__(None, None, None)
        except Exception:
            pass
        write_conn = None
        write_conn_context = None

    def _try_open_write_conn() -> Optional[Any]:
        nonlocal enqueue_error, write_conn, write_conn_context
        if engine is None:
            return None
        if write_conn is not None:
            return write_conn
        try:
            resolved_context = turso_connect_autocommit(engine)
            if hasattr(resolved_context, "__enter__") and hasattr(
                resolved_context, "__exit__"
            ):
                resolved_conn = resolved_context.__enter__()
                write_conn_context = resolved_context
            else:
                resolved_conn = resolved_context
                write_conn_context = None
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
            enqueue_error = True
            _maybe_dispose_turso_engine_on_transient_error(engine=engine, err=e)
            if verbose:
                print(
                    f"[rss] turso_connect_error {type(e).__name__}: {e}",
                    flush=True,
                )
            return None
        write_conn = resolved_conn
        return write_conn

    try:
        for feed_index, rss_url in enumerate(rss_urls, start=1):
            if verbose:
                print(
                    _build_rss_feed_start_log_line(
                        platform=normalized_platform,
                        feed_index=feed_index,
                        feed_total=feed_total,
                        rss_url=rss_url,
                    ),
                    flush=True,
                )

            feed_user_id = user_id
            feed_counter_key = _build_accepted_user_counter_key(
                feed_user_id=feed_user_id,
                rss_url=rss_url,
            )
            feed_error = False
            feed_accepted_before = accepted
            entries: list[dict[str, Any]] = []

            try:
                if not feed_user_id:
                    feed_user_id = infer_user_id_from_rss_url(rss_url)
                feed_counter_key = _build_accepted_user_counter_key(
                    feed_user_id=feed_user_id,
                    rss_url=rss_url,
                )
                feed = fetch_feed(rss_url, timeout=rss_timeout, retries=rss_retries)
                entries = feed.entries or []
                if limit:
                    entries = entries[:limit]
            except Exception as e:
                feed_error = True
                if verbose:
                    print(
                        f"[rss] source_error url={rss_url} {type(e).__name__}: {e}",
                        flush=True,
                    )

            entry_total = len(entries)
            for entry_index, entry in enumerate(entries, start=1):
                link = (entry.get("link") or entry.get("id") or "").strip()
                if not link:
                    continue
                platform_post_id, post_uid, _bid = build_ids(
                    entry, link, feed_user_id, platform=normalized_platform
                )
                if not post_uid or not platform_post_id:
                    continue
                if post_uid in seen_post_uids:
                    continue
                if link in seen_urls and normalized_platform == "weibo":
                    continue

                raw_title = clean_text(entry.get("title") or "")
                title = (html_to_text(raw_title) or raw_title).strip()
                content_html = get_entry_content(entry)
                content_text = html_to_text(content_html)
                image_urls = extract_image_urls_from_html(content_html)

                raw_text, display_text = _build_post_texts(
                    title=title,
                    content_text=content_text,
                    platform=normalized_platform,
                )
                if not raw_text:
                    continue

                created_at = parse_datetime(entry)
                resolved_author = choose_author(
                    entry, feed, author, platform=normalized_platform
                )
                display_md = build_display_md(
                    text=display_text,
                    author_name=resolved_author,
                    image_urls=list(image_urls or []),
                )

                payload: Dict[str, Any] = {
                    "post_uid": post_uid,
                    "platform": normalized_platform,
                    "platform_post_id": platform_post_id,
                    "author": resolved_author,
                    "created_at": created_at,
                    "url": link,
                    "raw_text": raw_text,
                    "display_md": display_md,
                    "ingested_at": int(time.time()),
                }

                enqueued = False
                redis_status = REDIS_PUSH_STATUS_ERROR
                try:
                    spool_write(spool_dir, post_uid, payload)
                    enqueued = True
                    _mark_item_accepted(
                        post_uid=post_uid,
                        resolved_author=resolved_author,
                        entry_index=entry_index,
                        entry_total=entry_total,
                        feed_index=feed_index,
                        feed_total=feed_total,
                        feed_counter_key=feed_counter_key,
                    )
                except Exception as err:
                    enqueue_error = True
                    redis_status = _try_push_to_redis_status(
                        redis_client,
                        redis_queue_key,
                        post_uid=post_uid,
                        payload=payload,
                        verbose=bool(verbose),
                    )
                    pushed_or_duplicate = redis_status in {
                        REDIS_PUSH_STATUS_PUSHED,
                        REDIS_PUSH_STATUS_DUPLICATE,
                    }
                    if redis_status == REDIS_PUSH_STATUS_PUSHED:
                        enqueued = True
                        _mark_item_accepted(
                            post_uid=post_uid,
                            resolved_author=resolved_author,
                            entry_index=entry_index,
                            entry_total=entry_total,
                            feed_index=feed_index,
                            feed_total=feed_total,
                            feed_counter_key=feed_counter_key,
                        )
                    if verbose:
                        print(
                            f"[spool] enqueue_error post_uid={post_uid} "
                            f"spool={type(err).__name__}: {err} "
                            f"redis_fallback={1 if pushed_or_duplicate else 0}",
                            flush=True,
                        )
                    if enqueued:
                        seen_post_uids.add(post_uid)
                        seen_urls.add(link)
                    continue

                redis_status = _try_push_to_redis_status(
                    redis_client,
                    redis_queue_key,
                    post_uid=post_uid,
                    payload=payload,
                    verbose=bool(verbose),
                )
                if redis_status == REDIS_PUSH_STATUS_PUSHED:
                    seen_post_uids.add(post_uid)
                    seen_urls.add(link)
                    continue
                if redis_status == REDIS_PUSH_STATUS_DUPLICATE:
                    try:
                        spool_delete(spool_dir, post_uid)
                    except Exception:
                        pass
                    seen_post_uids.add(post_uid)
                    seen_urls.add(link)
                    continue
                if redis_client and redis_queue_key:
                    enqueue_error = True

                if engine is None:
                    if enqueued:
                        seen_post_uids.add(post_uid)
                        seen_urls.add(link)
                    continue

                write_conn_instance = _try_open_write_conn()
                if write_conn_instance is None:
                    redis_status = _try_push_to_redis_status(
                        redis_client,
                        redis_queue_key,
                        post_uid=post_uid,
                        payload=payload,
                        verbose=bool(verbose),
                    )
                    pushed_or_duplicate = redis_status in {
                        REDIS_PUSH_STATUS_PUSHED,
                        REDIS_PUSH_STATUS_DUPLICATE,
                    }
                    if redis_status == REDIS_PUSH_STATUS_DUPLICATE:
                        try:
                            spool_delete(spool_dir, post_uid)
                        except Exception:
                            pass
                    if not pushed_or_duplicate:
                        enqueue_error = True
                    if verbose:
                        print(
                            f"[rss] turso_unavailable post_uid={post_uid} "
                            f"redis_fallback={1 if pushed_or_duplicate else 0}",
                            flush=True,
                        )
                    if enqueued:
                        seen_post_uids.add(post_uid)
                        seen_urls.add(link)
                    continue

                try:
                    upsert_pending_post(
                        write_conn_instance,
                        post_uid=post_uid,
                        platform=normalized_platform,
                        platform_post_id=platform_post_id,
                        author=resolved_author,
                        created_at=created_at,
                        url=link,
                        raw_text=raw_text,
                        display_md=display_md,
                        archived_at=now_str(),
                        ingested_at=int(payload["ingested_at"]),
                    )
                    spool_delete(spool_dir, post_uid)
                except BaseException as e:
                    if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                        raise
                    enqueue_error = True
                    is_broken_conn = bool(
                        is_turso_stream_not_found_error(e)
                        or is_turso_libsql_panic_error(e)
                    )
                    _maybe_dispose_turso_engine_on_transient_error(engine=engine, err=e)
                    _close_write_conn(broken=is_broken_conn)
                    redis_status = _try_push_to_redis_status(
                        redis_client,
                        redis_queue_key,
                        post_uid=post_uid,
                        payload=payload,
                        verbose=bool(verbose),
                    )
                    pushed_or_duplicate = redis_status in {
                        REDIS_PUSH_STATUS_PUSHED,
                        REDIS_PUSH_STATUS_DUPLICATE,
                    }
                    if redis_status == REDIS_PUSH_STATUS_DUPLICATE:
                        try:
                            spool_delete(spool_dir, post_uid)
                        except Exception:
                            pass
                    if verbose:
                        print(
                            f"[rss] turso_write_error {post_uid} {type(e).__name__}: {e} "
                            f"redis_fallback={1 if pushed_or_duplicate else 0}",
                            flush=True,
                        )

                if enqueued:
                    seen_post_uids.add(post_uid)
                    seen_urls.add(link)

            if verbose:
                print(
                    _build_rss_feed_done_log_line(
                        platform=normalized_platform,
                        feed_index=feed_index,
                        feed_total=feed_total,
                        entry_total=entry_total,
                        accepted_in_feed=(accepted - feed_accepted_before),
                        source_error=feed_error,
                    ),
                    flush=True,
                )

            if feed_index < feed_total and feed_sleep_seconds > 0:
                if verbose:
                    print(
                        _build_rss_feed_sleep_log_line(
                            platform=normalized_platform,
                            feed_index=feed_index,
                            feed_total=feed_total,
                            sleep_seconds=feed_sleep_seconds,
                        ),
                        flush=True,
                    )
                time.sleep(feed_sleep_seconds)
    finally:
        _close_write_conn()

    if verbose:
        print(
            _build_rss_cycle_done_log_line(
                platform=normalized_platform,
                feed_total=feed_total,
                accepted_total=accepted,
                enqueue_error=enqueue_error,
            ),
            flush=True,
        )

    return accepted, enqueue_error
