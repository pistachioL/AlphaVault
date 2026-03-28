from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.engine import Engine

from alphavault.ai.analyze import clean_text
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
    DEFAULT_REDIS_DEDUP_TTL_SECONDS,
    redis_try_push_dedup,
)
from alphavault.worker.spool import spool_delete, spool_write

RSS_LOG_PREFIX = "[rss]"
RSS_LOG_EVENT_INSERTED = "inserted"
LOG_EMPTY_VALUE = "(empty)"


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


def _try_push_to_redis(
    redis_client,
    redis_queue_key: str,
    *,
    post_uid: str,
    payload: Dict[str, Any],
    verbose: bool,
) -> bool:
    if not redis_client or not redis_queue_key or not post_uid:
        return False
    return redis_try_push_dedup(
        redis_client,
        redis_queue_key,
        post_uid=post_uid,
        payload=payload,
        ttl_seconds=DEFAULT_REDIS_DEDUP_TTL_SECONDS,
        verbose=bool(verbose),
    )


def _clean_log_value(value: object) -> str:
    text = " ".join(str(value or "").split())
    return text if text else LOG_EMPTY_VALUE


def _format_progress(*, current: int, total: int) -> str:
    safe_current = max(0, int(current))
    safe_total = max(0, int(total))
    return f"{safe_current}/{safe_total}"


def _build_rss_inserted_log_line(
    *,
    platform: str,
    post_uid: str,
    author: str,
    entry_index: int,
    entry_total: int,
    feed_index: int,
    feed_total: int,
    inserted_total: int,
) -> str:
    return " ".join(
        [
            f"{RSS_LOG_PREFIX} {RSS_LOG_EVENT_INSERTED}",
            f"platform={_clean_log_value(platform)}",
            f"post_uid={_clean_log_value(post_uid)}",
            f"author={_clean_log_value(author)}",
            f"progress={_format_progress(current=entry_index, total=entry_total)}",
            f"feed_progress={_format_progress(current=feed_index, total=feed_total)}",
            f"inserted_total={max(0, int(inserted_total))}",
        ]
    )


def ingest_rss_many_once(
    *,
    rss_urls: list[str],
    engine: Optional[Engine],
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
) -> Tuple[int, bool]:
    inserted = 0
    turso_error = False
    seen_post_uids: set[str] = set()
    seen_urls: set[str] = set()
    normalized_platform = str(platform or "weibo").strip().lower()

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

    feed_total = len(rss_urls)
    for feed_index, rss_url in enumerate(rss_urls, start=1):
        try:
            feed_user_id = user_id or infer_user_id_from_rss_url(rss_url)
            feed = fetch_feed(rss_url, timeout=rss_timeout, retries=rss_retries)
            entries = feed.entries or []
            if limit:
                entries = entries[:limit]
        except Exception as e:
            if verbose:
                print(
                    f"[rss] source_error url={rss_url} {type(e).__name__}: {e}",
                    flush=True,
                )
            continue

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

            try:
                spool_write(spool_dir, post_uid, payload)
            except Exception as e:
                print(
                    f"[spool] write_error {post_uid} {type(e).__name__}: {e}",
                    flush=True,
                )

            if engine is None:
                _try_push_to_redis(
                    redis_client,
                    redis_queue_key,
                    post_uid=post_uid,
                    payload=payload,
                    verbose=bool(verbose),
                )
                continue

            try:
                upsert_pending_post(
                    engine,
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
                inserted += 1
                if verbose:
                    print(
                        _build_rss_inserted_log_line(
                            platform=normalized_platform,
                            post_uid=post_uid,
                            author=resolved_author,
                            entry_index=entry_index,
                            entry_total=entry_total,
                            feed_index=feed_index,
                            feed_total=feed_total,
                            inserted_total=inserted,
                        ),
                        flush=True,
                    )
            except Exception as e:
                turso_error = True
                _try_push_to_redis(
                    redis_client,
                    redis_queue_key,
                    post_uid=post_uid,
                    payload=payload,
                    verbose=bool(verbose),
                )
                if verbose:
                    print(
                        f"[rss] turso_write_error {post_uid} {type(e).__name__}: {e}",
                        flush=True,
                    )

            seen_post_uids.add(post_uid)
            seen_urls.add(link)

    return inserted, turso_error
