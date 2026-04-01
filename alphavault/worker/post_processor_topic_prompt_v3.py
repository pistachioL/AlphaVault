from __future__ import annotations

from contextlib import ExitStack
import json
import time

from alphavault.ai._client import AiInvalidJsonError
from alphavault.ai.analyze import (
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_MODEL,
    _call_ai_with_litellm,
    format_llm_error_one_line,
    normalize_action,
)
from alphavault.ai.tag_validate import validate_topic_prompt_v3_ai_result
from alphavault.db.turso_db import TursoEngine
from alphavault.db.turso_queue import (
    CloudPost,
    load_cloud_post,
    load_recent_posts_by_author,
    mark_ai_error,
    try_mark_ai_running,
    write_assertions_and_mark_done,
)
from alphavault.rss.utils import RateLimiter, now_str
from alphavault.research_backfill_cache import mark_stock_backfill_dirty_from_assertions
from alphavault.research_stock_cache import mark_stock_dirty_from_assertions
from alphavault.weibo.topic_prompt_tree import (
    MAX_THREAD_POSTS,
    MAX_TOPIC_PROMPT_CHARS,
    thread_root_info_for_post,
)
from alphavault.worker.backoff import backoff_seconds
from alphavault.worker.post_processor_utils import (
    as_str_list,
    build_assertion_outbox_event_payload,
    ensure_prefetched_post_persisted,
    score_from_assertions,
)
from alphavault.worker.local_cache import (
    apply_outbox_event_payload,
    open_local_cache,
    resolve_local_cache_db_path,
)
from alphavault.worker.runtime_models import LLMConfig, _clamp_float, _clamp_int
from alphavault.worker.topic_prompt_v3 import (
    build_topic_prompt_v3_llm_log_line,
    build_topic_prompt_v3_with_prompt_chars_limit,
    to_one_line_tail,
)


def map_topic_prompt_items_to_assertions(
    *,
    ai_result: dict[str, object],
    focus_username: str,
    message_lookup: dict[tuple[str, str], dict[str, object]],
    post_uid_by_platform_post_id: dict[str, str],
    max_assertions_per_post: int = 5,
) -> dict[str, list[dict[str, object]]]:
    focus = str(focus_username or "").strip()
    items = ai_result.get("items")
    if not isinstance(items, list):
        raise RuntimeError("ai_topic_items_missing")

    out: dict[str, list[dict[str, object]]] = {}
    for raw_item in items:
        if not isinstance(raw_item, dict):
            continue

        speaker = str(raw_item.get("speaker") or "").strip()
        if focus and speaker != focus:
            continue

        topic_key = str(raw_item.get("topic_key") or "").strip()
        if not topic_key:
            continue

        evidence_refs = raw_item.get("evidence_refs")
        refs = evidence_refs if isinstance(evidence_refs, list) else []
        first_ref = refs[0] if refs and isinstance(refs[0], dict) else {}
        source_kind = str(first_ref.get("source_kind") or "").strip()
        source_id = str(first_ref.get("source_id") or "").strip()
        quote = str(first_ref.get("quote") or "").strip()
        if not source_id:
            continue

        post_uid = post_uid_by_platform_post_id.get(source_id)
        if not post_uid:
            continue

        lookup_key = (source_kind, source_id)
        node = message_lookup.get(lookup_key)
        if node is None and source_id:
            node = message_lookup.get(("status", source_id))
        node_text = str((node or {}).get("text") or "")

        evidence = (
            quote
            if quote and node_text and quote in node_text
            else (node_text[:120] if node_text else quote)
        )
        if not evidence:
            continue

        summary = str(raw_item.get("summary") or "").strip() or "未提供摘要"
        confidence = _clamp_float(raw_item.get("confidence"), 0.0, 1.0, 0.5)
        action_strength = _clamp_int(raw_item.get("action_strength"), 0, 3, 1)
        action = normalize_action(
            str(raw_item.get("action") or "").strip() or "trade.watch"
        )

        row = {
            "topic_key": topic_key,
            "action": action,
            "action_strength": action_strength,
            "summary": summary,
            "evidence": evidence,
            "confidence": confidence,
            "stock_codes_json": json.dumps(
                as_str_list(raw_item.get("stock_codes")), ensure_ascii=False
            ),
            "stock_names_json": json.dumps(
                as_str_list(raw_item.get("stock_names")), ensure_ascii=False
            ),
            "industries_json": json.dumps(
                as_str_list(raw_item.get("industries")), ensure_ascii=False
            ),
            "commodities_json": json.dumps(
                as_str_list(raw_item.get("commodities")), ensure_ascii=False
            ),
            "indices_json": json.dumps(
                as_str_list(raw_item.get("indices")), ensure_ascii=False
            ),
        }
        bucket = out.setdefault(post_uid, [])
        if len(bucket) < max(0, int(max_assertions_per_post)):
            bucket.append(row)

    return out


def process_one_post_uid_topic_prompt_v3(
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
    post = (
        prefetched_post
        if prefetched_post is not None
        else load_cloud_post(engine, post_uid)
    )
    focus = str(post.author or "").strip()
    root_key, root_segment, root_content_key = thread_root_info_for_post(
        raw_text=post.raw_text or "",
        display_md=post.display_md or "",
        author=focus,
    )

    recent = (
        list(prefetched_recent or [])
        if prefetched_recent is not None
        else load_recent_posts_by_author(engine, author=focus, limit=200)
    )
    current_row = {
        "post_uid": post.post_uid,
        "platform_post_id": post.platform_post_id,
        "author": post.author,
        "created_at": post.created_at,
        "url": post.url,
        "raw_text": post.raw_text,
        "display_md": post.display_md,
        "processed_at": "",
        "ai_status": "running",
        "ai_retry_count": int(post.ai_retry_count or 0),
    }

    thread_rows: list[dict[str, object]] = []
    seen_uids: set[str] = set()
    for row in [current_row, *recent]:
        uid = str(row.get("post_uid") or "").strip()
        if not uid or uid in seen_uids:
            continue
        seen_uids.add(uid)

        is_current = uid == str(post.post_uid or "").strip()
        ai_status = str(row.get("ai_status") or "").strip().lower()
        if not is_current and ai_status not in {"pending", "error"}:
            continue

        rk, _seg, _ck = thread_root_info_for_post(
            raw_text=str(row.get("raw_text") or ""),
            display_md=str(row.get("display_md") or ""),
            author=str(row.get("author") or "").strip(),
        )
        if rk != root_key:
            continue
        thread_rows.append(row)

    post_count = len(thread_rows)
    thread_rows.sort(key=lambda r: str(r.get("created_at") or ""), reverse=True)
    kept = (
        thread_rows[:MAX_THREAD_POSTS] if post_count > MAX_THREAD_POSTS else thread_rows
    )
    if str(post.post_uid or "").strip() not in {
        str(r.get("post_uid") or "").strip() for r in kept
    }:
        kept = [current_row, *kept[: max(0, MAX_THREAD_POSTS - 1)]]
    trimmed_count = max(0, post_count - len(kept))

    locked_post_uids: list[str] = [post.post_uid]
    locked_set: set[str] = {post.post_uid}
    now_epoch = int(time.time())
    for row in kept:
        uid = str(row.get("post_uid") or "").strip()
        if not uid or uid in locked_set:
            continue
        ai_status = str(row.get("ai_status") or "").strip().lower()
        if ai_status not in {"pending", "error"}:
            continue
        try:
            if try_mark_ai_running(engine, post_uid=uid, now_epoch=now_epoch):
                locked_post_uids.append(uid)
                locked_set.add(uid)
        except Exception:
            continue

    (
        runtime_context,
        truncated_nodes,
        prompt,
        prompt_chars,
        node_chars,
        compact_json,
        include_comments,
    ) = build_topic_prompt_v3_with_prompt_chars_limit(
        root_key=root_key,
        root_segment=root_segment,
        root_content_key=root_content_key,
        focus_username=focus,
        posts=kept,
        max_prompt_chars=MAX_TOPIC_PROMPT_CHARS,
    )
    if (
        trimmed_count > 0
        or truncated_nodes > 0
        or compact_json
        or (not include_comments)
    ):
        print(
            " ".join(
                [
                    "[ai_topic] tree_trim",
                    f"author={focus or '(empty)'}",
                    f"root_key={root_key}",
                    f"post_count={post_count}",
                    f"trimmed_count={trimmed_count}",
                    f"max_nodes={MAX_THREAD_POSTS}",
                    f"prompt_chars={prompt_chars}",
                    f"max_prompt_chars={MAX_TOPIC_PROMPT_CHARS}",
                    f"compact_json={1 if compact_json else 0}",
                    f"comments={1 if include_comments else 0}",
                    f"node_chars={node_chars}",
                    f"truncated_nodes={truncated_nodes}",
                ]
            ),
            flush=True,
        )

    trace_label = f"topic:{root_key}"

    if config.verbose:
        print(
            build_topic_prompt_v3_llm_log_line(
                event="call_api",
                root_key=root_key,
                post_uid=str(post.post_uid or ""),
                author=focus,
                locked_count=len(locked_post_uids),
            ),
            flush=True,
        )

    retry_count_by_uid = {
        str(row.get("post_uid") or "").strip(): _clamp_int(
            row.get("ai_retry_count"),
            1,
            1000,
            1,
        )
        for row in kept
        if str(row.get("post_uid") or "").strip()
    }

    try:
        limiter.wait()
        start_ts = time.time()
        parsed = _call_ai_with_litellm(
            prompt=prompt,
            api_mode=str(config.api_mode or DEFAULT_AI_MODE),
            ai_stream=bool(config.ai_stream),
            model_name=str(config.model or DEFAULT_MODEL),
            base_url=str(config.base_url or ""),
            api_key=str(config.api_key or ""),
            timeout_seconds=float(config.ai_timeout_seconds),
            retry_count=int(config.ai_retries),
            temperature=float(config.ai_temperature),
            reasoning_effort=str(
                config.ai_reasoning_effort or DEFAULT_AI_REASONING_EFFORT
            ),
            trace_out=config.trace_out,
            trace_label=trace_label,
            validator=validate_topic_prompt_v3_ai_result,
        )

        if config.verbose:
            cost = time.time() - start_ts
            print(
                build_topic_prompt_v3_llm_log_line(
                    event="done",
                    root_key=root_key,
                    post_uid=str(post.post_uid or ""),
                    author=focus,
                    locked_count=len(locked_post_uids),
                    cost_seconds=cost,
                ),
                flush=True,
            )

        if not isinstance(parsed, dict):
            raise RuntimeError("ai_topic_invalid_json_root")

        message_lookup = runtime_context.get("message_lookup")
        if not isinstance(message_lookup, dict):
            raise RuntimeError("ai_topic_message_lookup_invalid")

        post_uid_by_pid = {
            str(row.get("platform_post_id") or "").strip(): str(
                row.get("post_uid") or ""
            ).strip()
            for row in kept
            if str(row.get("post_uid") or "").strip() in locked_set
        }
        assertions_by_post_uid = map_topic_prompt_items_to_assertions(
            ai_result=parsed,
            focus_username=focus,
            message_lookup=message_lookup,  # type: ignore[arg-type]
            post_uid_by_platform_post_id=post_uid_by_pid,
            max_assertions_per_post=5,
        )

        post_by_uid: dict[str, CloudPost] = {}
        platform_value = str(post.platform or "").strip() or "weibo"
        for row in kept:
            uid = str(row.get("post_uid") or "").strip()
            if not uid:
                continue
            post_by_uid[uid] = CloudPost(
                post_uid=uid,
                platform=platform_value,
                platform_post_id=str(row.get("platform_post_id") or "").strip(),
                author=str(row.get("author") or "").strip(),
                created_at=str(row.get("created_at") or "").strip(),
                url=str(row.get("url") or "").strip(),
                raw_text=str(row.get("raw_text") or ""),
                display_md=str(row.get("display_md") or ""),
                ai_retry_count=_clamp_int(
                    row.get("ai_retry_count"),
                    0,
                    1_000_000,
                    0,
                ),
            )

        with ExitStack() as stack:
            cache_conn = None
            try:
                db_path = resolve_local_cache_db_path(
                    source_name=str(source_name or "").strip()
                )
                cache_conn = stack.enter_context(open_local_cache(db_path=db_path))
            except Exception as cache_err:
                cache_conn = None
                if config.verbose:
                    print(
                        f"[local_cache] open_error post_uid={post_uid} "
                        f"{type(cache_err).__name__}: {cache_err}",
                        flush=True,
                    )

            for uid in locked_post_uids:
                rows = assertions_by_post_uid.get(uid, [])
                is_relevant = bool(rows)
                final_status = "relevant" if is_relevant else "irrelevant"
                invest_score = score_from_assertions(rows)
                processed_at = now_str()
                archived_at = now_str()
                post_for_outbox = post_by_uid.get(uid) or post
                outbox_payload = build_assertion_outbox_event_payload(
                    post=post_for_outbox,
                    final_status=final_status,
                    rows=rows,
                )
                if (
                    prefetched_post is not None
                    and uid == str(post.post_uid or "").strip()
                ):
                    ensure_prefetched_post_persisted(
                        engine=engine,
                        post=prefetched_post,
                        archived_at=archived_at,
                        ingested_at=int(time.time()),
                    )
                write_assertions_and_mark_done(
                    engine,
                    post_uid=uid,
                    final_status=final_status,
                    invest_score=invest_score,
                    processed_at=processed_at,
                    model=config.model,
                    prompt_version=config.prompt_version,
                    archived_at=archived_at,
                    ai_result_json=None,
                    assertions=rows,
                    outbox_source=str(outbox_source or "").strip(),
                    outbox_author=str(post_for_outbox.author or "").strip(),
                    outbox_event_json=json.dumps(outbox_payload, ensure_ascii=False),
                )
                if cache_conn is not None:
                    try:
                        apply_outbox_event_payload(cache_conn, payload=outbox_payload)
                    except Exception as write_err:
                        if config.verbose:
                            print(
                                f"[local_cache] write_error post_uid={uid} "
                                f"{type(write_err).__name__}: {write_err}",
                                flush=True,
                            )

                if rows:
                    try:
                        mark_stock_dirty_from_assertions(
                            engine,
                            assertions=rows,
                            reason="ai_done",
                        )
                    except BaseException:
                        if config.verbose:
                            print(
                                f"[stock_hot] mark_dirty_failed post_uid={uid}",
                                flush=True,
                            )
                    try:
                        mark_stock_backfill_dirty_from_assertions(
                            engine,
                            assertions=rows,
                            reason="ai_done",
                        )
                    except BaseException:
                        if config.verbose:
                            print(
                                f"[backfill_cache] mark_dirty_failed post_uid={uid}",
                                flush=True,
                            )
        return True
    except Exception as err:
        if isinstance(err, AiInvalidJsonError):
            raw_tail = to_one_line_tail(getattr(err, "raw_ai_text", ""), max_chars=240)
            print(
                " ".join(
                    [
                        "[ai_topic] invalid_json",
                        f"post_uid={post.post_uid}",
                        f"author={focus or '(empty)'}",
                        f"root_key={root_key}",
                        f"prompt_version={config.prompt_version}",
                        f"raw_ai_len={len(getattr(err, 'raw_ai_text', '') or '')}",
                        f"raw_ai_tail={raw_tail}",
                    ]
                ),
                flush=True,
            )

        base_url_for_log = (config.base_url or "").strip()
        if base_url_for_log:
            base_url_for_log = base_url_for_log.split("?", 1)[0].split("#", 1)[0]
            base_url_for_log = base_url_for_log[:220]
        ctx = (
            f" cfg_model={config.model}"
            f" api_mode={config.api_mode}"
            f" stream={1 if config.ai_stream else 0}"
            f" base_url={base_url_for_log or '(empty)'}"
            f" prompt_version={config.prompt_version}"
        )
        msg = f"ai:{format_llm_error_one_line(err, limit=700)}{ctx}"
        now_epoch = int(time.time())
        for uid in locked_post_uids:
            retry_count = retry_count_by_uid.get(uid, 1)
            next_retry = now_epoch + backoff_seconds(retry_count)
            try:
                mark_ai_error(
                    engine,
                    post_uid=uid,
                    error=msg,
                    next_retry_at=next_retry,
                    archived_at=now_str(),
                )
            except Exception:
                continue
        print(
            build_topic_prompt_v3_llm_log_line(
                event="error",
                root_key=root_key,
                post_uid=str(post.post_uid or ""),
                author=focus,
                locked_count=len(locked_post_uids),
                message=msg,
            ),
            flush=True,
        )
        return False


__all__ = [
    "map_topic_prompt_items_to_assertions",
    "process_one_post_uid_topic_prompt_v3",
]
