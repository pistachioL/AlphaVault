from __future__ import annotations
import logging
import threading
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
from alphavault.ai.tag_validate import validate_topic_prompt_v4_ai_result
from alphavault.db.analysis_feedback import (
    load_latest_pending_feedback,
    mark_feedback_applied,
)
from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    run_postgres_transaction,
)
from alphavault.db.turso_queue import (
    CloudPost,
    load_cloud_post,
    write_assertions_and_mark_done,
)
from alphavault.domains.common.assertion_entities import build_assertion_entities
from alphavault.domains.entity_match import (
    EntityMatchResult,
    load_entity_match_lookup_maps,
    resolve_assertion_mentions,
)
from alphavault.rss.utils import RateLimiter, now_str
from alphavault.research_workbench.service import (
    get_research_workbench_engine_from_env,
)
from alphavault.research_stock_cache import mark_entity_page_dirty_from_assertions
from alphavault.weibo.topic_prompt_tree import (
    MAX_TOPIC_PROMPT_CHARS,
    thread_root_info_for_post,
)
from alphavault.worker.post_processor_utils import score_from_assertions
from alphavault.worker.runtime_models import LLMConfig, _clamp_float, _clamp_int
from alphavault.worker.topic_prompt_v4 import (
    build_topic_prompt_v4_llm_log_line,
    build_topic_prompt_v4_with_prompt_chars_limit,
    to_one_line_tail,
)
from alphavault.logging_config import get_logger

MAX_MANUAL_FEEDBACK_HINT_NOTE_CHARS = 300


def _build_manual_feedback_hint(
    feedback_row: dict[str, str] | None,
) -> dict[str, object] | None:
    if not isinstance(feedback_row, dict):
        return None
    feedback_tag = str(feedback_row.get("feedback_tag") or "").strip()
    feedback_note = str(feedback_row.get("feedback_note") or "").strip()
    if len(feedback_note) > MAX_MANUAL_FEEDBACK_HINT_NOTE_CHARS:
        feedback_note = feedback_note[:MAX_MANUAL_FEEDBACK_HINT_NOTE_CHARS].rstrip()
    submitted_at = str(feedback_row.get("submitted_at") or "").strip()
    if not feedback_tag and not feedback_note:
        return None
    return {
        "feedback_tag": feedback_tag,
        "feedback_note": feedback_note,
        "submitted_at": submitted_at,
    }


_TOPIC_PROMPT_TRACE_CONTEXT = threading.local()
logger = get_logger(__name__)


def _trace_log_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    return " ".join(str(value or "").split()).strip()


def set_topic_prompt_trace_context(
    *,
    trace_id: str,
    consumer_name: str,
    message_id: str,
    redis_queue_key: str,
    source_name: str,
) -> None:
    _TOPIC_PROMPT_TRACE_CONTEXT.value = {
        "trace_id": str(trace_id or "").strip(),
        "consumer": str(consumer_name or "").strip(),
        "message_id": str(message_id or "").strip(),
        "queue": str(redis_queue_key or "").strip(),
        "source": str(source_name or "").strip(),
    }


def clear_topic_prompt_trace_context() -> None:
    if hasattr(_TOPIC_PROMPT_TRACE_CONTEXT, "value"):
        delattr(_TOPIC_PROMPT_TRACE_CONTEXT, "value")


def _topic_prompt_trace_suffix() -> str:
    raw_context = getattr(_TOPIC_PROMPT_TRACE_CONTEXT, "value", None)
    if not isinstance(raw_context, dict):
        return ""
    parts: list[str] = []
    for key in ("trace_id", "consumer", "message_id", "queue", "source"):
        text = _trace_log_value(raw_context.get(key))
        if text:
            parts.append(f"{key}={text}")
    return " ".join(parts)


def _append_diag_field(parts: list[str], key: str, value: object) -> None:
    text = _trace_log_value(value)
    if text:
        parts.append(f"{key}={text}")


def _build_ai_topic_diag_log_line(
    *,
    event: str,
    post_uid: str,
    author: str = "",
    root_key: str = "",
    ai_retry_count: int | None = None,
    prompt_chars: int | None = None,
    compact_json: bool | None = None,
    comments: bool | None = None,
    truncated_nodes: int | None = None,
    final_status: str = "",
    assertion_count: int | None = None,
    invest_score: float | None = None,
) -> str:
    parts = ["[ai_topic]", f"event={_trace_log_value(event)}"]
    _append_diag_field(parts, "post_uid", post_uid)
    _append_diag_field(parts, "author", author)
    _append_diag_field(parts, "root_key", root_key)
    if ai_retry_count is not None:
        _append_diag_field(parts, "ai_retry_count", int(ai_retry_count))
    if prompt_chars is not None:
        _append_diag_field(parts, "prompt_chars", int(prompt_chars))
    if compact_json is not None:
        _append_diag_field(parts, "compact_json", bool(compact_json))
    if comments is not None:
        _append_diag_field(parts, "comments", bool(comments))
    if truncated_nodes is not None:
        _append_diag_field(parts, "truncated_nodes", int(truncated_nodes))
    _append_diag_field(parts, "final_status", final_status)
    if assertion_count is not None:
        _append_diag_field(parts, "assertion_count", int(assertion_count))
    if invest_score is not None:
        _append_diag_field(parts, "invest_score", invest_score)
    trace_suffix = _topic_prompt_trace_suffix()
    if trace_suffix:
        parts.append(trace_suffix)
    return " ".join(parts)


def _build_top_level_mentions_lookup(
    ai_result: dict[str, object],
) -> dict[str, dict[str, object]]:
    mentions = ai_result.get("mentions")
    if not isinstance(mentions, list):
        raise RuntimeError("ai_topic_mentions_missing")
    out: dict[str, dict[str, object]] = {}
    for raw_mention in mentions:
        if not isinstance(raw_mention, dict):
            continue
        mention_text = str(raw_mention.get("mention_text") or "").strip()
        if not mention_text or mention_text in out:
            continue
        out[mention_text] = {
            "mention_text": mention_text,
            "mention_norm": mention_text,
            "mention_type": str(raw_mention.get("mention_type") or "").strip(),
            "evidence": str(raw_mention.get("evidence") or "").strip(),
            "confidence": _clamp_float(raw_mention.get("confidence"), 0.0, 1.0, 0.0),
        }
    return out


def _clip_text(value: object, *, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, int(limit))].rstrip()


def _write_done_with_feedback_apply(
    *,
    engine: PostgresEngine,
    post_uid: str,
    final_status: str,
    invest_score: float,
    processed_at: str,
    model: str,
    prompt_version: str,
    archived_at: str,
    assertions: list[dict[str, object]],
    entity_match_results: list[EntityMatchResult],
    prefetched_post: CloudPost | None,
    prefetched_ingested_at: int,
    latest_pending_feedback: dict[str, str] | None,
) -> None:
    if latest_pending_feedback is None:
        write_assertions_and_mark_done(
            engine,
            post_uid=post_uid,
            final_status=final_status,
            invest_score=invest_score,
            processed_at=processed_at,
            model=model,
            prompt_version=prompt_version,
            archived_at=archived_at,
            assertions=assertions,
            entity_match_results=entity_match_results,
            prefetched_post=prefetched_post,
            prefetched_ingested_at=prefetched_ingested_at,
        )
        return

    feedback_id = ""
    feedback_id = str(latest_pending_feedback.get("feedback_id") or "").strip()
    if not feedback_id:
        raise RuntimeError(f"feedback_id_missing:{post_uid}")

    def _write(conn: PostgresConnection) -> None:
        if feedback_id:
            updated = mark_feedback_applied(
                conn,
                feedback_id=feedback_id,
                applied_at=processed_at,
            )
            if updated != 1:
                raise RuntimeError(f"feedback_apply_missing:{post_uid}")
        write_assertions_and_mark_done(
            conn,
            post_uid=post_uid,
            final_status=final_status,
            invest_score=invest_score,
            processed_at=processed_at,
            model=model,
            prompt_version=prompt_version,
            archived_at=archived_at,
            assertions=assertions,
            entity_match_results=entity_match_results,
            prefetched_post=prefetched_post,
            prefetched_ingested_at=prefetched_ingested_at,
        )

    run_postgres_transaction(engine, _write)


def map_topic_prompt_assertions_to_rows(
    *,
    ai_result: dict[str, object],
    focus_username: str,
    message_lookup: dict[tuple[str, str], dict[str, object]],
    post_uid_by_platform_post_id: dict[str, str],
    fallback_post_uid: str = "",
    max_assertions_per_post: int = 5,
) -> dict[str, list[dict[str, object]]]:
    focus = str(focus_username or "").strip()
    assertions = ai_result.get("assertions")
    if not isinstance(assertions, list):
        raise RuntimeError("ai_topic_assertions_missing")

    mention_lookup = _build_top_level_mentions_lookup(ai_result)
    out: dict[str, list[dict[str, object]]] = {}
    for raw_assertion in assertions:
        if not isinstance(raw_assertion, dict):
            continue

        speaker = str(raw_assertion.get("speaker") or "").strip()
        if focus and speaker != focus:
            continue

        evidence_refs = raw_assertion.get("evidence_refs")
        refs = evidence_refs if isinstance(evidence_refs, list) else []
        first_ref = refs[0] if refs and isinstance(refs[0], dict) else {}
        source_kind = str(first_ref.get("source_kind") or "").strip()
        source_id = str(first_ref.get("source_id") or "").strip()
        quote = str(first_ref.get("quote") or "").strip()
        if not source_id:
            continue

        lookup_key = (source_kind, source_id)
        node = message_lookup.get(lookup_key)
        if node is None and source_id:
            node = message_lookup.get(("status", source_id))
        if node is None:
            continue

        post_uid = post_uid_by_platform_post_id.get(source_id)
        if not post_uid:
            allow_fallback = source_kind in {"talk_reply", "topic_post"}
            if allow_fallback and str(fallback_post_uid or "").strip():
                post_uid = str(fallback_post_uid or "").strip()
            else:
                continue
        node_text = str((node or {}).get("text") or "")

        evidence = (
            quote
            if quote and node_text and quote in node_text
            else (node_text[:120] if node_text else quote)
        )
        if not evidence:
            continue

        mention_texts = raw_assertion.get("mentions")
        mention_refs = mention_texts if isinstance(mention_texts, list) else []
        assertion_mentions = [
            dict(mention_lookup[mention_text])
            for mention_text in mention_refs
            if str(mention_text or "").strip() in mention_lookup
        ]
        if not assertion_mentions:
            continue

        row = {
            "action": normalize_action(
                str(raw_assertion.get("action") or "").strip() or "trade.watch"
            ),
            "action_strength": _clamp_int(
                raw_assertion.get("action_strength"), 0, 3, 1
            ),
            "summary": str(raw_assertion.get("summary") or "").strip() or "未提供摘要",
            "evidence": evidence,
            "created_at": "",
            "assertion_mentions": assertion_mentions,
            "assertion_entities": build_assertion_entities(assertion_mentions),
            "source_text_excerpt": _clip_text(node_text, limit=220),
        }
        bucket = out.setdefault(post_uid, [])
        if len(bucket) < max(0, int(max_assertions_per_post)):
            bucket.append(row)

    return out


def resolve_rows_entity_matches(
    engine_or_conn,
    rows_by_post_uid: dict[str, list[dict[str, object]]],
) -> dict[str, list[EntityMatchResult]]:
    stock_name_texts: list[str] = []
    stock_alias_texts: list[str] = []
    seen_stock_names: set[str] = set()
    seen_stock_aliases: set[str] = set()
    for rows in rows_by_post_uid.values():
        for row in rows:
            raw_mentions = row.get("assertion_mentions")
            assertion_mentions = raw_mentions if isinstance(raw_mentions, list) else []
            for item in assertion_mentions:
                if not isinstance(item, dict):
                    continue
                mention_text = str(item.get("mention_text") or "").strip()
                mention_type = str(item.get("mention_type") or "").strip()
                if not mention_text:
                    continue
                if (
                    mention_type == "stock_name"
                    and mention_text not in seen_stock_names
                ):
                    seen_stock_names.add(mention_text)
                    stock_name_texts.append(mention_text)
                    continue
                if (
                    mention_type == "stock_alias"
                    and mention_text not in seen_stock_aliases
                ):
                    seen_stock_aliases.add(mention_text)
                    stock_alias_texts.append(mention_text)
    stock_name_targets: dict[str, str] = {}
    stock_alias_targets: dict[str, str] = {}
    if stock_name_texts or stock_alias_texts:
        standard_engine = get_research_workbench_engine_from_env()
        stock_name_targets, stock_alias_targets = load_entity_match_lookup_maps(
            standard_engine,
            stock_name_texts=stock_name_texts,
            stock_alias_texts=stock_alias_texts,
        )
    followups_by_post_uid: dict[str, list[EntityMatchResult]] = {}
    for post_uid, rows in rows_by_post_uid.items():
        post_followups: list[EntityMatchResult] = []
        for row in rows:
            raw_mentions = row.get("assertion_mentions")
            assertion_mentions = raw_mentions if isinstance(raw_mentions, list) else []
            match_result = resolve_assertion_mentions(
                engine_or_conn,
                assertion_mentions=assertion_mentions,
                stock_name_targets=stock_name_targets,
                stock_alias_targets=stock_alias_targets,
                alias_task_sample={
                    "sample_post_uid": post_uid,
                    "sample_evidence": str(row.get("evidence") or "").strip(),
                    "sample_raw_text_excerpt": str(
                        row.get("source_text_excerpt") or row.get("evidence") or ""
                    ).strip(),
                },
            )
            row["assertion_entities"] = match_result.entities
            if match_result.relation_candidates or match_result.alias_task_keys:
                post_followups.append(match_result)
        followups_by_post_uid[post_uid] = post_followups
    return followups_by_post_uid


def process_one_post_uid_topic_prompt_v4(
    *,
    engine: PostgresEngine,
    post_uid: str,
    config: LLMConfig,
    limiter: RateLimiter,
    prefetched_post: CloudPost | None = None,
    prefetched_recent: list[dict[str, object]] | None = None,
    source_name: str = "",
) -> bool:
    debug_enabled = logger.isEnabledFor(logging.DEBUG)
    if debug_enabled:
        logger.debug(
            _build_ai_topic_diag_log_line(
                event="load_post_start",
                post_uid=str(post_uid or ""),
            )
        )
    post = (
        prefetched_post
        if prefetched_post is not None
        else load_cloud_post(engine, post_uid)
    )
    latest_pending_feedback = load_latest_pending_feedback(engine, post_uid=post_uid)
    manual_feedback_hint = _build_manual_feedback_hint(latest_pending_feedback)
    if debug_enabled:
        logger.debug(
            _build_ai_topic_diag_log_line(
                event="load_post_done",
                post_uid=str(post.post_uid or ""),
                author=str(post.author or "").strip(),
                ai_retry_count=int(post.ai_retry_count or 0),
            )
        )
    focus = str(post.author or "").strip()
    root_key, root_segment, root_content_key = thread_root_info_for_post(
        raw_text=post.raw_text or "",
        author=focus,
    )
    current_row = {
        "post_uid": post.post_uid,
        "platform_post_id": post.platform_post_id,
        "author": post.author,
        "created_at": post.created_at,
        "url": post.url,
        "raw_text": post.raw_text,
        "processed_at": "",
        "ai_status": "running",
        "ai_retry_count": int(post.ai_retry_count or 0),
    }

    if debug_enabled:
        logger.debug(
            _build_ai_topic_diag_log_line(
                event="start",
                post_uid=str(post.post_uid or ""),
                author=focus,
                root_key=root_key,
                ai_retry_count=int(post.ai_retry_count or 0),
            )
        )

    kept = [current_row]
    post_count = 1
    trimmed_count = 0

    locked_post_uids: list[str] = [post.post_uid]

    (
        runtime_context,
        truncated_nodes,
        prompt,
        prompt_chars,
        node_chars,
        compact_json,
        include_comments,
    ) = build_topic_prompt_v4_with_prompt_chars_limit(
        root_key=root_key,
        root_segment=root_segment,
        root_content_key=root_content_key,
        focus_username=focus,
        posts=kept,
        manual_feedback_hint=manual_feedback_hint,
        max_prompt_chars=MAX_TOPIC_PROMPT_CHARS,
    )
    if (
        trimmed_count > 0
        or truncated_nodes > 0
        or compact_json
        or (not include_comments)
    ):
        logger.debug(
            " ".join(
                [
                    "[ai_topic] tree_trim",
                    f"author={focus or '(empty)'}",
                    f"root_key={root_key}",
                    f"post_count={post_count}",
                    f"trimmed_count={trimmed_count}",
                    "max_nodes=1",
                    f"prompt_chars={prompt_chars}",
                    f"max_prompt_chars={MAX_TOPIC_PROMPT_CHARS}",
                    f"compact_json={1 if compact_json else 0}",
                    f"comments={1 if include_comments else 0}",
                    f"node_chars={node_chars}",
                    f"truncated_nodes={truncated_nodes}",
                ]
            ),
        )

    trace_label = f"topic:{root_key}"

    if debug_enabled:
        logger.debug(
            _build_ai_topic_diag_log_line(
                event="prompt_ready",
                post_uid=str(post.post_uid or ""),
                author=focus,
                root_key=root_key,
                prompt_chars=int(prompt_chars),
                compact_json=bool(compact_json),
                comments=bool(include_comments),
                truncated_nodes=int(truncated_nodes),
            )
        )

    logger.info(
        build_topic_prompt_v4_llm_log_line(
            event="call_api",
            root_key=root_key,
            post_uid=str(post.post_uid or ""),
            author=focus,
            locked_count=len(locked_post_uids),
            message=_topic_prompt_trace_suffix(),
        )
    )

    try:
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
            validator=validate_topic_prompt_v4_ai_result,
            request_gate=limiter.wait,
        )

        cost = time.time() - start_ts
        logger.info(
            build_topic_prompt_v4_llm_log_line(
                event="done",
                root_key=root_key,
                post_uid=str(post.post_uid or ""),
                author=focus,
                locked_count=len(locked_post_uids),
                cost_seconds=cost,
                message=_topic_prompt_trace_suffix(),
            )
        )

        if not isinstance(parsed, dict):
            raise RuntimeError("ai_topic_invalid_json_root")

        message_lookup = runtime_context.get("message_lookup")
        if not isinstance(message_lookup, dict):
            raise RuntimeError("ai_topic_message_lookup_invalid")

        post_uid_by_pid = {
            str(post.platform_post_id or "").strip(): str(post.post_uid or "").strip()
        }
        assertions_by_post_uid = map_topic_prompt_assertions_to_rows(
            ai_result=parsed,
            focus_username=focus,
            message_lookup=message_lookup,  # type: ignore[arg-type]
            post_uid_by_platform_post_id=post_uid_by_pid,
            fallback_post_uid=str(post.post_uid or "").strip(),
            max_assertions_per_post=5,
        )
        entity_match_results_by_post_uid = resolve_rows_entity_matches(
            engine,
            assertions_by_post_uid,
        )

        for uid in locked_post_uids:
            rows = assertions_by_post_uid.get(uid, [])
            is_relevant = bool(rows)
            final_status = "relevant" if is_relevant else "irrelevant"
            invest_score = score_from_assertions(rows)
            processed_at = now_str()
            archived_at = now_str()
            if debug_enabled:
                logger.debug(
                    _build_ai_topic_diag_log_line(
                        event="db_write_start",
                        post_uid=uid,
                        author=focus,
                        root_key=root_key,
                        final_status=final_status,
                        assertion_count=len(rows),
                        invest_score=float(invest_score),
                    )
                )
            _write_done_with_feedback_apply(
                engine=engine,
                post_uid=uid,
                final_status=final_status,
                invest_score=invest_score,
                processed_at=processed_at,
                model=config.model,
                prompt_version=config.prompt_version,
                archived_at=archived_at,
                assertions=rows,
                entity_match_results=entity_match_results_by_post_uid.get(uid, []),
                prefetched_post=(
                    prefetched_post
                    if prefetched_post is not None
                    and uid == str(post.post_uid or "").strip()
                    else None
                ),
                prefetched_ingested_at=int(time.time()),
                latest_pending_feedback=latest_pending_feedback,
            )
            logger.info(
                _build_ai_topic_diag_log_line(
                    event="db_write_done",
                    post_uid=uid,
                    author=focus,
                    root_key=root_key,
                    final_status=final_status,
                    assertion_count=len(rows),
                    invest_score=float(invest_score),
                )
            )

            if rows:
                try:
                    mark_entity_page_dirty_from_assertions(
                        engine,
                        assertions=rows,
                        reason="ai_done",
                    )
                except BaseException:
                    logger.warning("[stock_hot] mark_dirty_failed post_uid=%s", uid)
        return True
    except Exception as err:
        if isinstance(err, AiInvalidJsonError):
            raw_tail = to_one_line_tail(getattr(err, "raw_ai_text", ""), max_chars=240)
            logger.info(
                " ".join(
                    [
                        "[ai_topic] invalid_json",
                        f"post_uid={post.post_uid}",
                        f"author={focus or '(empty)'}",
                        f"root_key={root_key}",
                        f"prompt_version={config.prompt_version}",
                        f"raw_ai_len={len(getattr(err, 'raw_ai_text', '') or '')}",
                        f"raw_ai_tail={raw_tail}",
                        _topic_prompt_trace_suffix(),
                    ]
                ),
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
        trace_suffix = _topic_prompt_trace_suffix()
        if trace_suffix:
            msg = f"{msg} {trace_suffix}"
        logger.info(
            build_topic_prompt_v4_llm_log_line(
                event="error",
                root_key=root_key,
                post_uid=str(post.post_uid or ""),
                author=focus,
                locked_count=len(locked_post_uids),
                message=msg,
            )
        )
        return False


__all__ = [
    "clear_topic_prompt_trace_context",
    "map_topic_prompt_assertions_to_rows",
    "process_one_post_uid_topic_prompt_v4",
    "resolve_rows_entity_matches",
    "set_topic_prompt_trace_context",
]
