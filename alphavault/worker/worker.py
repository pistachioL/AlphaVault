from __future__ import annotations

import argparse
import json
import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional, Sequence, Tuple

from sqlalchemy.engine import Engine

from alphavault.constants import (
    DEFAULT_RSS_FEED_SLEEP_SECONDS,
    ENV_AI_API_KEY,
    ENV_AI_STREAM,
    ENV_AI_TRACE_OUT,
    ENV_RSS_ACTIVE_HOURS,
    ENV_RSS_FEED_SLEEP_SECONDS,
    ENV_RSS_INTERVAL_SECONDS,
    ENV_WORKER_STOCK_HOT_CACHE_INTERVAL_SECONDS,
    ENV_WORKER_STOCK_ALIAS_SYNC_INTERVAL_SECONDS,
)
from alphavault.ai.analyze import (
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_MODEL,
    DEFAULT_PROMPT_VERSION,
    AnalyzeResult,
    _call_ai_with_litellm,
    analyze_with_litellm,
    format_llm_error_one_line,
    normalize_action,
    validate_and_adjust_assertions,
)
from alphavault.ai._client import AiInvalidJsonError
from alphavault.ai.tag_validate import validate_topic_prompt_v3_ai_result
from alphavault.ai.topic_prompt_v3 import TOPIC_PROMPT_VERSION, build_topic_prompt
from alphavault.db.turso_db import (
    ensure_turso_engine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
)
from alphavault.db.turso_queue import (
    CloudPost,
    ensure_cloud_queue_schema,
    load_cloud_post,
    load_recent_posts_by_author,
    mark_ai_error,
    recover_done_without_processed_at,
    recover_stuck_ai_tasks,
    select_due_post_uids,
    try_mark_ai_running,
    upsert_pending_post,
    write_assertions_and_mark_done,
)
from alphavault.rss.utils import (
    CST,
    RateLimiter,
    build_analysis_context,
    build_row_meta,
    env_bool,
    env_float,
    in_active_hours,
    now_str,
    parse_active_hours,
    sleep_until_active,
)
from alphavault.worker.cli import (
    _parse_worker_active_hours_from_args,
    _resolve_worker_interval_seconds,
    parse_args,
    resolve_rss_source_configs,
)
from alphavault.worker.ingest import ingest_rss_many_once
from alphavault.worker.redis_queue import (
    redis_ai_ack_processing,
    redis_ai_ack_and_cleanup,
    redis_ai_due_count,
    redis_ai_move_due_delayed_to_ready,
    redis_ai_pop_to_processing,
    redis_ai_push_delayed,
    redis_ai_requeue_processing,
    try_get_redis,
)
from alphavault.worker.spool import ensure_spool_dir, flush_spool_to_turso
from alphavault.worker.research_backfill_cache import sync_stock_backfill_cache
from alphavault.worker.research_relation_candidates_cache import (
    sync_relation_candidates_cache,
)
from alphavault.worker.research_stock_cache import sync_stock_hot_cache
from alphavault.worker.stock_alias_sync import sync_stock_alias_relations
from alphavault.worker.job_state import (
    save_worker_job_cursor,
    worker_progress_state_key,
    WORKER_PROGRESS_STAGE_AI,
    WORKER_PROGRESS_STAGE_ALIAS,
    WORKER_PROGRESS_STAGE_BACKFILL,
    WORKER_PROGRESS_STAGE_CYCLE,
    WORKER_PROGRESS_STAGE_RELATION,
    WORKER_PROGRESS_STAGE_STOCK_HOT,
)
from alphavault.research_stock_cache import mark_stock_dirty_from_assertions
from alphavault_reflex.services.stock_objects import AiRuntimeConfig

from alphavault.weibo.display import format_weibo_display_md
from alphavault.weibo.topic_prompt_tree import (
    MAX_THREAD_POSTS,
    MAX_TOPIC_PROMPT_CHARS,
    build_topic_runtime_context,
    thread_root_info_for_post,
)

TURSO_READY_RETRY_SECONDS = 5.0
_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
LOW_PRIORITY_SCHEDULER_MODE = "rss_priority_fill"
WORKER_PROGRESS_STATUS_IDLE = "idle"
WORKER_PROGRESS_STATUS_RUNNING = "running"
LLM_LOG_PREFIX = "[llm]"
TOPIC_PROMPT_V3_LABEL = "topic_prompt_v3"
LOG_EMPTY_VALUE = "(empty)"
BACKFILL_MAX_STOCKS_PER_RUN_CAP = 32
SPOOL_FLUSH_MAX_ITEMS_PER_RUN = 200
SPOOL_FLUSH_RETRY_INTERVAL_SECONDS = 1.0


@dataclass(frozen=True)
class WorkerSourceConfig:
    name: str
    platform: str
    rss_urls: list[str]
    author: str
    user_id: Optional[str]
    database_url: str
    auth_token: str


@dataclass
class WorkerSourceRuntime:
    config: WorkerSourceConfig
    engine: Engine
    spool_dir: Path
    redis_queue_key: str
    rss_next_ingest_at: float
    rss_ingest_future: Future | None = None
    spool_flush_future: Future | None = None
    spool_flush_next_at: float = 0.0
    spool_seq_written: int = 0
    spool_seq_scheduled: int = 0
    spool_need_retry: bool = False
    spool_state_lock: threading.Lock = field(default_factory=threading.Lock)
    turso_ready: bool = False
    turso_next_ready_check_at: float = 0.0
    alias_sync_future: Future | None = None
    alias_sync_next_at: float = 0.0
    backfill_cache_future: Future | None = None
    backfill_cache_next_at: float = 0.0
    relation_cache_future: Future | None = None
    relation_cache_next_at: float = 0.0
    stock_hot_cache_future: Future | None = None
    stock_hot_cache_next_at: float = 0.0
    cycle_running: bool = False
    cycle_started_at: float = 0.0
    cycle_finished_at: float = 0.0


def _clamp_float(value: object, low: float, high: float, default: float) -> float:
    try:
        v = float(str(value).strip())
    except Exception:
        return float(default)
    return float(max(low, min(high, v)))


def _clamp_int(value: object, low: int, high: int, default: int) -> int:
    try:
        v = int(str(value).strip())
    except Exception:
        return int(default)
    return int(max(low, min(high, v)))


def _parse_int_or_default(value: object, default: int) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return int(default)
    try:
        return int(text)
    except Exception:
        return int(default)


def _score_from_assertions(rows: list[dict[str, object]]) -> float:
    if not rows:
        return 0.0
    scores: list[float] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        confidence = _clamp_float(row.get("confidence", 0.0), 0.0, 1.0, 0.0)
        strength = _clamp_int(row.get("action_strength", 1), 0, 3, 1)
        strength_weight = strength / 3.0
        scores.append(0.7 * confidence + 0.3 * strength_weight)
    return max(scores) if scores else 0.0


@dataclass
class LLMConfig:
    api_key: str
    model: str
    prompt_version: str
    relevant_threshold: float
    base_url: str
    api_mode: str
    ai_stream: bool
    ai_retries: int
    ai_temperature: float
    ai_reasoning_effort: str
    ai_rpm: float
    ai_timeout_seconds: float
    trace_out: Optional[Path]
    verbose: bool


def _build_config(args: argparse.Namespace) -> LLMConfig:
    ai_stream_env = env_bool(ENV_AI_STREAM)
    ai_stream = True
    if ai_stream_env is not None:
        ai_stream = bool(ai_stream_env)
    elif args.ai_stream:
        ai_stream = True

    trace_out = args.trace_out
    trace_out_env = os.getenv(ENV_AI_TRACE_OUT, "").strip()
    if trace_out_env and trace_out is None:
        trace_out = Path(trace_out_env)

    base_url = str(args.base_url or "").strip()
    if bool(args.verbose) and base_url:
        if not base_url.rstrip("/").endswith("/v1"):
            print(
                f"[ai] warn base_url_maybe_missing_v1 base_url={base_url}", flush=True
            )

    api_key = ""
    if args.api_key:
        api_key = str(args.api_key).strip()
    else:
        api_key = os.getenv(ENV_AI_API_KEY, "").strip()
    if not api_key:
        raise RuntimeError(f"Missing {ENV_AI_API_KEY}. Set {ENV_AI_API_KEY}.")

    return LLMConfig(
        api_key=api_key,
        model=str(args.model or DEFAULT_MODEL),
        prompt_version=str(args.prompt_version or DEFAULT_PROMPT_VERSION),
        relevant_threshold=max(0.0, min(1.0, float(args.relevant_threshold))),
        base_url=str(base_url or ""),
        api_mode=str(args.api_mode or DEFAULT_AI_MODE),
        ai_stream=ai_stream,
        ai_retries=max(0, int(args.ai_retries)),
        ai_temperature=float(args.ai_temperature),
        ai_reasoning_effort=str(
            args.ai_reasoning_effort or DEFAULT_AI_REASONING_EFFORT
        ),
        ai_rpm=max(0.0, float(args.ai_rpm or 0.0)),
        ai_timeout_seconds=max(1.0, float(args.ai_timeout_sec)),
        trace_out=trace_out,
        verbose=bool(args.verbose),
    )


def _backoff_seconds(retry_count: int) -> int:
    n = max(1, int(retry_count))
    delay = 30 * (2 ** max(0, n - 1))
    return int(min(3600, delay))


def _to_one_line_tail(value: str, *, max_chars: int) -> str:
    s = str(value or "")
    s = " ".join(s.split())
    if max_chars <= 0 or len(s) <= max_chars:
        return s
    return s[-max_chars:]


def _clean_log_value(value: object) -> str:
    text = " ".join(str(value or "").split())
    return text if text else LOG_EMPTY_VALUE


def _build_topic_prompt_v3_llm_log_line(
    *,
    event: str,
    root_key: str,
    post_uid: str,
    author: str,
    locked_count: int,
    cost_seconds: Optional[float] = None,
    message: str = "",
) -> str:
    parts = [
        f"{LLM_LOG_PREFIX} {event} {TOPIC_PROMPT_V3_LABEL}",
        f"root_key={_clean_log_value(root_key)}",
        f"post_uid={_clean_log_value(post_uid)}",
        f"author={_clean_log_value(author)}",
        f"locked={max(0, int(locked_count))}",
    ]
    if cost_seconds is not None:
        parts.append(f"cost={float(cost_seconds):.1f}s")
    if message:
        parts.append(str(message))
    return " ".join(parts)


def _max_message_tree_text_len(node: object) -> int:
    if not isinstance(node, dict):
        return 0
    max_len = len(str(node.get("text") or ""))
    children = node.get("children")
    if isinstance(children, list):
        for child in children:
            max_len = max(max_len, _max_message_tree_text_len(child))
    return max_len


def _build_topic_prompt_v3_with_prompt_chars_limit(
    *,
    root_key: str,
    root_segment: str,
    root_content_key: str,
    focus_username: str,
    posts: list[dict[str, object]],
    max_prompt_chars: int,
) -> tuple[dict[str, object], int, str, int, int, bool, bool]:
    """
    Build a topic-prompt-v3 prompt with a hard prompt chars budget.

    Returns:
      (runtime_context, truncated_nodes, prompt, prompt_chars, node_chars_limit, compact_json, include_comments)
    """

    def build_ctx(
        *, node_chars: int, include_comments: bool
    ) -> tuple[dict[str, object], int]:
        return build_topic_runtime_context(
            root_key=root_key,
            root_segment=root_segment,
            root_content_key=root_content_key,
            focus_username=focus_username,
            posts=posts,
            include_virtual_comments=bool(include_comments),
            max_node_text_chars=int(node_chars),
        )

    def build_prompt(ctx: dict[str, object], *, compact_json: bool) -> tuple[str, int]:
        pkg = ctx.get("ai_topic_package")
        if not isinstance(pkg, dict):
            raise RuntimeError("ai_topic_package_invalid")
        p = build_topic_prompt(ai_topic_package=pkg, compact_json=bool(compact_json))
        return p, len(p)

    def search_best_cap(
        *, include_comments: bool
    ) -> Optional[tuple[dict[str, object], int, str, int, int]]:
        base_ctx, _base_truncated = build_ctx(
            node_chars=0, include_comments=include_comments
        )
        max_len = max(1, _max_message_tree_text_len(base_ctx.get("message_tree")))
        lo = 1
        hi = int(max_len)
        best: Optional[tuple[dict[str, object], int, str, int, int]] = None
        while lo <= hi:
            mid = (lo + hi) // 2
            mid_ctx, mid_truncated = build_ctx(
                node_chars=mid, include_comments=include_comments
            )
            mid_prompt, mid_chars = build_prompt(mid_ctx, compact_json=True)
            if mid_chars <= max_prompt_chars:
                best = (
                    mid_ctx,
                    int(mid_truncated),
                    mid_prompt,
                    int(mid_chars),
                    int(mid),
                )
                lo = mid + 1
                continue
            hi = mid - 1
        return best

    # 1) Full context + pretty JSON (readable)
    ctx_full, truncated_full = build_ctx(node_chars=0, include_comments=True)
    pretty_prompt, pretty_chars = build_prompt(ctx_full, compact_json=False)
    if max_prompt_chars <= 0 or pretty_chars <= max_prompt_chars:
        return (
            ctx_full,
            int(truncated_full),
            pretty_prompt,
            int(pretty_chars),
            0,
            False,
            True,
        )

    # 2) Full context + compact JSON (save chars on whitespace)
    compact_prompt, compact_chars = build_prompt(ctx_full, compact_json=True)
    if compact_chars <= max_prompt_chars:
        return (
            ctx_full,
            int(truncated_full),
            compact_prompt,
            int(compact_chars),
            0,
            True,
            True,
        )

    # 3) Full context + compact JSON + per-node cap
    best = search_best_cap(include_comments=True)
    if best is not None:
        best_ctx, best_truncated, best_prompt, best_prompt_chars, best_cap = best
        return (
            best_ctx,
            best_truncated,
            best_prompt,
            best_prompt_chars,
            best_cap,
            True,
            True,
        )

    # 4) Fallback: remove virtual comment nodes (keep only status nodes)
    ctx_no_comments, truncated_nc = build_ctx(node_chars=0, include_comments=False)
    nc_pretty_prompt, nc_pretty_chars = build_prompt(
        ctx_no_comments, compact_json=False
    )
    if nc_pretty_chars <= max_prompt_chars:
        return (
            ctx_no_comments,
            int(truncated_nc),
            nc_pretty_prompt,
            int(nc_pretty_chars),
            0,
            False,
            False,
        )

    nc_compact_prompt, nc_compact_chars = build_prompt(
        ctx_no_comments, compact_json=True
    )
    if nc_compact_chars <= max_prompt_chars:
        return (
            ctx_no_comments,
            int(truncated_nc),
            nc_compact_prompt,
            int(nc_compact_chars),
            0,
            True,
            False,
        )

    best_nc = search_best_cap(include_comments=False)
    if best_nc is not None:
        best_ctx, best_truncated, best_prompt, best_prompt_chars, best_cap = best_nc
        return (
            best_ctx,
            best_truncated,
            best_prompt,
            best_prompt_chars,
            best_cap,
            True,
            False,
        )

    raise RuntimeError(f"topic_prompt_too_long max_prompt_chars={max_prompt_chars}")


def _as_str_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    return []


def _map_topic_prompt_items_to_assertions(
    *,
    ai_result: dict[str, object],
    focus_username: str,
    message_lookup: dict[tuple[str, str], dict[str, object]],
    post_uid_by_platform_post_id: dict[str, str],
    max_assertions_per_post: int = 5,
) -> dict[str, list[dict[str, object]]]:
    """
    Convert topic-prompt-v3 items -> per-post assertions rows (AlphaVault schema).

    We only accept items that:
    - speaker == focus_username
    - have evidence_refs pointing to a known post platform_post_id (leaf "status" nodes)
    """
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
                _as_str_list(raw_item.get("stock_codes")), ensure_ascii=False
            ),
            "stock_names_json": json.dumps(
                _as_str_list(raw_item.get("stock_names")), ensure_ascii=False
            ),
            "industries_json": json.dumps(
                _as_str_list(raw_item.get("industries")), ensure_ascii=False
            ),
            "commodities_json": json.dumps(
                _as_str_list(raw_item.get("commodities")), ensure_ascii=False
            ),
            "indices_json": json.dumps(
                _as_str_list(raw_item.get("indices")), ensure_ascii=False
            ),
        }
        bucket = out.setdefault(post_uid, [])
        if len(bucket) < max(0, int(max_assertions_per_post)):
            bucket.append(row)

    return out


def _process_one_post_uid_topic_prompt_v3(
    *,
    engine: Engine,
    post_uid: str,
    config: LLMConfig,
    limiter: RateLimiter,
    prefetched_post: CloudPost | None = None,
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

    # Scan recent posts from the same author, then keep only the same "root_key" thread.
    recent = load_recent_posts_by_author(engine, author=focus, limit=200)
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

    # Lock additional posts in this thread (best-effort), so we can write results back once.
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
    ) = _build_topic_prompt_v3_with_prompt_chars_limit(
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
            _build_topic_prompt_v3_llm_log_line(
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
                _build_topic_prompt_v3_llm_log_line(
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
        assertions_by_post_uid = _map_topic_prompt_items_to_assertions(
            ai_result=parsed,
            focus_username=focus,
            message_lookup=message_lookup,  # type: ignore[arg-type]
            post_uid_by_platform_post_id=post_uid_by_pid,
            max_assertions_per_post=5,
        )

        for uid in locked_post_uids:
            rows = assertions_by_post_uid.get(uid, [])
            is_relevant = bool(rows)
            final_status = "relevant" if is_relevant else "irrelevant"
            invest_score = _score_from_assertions(rows)
            write_assertions_and_mark_done(
                engine,
                post_uid=uid,
                final_status=final_status,
                invest_score=invest_score,
                processed_at=now_str(),
                model=config.model,
                prompt_version=config.prompt_version,
                archived_at=now_str(),
                ai_result_json=None,
                assertions=rows,
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
        return True
    except Exception as e:
        if isinstance(e, AiInvalidJsonError):
            raw_tail = _to_one_line_tail(getattr(e, "raw_ai_text", ""), max_chars=240)
            print(
                " ".join(
                    [
                        "[ai_topic] invalid_json",
                        f"post_uid={post.post_uid}",
                        f"author={focus or '(empty)'}",
                        f"root_key={root_key}",
                        f"prompt_version={config.prompt_version}",
                        f"raw_ai_len={len(getattr(e, 'raw_ai_text', '') or '')}",
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
        msg = f"ai:{format_llm_error_one_line(e, limit=700)}{ctx}"
        now_epoch = int(time.time())
        for uid in locked_post_uids:
            retry_count = retry_count_by_uid.get(uid, 1)
            next_retry = now_epoch + _backoff_seconds(retry_count)
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
            _build_topic_prompt_v3_llm_log_line(
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


def _process_one_post_uid(
    *,
    engine: Engine,
    post_uid: str,
    config: LLMConfig,
    limiter: RateLimiter,
    prefetched_post: CloudPost | None = None,
) -> bool:
    try:
        if str(config.prompt_version or "").strip() == TOPIC_PROMPT_VERSION:
            return _process_one_post_uid_topic_prompt_v3(
                engine=engine,
                post_uid=post_uid,
                config=config,
                limiter=limiter,
                prefetched_post=prefetched_post,
            )
        post = (
            prefetched_post
            if prefetched_post is not None
            else load_cloud_post(engine, post_uid)
        )
        analysis_context = build_analysis_context(post.raw_text or "")
        row_meta = build_row_meta(
            mid_or_bid=str(post.platform_post_id or ""),
            bid="",
            link=str(post.url or ""),
            title="",
            author=str(post.author or ""),
            created_at=str(post.created_at or ""),
            raw_text=str(post.raw_text or ""),
        )
        if config.verbose:
            print(f"[llm] call_api {post_uid}", flush=True)
        limiter.wait()
        start_ts = time.time()
        result: AnalyzeResult = analyze_with_litellm(
            api_key=config.api_key,
            model=config.model,
            analysis_context=analysis_context,
            row=row_meta,
            base_url=config.base_url,
            api_mode=config.api_mode,
            ai_stream=config.ai_stream,
            ai_retries=config.ai_retries,
            ai_temperature=config.ai_temperature,
            ai_reasoning_effort=config.ai_reasoning_effort,
            trace_out=config.trace_out,
            timeout_seconds=config.ai_timeout_seconds,
        )
        if config.verbose:
            cost = time.time() - start_ts
            print(
                f"[llm] done {post_uid} status={result.status} score={result.invest_score:.3f} cost={cost:.1f}s",
                flush=True,
            )

        final_result = result
        if final_result.invest_score < config.relevant_threshold:
            final_result = AnalyzeResult(
                status="irrelevant",
                invest_score=final_result.invest_score,
                assertions=[],
            )
        else:
            final_result.assertions = validate_and_adjust_assertions(
                final_result.assertions,
                commentary_text=analysis_context["commentary_text"],
                quoted_text=analysis_context["quoted_text"],
            )

        assertions = (
            final_result.assertions if final_result.status == "relevant" else []
        )

        write_assertions_and_mark_done(
            engine,
            post_uid=post_uid,
            final_status=final_result.status,
            invest_score=float(final_result.invest_score),
            processed_at=now_str(),
            model=config.model,
            prompt_version=config.prompt_version,
            archived_at=now_str(),
            ai_result_json=None,
            assertions=assertions,
        )
        if assertions:
            try:
                mark_stock_dirty_from_assertions(
                    engine,
                    assertions=assertions,
                    reason="ai_done",
                )
            except BaseException:
                if config.verbose:
                    print(
                        f"[stock_hot] mark_dirty_failed post_uid={post_uid}",
                        flush=True,
                    )
        return True
    except Exception as e:
        base_url_for_log = (config.base_url or "").strip()
        if base_url_for_log:
            base_url_for_log = base_url_for_log.split("?", 1)[0].split("#", 1)[0]
            base_url_for_log = base_url_for_log[:220]
        ctx = (
            f" cfg_model={config.model}"
            f" api_mode={config.api_mode}"
            f" stream={1 if config.ai_stream else 0}"
            f" base_url={base_url_for_log or '(empty)'}"
        )
        msg = f"ai:{format_llm_error_one_line(e, limit=700)}{ctx}"
        now_epoch = int(time.time())
        retry_count = int(getattr(prefetched_post, "ai_retry_count", 1) or 1)
        next_retry = now_epoch + _backoff_seconds(retry_count)
        try:
            mark_ai_error(
                engine,
                post_uid=post_uid,
                error=msg,
                next_retry_at=next_retry,
                archived_at=now_str(),
            )
        except Exception as mark_e:
            if config.verbose:
                print(
                    f"[llm] mark_error_failed {post_uid} {type(mark_e).__name__}: {mark_e}",
                    flush=True,
                )
        print(f"[llm] error {post_uid} {msg}", flush=True)
        return False


def _log_spool_and_redis(
    *, verbose: bool, spool_dir: Path, redis_client, redis_queue_key: str
) -> None:
    if not verbose:
        return
    print(f"[spool] dir={spool_dir}", flush=True)
    if redis_client:
        print(f"[redis] enabled key={redis_queue_key}", flush=True)


def _build_source_spool_dir(
    *, base_spool_dir: Path, source_name: str, multi_source: bool
) -> Path:
    path = base_spool_dir if not multi_source else (base_spool_dir / source_name)
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"[spool] dir_error {path} {type(e).__name__}: {e}", flush=True)
    return path


def _build_source_redis_queue_key(
    *, base_queue_key: str, source_name: str, multi_source: bool
) -> str:
    if not base_queue_key:
        return ""
    if not multi_source:
        return base_queue_key
    return f"{base_queue_key}:{source_name}"


def _log_source_runtime(
    *,
    verbose: bool,
    source: WorkerSourceRuntime,
    redis_client,
    rss_interval_seconds: float,
    rss_feed_sleep_seconds: float,
) -> None:
    if not verbose:
        return
    cfg = source.config
    print(
        f"[source] name={cfg.name} platform={cfg.platform} rss={len(cfg.rss_urls)} "
        f"rss_interval={int(max(1.0, float(rss_interval_seconds)))}s "
        f"rss_feed_sleep={float(max(0.0, float(rss_feed_sleep_seconds))):.1f}s "
        f"db={cfg.database_url}",
        flush=True,
    )
    _log_spool_and_redis(
        verbose=verbose,
        spool_dir=source.spool_dir,
        redis_client=redis_client,
        redis_queue_key=source.redis_queue_key,
    )


def _maybe_dispose_turso_engine_on_transient_error(
    *, engine: Engine, err: BaseException, verbose: bool
) -> None:
    reason = ""
    if is_turso_stream_not_found_error(err):
        reason = "stream_not_found"
    elif is_turso_libsql_panic_error(err):
        reason = "libsql_panic"
    else:
        return
    try:
        engine.dispose()
        if verbose:
            print(f"[turso] disposed_engine reason={reason}", flush=True)
    except Exception as dispose_e:
        if verbose:
            print(
                f"[turso] dispose_engine_failed {type(dispose_e).__name__}: {dispose_e}",
                flush=True,
            )


def _ensure_turso_ready(
    *, engine: Engine, verbose: bool, turso_ready: bool, source_name: str = ""
) -> bool:
    if turso_ready:
        return True
    prefix = f"[turso:{source_name}]" if source_name else "[turso]"
    try:
        ensure_cloud_queue_schema(engine, verbose=bool(verbose))
        print(f"{prefix} ready", flush=True)
        return True
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(
            engine=engine, err=e, verbose=bool(verbose)
        )
        if verbose:
            print(f"{prefix} not_ready {type(e).__name__}: {e}", flush=True)
        return False


def _seconds_until_next_active_start(
    now_dt: datetime, active_hours: tuple[int, int]
) -> float:
    start_hour, end_hour = active_hours
    today_start = now_dt.replace(hour=start_hour, minute=0, second=0, microsecond=0)

    if start_hour <= end_hour:
        if now_dt.hour < start_hour:
            next_dt = today_start
        else:
            next_dt = today_start + timedelta(days=1)
    else:
        next_dt = today_start

    return max(1.0, (next_dt - now_dt).total_seconds())


def _resolve_stock_alias_sync_interval_seconds() -> float:
    raw_value = os.getenv(ENV_WORKER_STOCK_ALIAS_SYNC_INTERVAL_SECONDS, "").strip()
    if not raw_value:
        return 1800.0
    try:
        seconds = float(raw_value)
    except Exception:
        return 1800.0
    return max(60.0, seconds)


def _resolve_stock_hot_cache_interval_seconds() -> float:
    raw_value = os.getenv(ENV_WORKER_STOCK_HOT_CACHE_INTERVAL_SECONDS, "").strip()
    if not raw_value:
        return 60.0
    try:
        seconds = float(raw_value)
    except Exception:
        return 60.0
    return max(15.0, seconds)


def _resolve_rss_feed_sleep_seconds() -> float:
    raw_value = env_float(ENV_RSS_FEED_SLEEP_SECONDS)
    if raw_value is None:
        return float(DEFAULT_RSS_FEED_SLEEP_SECONDS)
    return max(0.0, float(raw_value))


def _build_alias_ai_runtime_config(config: LLMConfig) -> AiRuntimeConfig:
    return AiRuntimeConfig(
        api_key=str(config.api_key or "").strip(),
        model=str(config.model or "").strip() or DEFAULT_MODEL,
        base_url=str(config.base_url or "").strip(),
        api_mode=str(config.api_mode or DEFAULT_AI_MODE).strip() or DEFAULT_AI_MODE,
        temperature=float(config.ai_temperature),
        reasoning_effort=str(config.ai_reasoning_effort or "").strip()
        or DEFAULT_AI_REASONING_EFFORT,
        timeout_seconds=float(config.ai_timeout_seconds),
        retries=int(config.ai_retries),
    )


def _collect_periodic_job_result(
    *,
    job_name: str,
    future: Future | None,
    engine: Engine,
    verbose: bool,
) -> tuple[Future | None, dict[str, int | bool], bool, bool]:
    if future is None or not future.done():
        return future, {}, False, False
    try:
        raw = future.result()
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(
            engine=engine, err=e, verbose=bool(verbose)
        )
        if verbose:
            print(f"[{job_name}] sync_error {type(e).__name__}: {e}", flush=True)
        return None, {}, True, True
    stats = raw if isinstance(raw, dict) else {}
    return None, stats, True, False


def _collect_rss_ingest_result(
    *,
    source_name: str,
    future: Future | None,
    engine: Engine,
    verbose: bool,
) -> tuple[Future | None, int, bool, bool]:
    if future is None or not future.done():
        return future, 0, False, False
    try:
        raw = future.result()
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(
            engine=engine, err=e, verbose=bool(verbose)
        )
        if verbose:
            print(
                f"[rss:{source_name}] ingest_error {type(e).__name__}: {e}",
                flush=True,
            )
        return None, 0, True, True

    accepted = 0
    ingest_enqueue_error = False
    if isinstance(raw, tuple) and len(raw) >= 2:
        try:
            accepted = max(0, int(raw[0]))
        except Exception:
            accepted = 0
        ingest_enqueue_error = bool(raw[1])
    return None, accepted, True, ingest_enqueue_error


def _submit_spool_flush_job(
    sync_engine: Engine,
    *,
    spool_dir: Path,
    redis_client,
    redis_queue_key: str,
    verbose: bool,
) -> dict[str, int | bool]:
    flushed, has_error = flush_spool_to_turso(
        spool_dir=spool_dir,
        engine=sync_engine,
        max_items=int(SPOOL_FLUSH_MAX_ITEMS_PER_RUN),
        verbose=bool(verbose),
        redis_client=redis_client,
        redis_queue_key=redis_queue_key,
        delete_spool_on_redis_push=True,
    )
    has_more = bool(
        (not has_error) and int(flushed) >= int(SPOOL_FLUSH_MAX_ITEMS_PER_RUN)
    )
    return {
        "flushed": int(flushed),
        "has_error": bool(has_error),
        "has_more": bool(has_more),
    }


def _mark_spool_item_ingested(
    *, source: WorkerSourceRuntime, wakeup_event: threading.Event
) -> None:
    with source.spool_state_lock:
        source.spool_seq_written += 1
    wakeup_event.set()


def _should_start_spool_flush(*, source: WorkerSourceRuntime) -> bool:
    with source.spool_state_lock:
        return bool(
            source.spool_need_retry
            or int(source.spool_seq_written) > int(source.spool_seq_scheduled)
        )


def _mark_spool_flush_started(*, source: WorkerSourceRuntime) -> None:
    with source.spool_state_lock:
        source.spool_seq_scheduled = int(source.spool_seq_written)
        source.spool_need_retry = False


def _mark_spool_flush_retry(
    *, source: WorkerSourceRuntime, has_more: bool, has_error: bool
) -> None:
    if not (bool(has_more) or bool(has_error)):
        return
    with source.spool_state_lock:
        source.spool_need_retry = True


def _request_spool_flush(*, source: WorkerSourceRuntime) -> None:
    with source.spool_state_lock:
        source.spool_need_retry = True


def _maybe_start_periodic_job(
    *,
    executor: ThreadPoolExecutor,
    future: Future | None,
    active_engine: Optional[Engine],
    trigger: bool,
    now: float,
    next_run_at: float,
    interval_seconds: float,
    wakeup_event: threading.Event,
    submit_fn: Callable[..., dict[str, int | bool]],
    submit_kwargs: dict[str, Any] | None = None,
) -> tuple[Future | None, float, bool]:
    engine_for_job = active_engine
    if (
        not trigger
        or engine_for_job is None
        or future is not None
        or now < float(next_run_at)
    ):
        return future, next_run_at, False
    new_future = executor.submit(submit_fn, engine_for_job, **(submit_kwargs or {}))
    new_future.add_done_callback(lambda _f: wakeup_event.set())
    if bool(interval_seconds) and float(interval_seconds) > 0:
        next_at = now + float(interval_seconds)
    else:
        next_at = now
    return new_future, next_at, True


def _prune_inflight_futures(
    inflight_futures: set[Future],
    inflight_owner_by_future: dict[Future, str],
) -> None:
    done = {f for f in inflight_futures if f.done()}
    if not done:
        return
    inflight_futures.difference_update(done)
    for fut in done:
        inflight_owner_by_future.pop(fut, None)


def _count_inflight_for_owner(
    inflight_owner_by_future: dict[Future, str],
    *,
    owner: str,
) -> int:
    resolved = str(owner or "").strip()
    if not resolved:
        return 0
    return sum(
        1
        for fut, fut_owner in inflight_owner_by_future.items()
        if fut_owner == resolved and not fut.done()
    )


def _format_epoch_to_cst(value: float) -> str:
    ts = float(value or 0.0)
    if ts <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ts, tz=CST).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _should_fast_retry_for_periodic_job(*, has_more: bool) -> bool:
    return bool(has_more)


def _build_source_turso_error(
    *,
    maintenance_error: bool,
    spool_flush_error: bool,
    schedule_error: bool,
    alias_sync_error: bool,
    backfill_cache_error: bool,
    relation_cache_error: bool,
    stock_hot_error: bool,
) -> bool:
    return bool(
        maintenance_error
        or spool_flush_error
        or schedule_error
        or alias_sync_error
        or backfill_cache_error
        or relation_cache_error
        or stock_hot_error
    )


def _should_wait_with_event(
    *,
    ai_inflight: bool,
    any_alias_inflight: bool,
    any_backfill_inflight: bool,
    any_relation_inflight: bool,
    any_stock_hot_inflight: bool,
    any_rss_inflight: bool,
    any_spool_flush_inflight: bool,
) -> bool:
    return bool(
        ai_inflight
        or any_alias_inflight
        or any_backfill_inflight
        or any_relation_inflight
        or any_stock_hot_inflight
        or any_rss_inflight
        or any_spool_flush_inflight
    )


def _save_worker_progress_state(
    *,
    source: WorkerSourceRuntime,
    stage: str,
    payload: dict[str, object],
    verbose: bool,
) -> None:
    state_key = worker_progress_state_key(source_name=source.config.name, stage=stage)
    if not state_key:
        return
    data = {str(key): value for key, value in payload.items() if str(key or "").strip()}
    data["source"] = str(source.config.name or "").strip()
    data["stage"] = str(stage or "").strip()
    data["updated_at"] = now_str()
    try:
        save_worker_job_cursor(
            source.engine,
            state_key=state_key,
            cursor=json.dumps(data, ensure_ascii=False),
        )
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(
            engine=source.engine, err=err, verbose=bool(verbose)
        )
        if verbose:
            print(
                f"[progress:{source.config.name}] write_error {type(err).__name__}: {err}",
                flush=True,
            )


def _has_due_ai_posts(
    *,
    engine: Optional[Engine],
    platform: str,
    verbose: bool,
    redis_client=None,
    redis_queue_key: str = "",
) -> bool:
    if redis_client and str(redis_queue_key or "").strip():
        try:
            return bool(
                redis_ai_due_count(
                    redis_client,
                    str(redis_queue_key),
                    now_epoch=int(time.time()),
                )
            )
        except BaseException as err:
            if isinstance(err, _FATAL_BASE_EXCEPTIONS):
                raise
            if verbose:
                print(
                    f"[ai] redis_due_check_error platform={platform} {type(err).__name__}: {err}",
                    flush=True,
                )
            return False
    if engine is None:
        return False
    try:
        due = select_due_post_uids(
            engine,
            now_epoch=int(time.time()),
            limit=1,
            platform=str(platform or "").strip().lower() or None,
        )
        return bool(due)
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(
            engine=engine, err=err, verbose=bool(verbose)
        )
        return False


def _compute_low_priority_budget(*, ai_cap: int, rss_inflight_now: int) -> int:
    return max(0, int(ai_cap) - max(0, int(rss_inflight_now)))


def _compute_backfill_max_stocks_per_run(*, low_budget: int) -> int:
    return max(
        1,
        min(
            int(BACKFILL_MAX_STOCKS_PER_RUN_CAP),
            max(1, int(low_budget)),
        ),
    )


def _compute_rss_available_slots(
    *,
    ai_cap: int,
    rss_inflight_now: int,
    low_inflight_now: int,
) -> int:
    return max(
        0,
        int(ai_cap) - max(0, int(rss_inflight_now)) - max(0, int(low_inflight_now)),
    )


def _build_low_priority_should_continue(
    *,
    ai_cap: int,
    rss_inflight_now_get: Callable[[], int],
    low_inflight_now_get: Callable[[], int] | None = None,
    has_due_ai_pending_get: Callable[[], bool] | None = None,
) -> Callable[[], bool]:
    resolve_low_inflight_now = low_inflight_now_get or (lambda: 0)
    resolve_due_ai_pending = has_due_ai_pending_get or (lambda: False)

    def _should_continue() -> bool:
        try:
            rss_inflight_now = max(0, int(rss_inflight_now_get()))
        except Exception:
            rss_inflight_now = 0
        low_budget = _compute_low_priority_budget(
            ai_cap=int(ai_cap),
            rss_inflight_now=int(rss_inflight_now),
        )
        if low_budget <= 0:
            return False
        try:
            low_inflight_now = max(0, int(resolve_low_inflight_now()))
        except Exception:
            low_inflight_now = 0
        rss_available_slots = _compute_rss_available_slots(
            ai_cap=int(ai_cap),
            rss_inflight_now=int(rss_inflight_now),
            low_inflight_now=int(low_inflight_now),
        )
        if rss_available_slots > 0:
            return True
        try:
            has_due_ai_pending = bool(resolve_due_ai_pending())
        except Exception:
            return False
        return not bool(has_due_ai_pending)

    return _should_continue


class _LowPriorityAISlotGate:
    def __init__(self, *, cap_getter: Callable[[], int]) -> None:
        self._cap_getter = cap_getter
        self._lock = threading.Lock()
        self._inflight = 0

    def try_acquire(self) -> bool:
        try:
            cap_now = max(0, int(self._cap_getter()))
        except Exception:
            cap_now = 0
        with self._lock:
            if cap_now <= 0 or int(self._inflight) >= int(cap_now):
                return False
            self._inflight += 1
            return True

    def release(self) -> None:
        with self._lock:
            if self._inflight <= 0:
                self._inflight = 0
                return
            self._inflight -= 1

    def inflight(self) -> int:
        with self._lock:
            return int(self._inflight)


def _payload_retry_count(payload: dict[str, object]) -> int:
    raw_retry_count = payload.get("retry_count")
    return max(0, _parse_int_or_default(raw_retry_count, 0))


def _payload_to_cloud_post(payload: dict[str, object]) -> CloudPost | None:
    post_uid = str(payload.get("post_uid") or "").strip()
    if not post_uid:
        return None
    return CloudPost(
        post_uid=post_uid,
        platform=str(payload.get("platform") or "weibo").strip() or "weibo",
        platform_post_id=str(payload.get("platform_post_id") or "").strip(),
        author=str(payload.get("author") or "").strip(),
        created_at=str(payload.get("created_at") or "").strip() or now_str(),
        url=str(payload.get("url") or "").strip(),
        raw_text=str(payload.get("raw_text") or ""),
        display_md=str(payload.get("display_md") or ""),
        ai_retry_count=max(1, int(_payload_retry_count(payload) or 1)),
    )


def _upsert_pending_post_from_payload(
    *, engine: Engine, payload: dict[str, object]
) -> None:
    raw_text = str(payload.get("raw_text") or "")
    author = str(payload.get("author") or "")
    display_md = str(payload.get("display_md") or "")
    if not display_md.strip():
        display_md = format_weibo_display_md(raw_text, author=author)
    raw_ingested_at = payload.get("ingested_at")
    ingested_at = _parse_int_or_default(raw_ingested_at, int(time.time()))
    upsert_pending_post(
        engine,
        post_uid=str(payload.get("post_uid") or ""),
        platform=str(payload.get("platform") or "weibo"),
        platform_post_id=str(payload.get("platform_post_id") or ""),
        author=author,
        created_at=str(payload.get("created_at") or now_str()),
        url=str(payload.get("url") or ""),
        raw_text=raw_text,
        display_md=display_md,
        archived_at=now_str(),
        ingested_at=int(ingested_at),
    )


def _process_one_redis_payload(
    *,
    engine: Engine,
    payload: dict[str, object],
    processing_msg: str,
    redis_client,
    redis_queue_key: str,
    spool_dir: Path,
    config: LLMConfig,
    limiter: RateLimiter,
    verbose: bool,
) -> None:
    cloud_post = _payload_to_cloud_post(payload)
    if cloud_post is None:
        try:
            redis_ai_ack_processing(redis_client, redis_queue_key, processing_msg)
        except Exception:
            return
        return

    success = _process_one_post_uid(
        engine=engine,
        post_uid=cloud_post.post_uid,
        config=config,
        limiter=limiter,
        prefetched_post=cloud_post,
    )
    if success:
        redis_ai_ack_and_cleanup(
            redis_client,
            redis_queue_key,
            msg=processing_msg,
            post_uid=cloud_post.post_uid,
            spool_dir=spool_dir,
            verbose=bool(verbose),
        )
        return

    retry_count = max(1, int(_payload_retry_count(payload)) + 1)
    next_retry_at = int(time.time()) + _backoff_seconds(retry_count)
    retry_payload = dict(payload)
    retry_payload["retry_count"] = int(retry_count)
    retry_payload["next_retry_at"] = int(next_retry_at)
    try:
        redis_ai_push_delayed(
            redis_client,
            redis_queue_key,
            payload=retry_payload,
            next_retry_at=int(next_retry_at),
        )
    except Exception as err:
        if verbose:
            print(
                f"[ai] redis_delay_push_error post_uid={cloud_post.post_uid} {type(err).__name__}: {err}",
                flush=True,
            )
        return
    try:
        redis_ai_ack_processing(redis_client, redis_queue_key, processing_msg)
    except Exception as err:
        if verbose:
            print(
                f"[ai] redis_ack_error post_uid={cloud_post.post_uid} {type(err).__name__}: {err}",
                flush=True,
            )


def _schedule_ai_from_redis(
    executor: ThreadPoolExecutor,
    *,
    engine: Engine,
    ai_cap: int,
    low_inflight_now_get: Callable[[], int],
    inflight_futures: set[Future],
    inflight_owner_by_future: dict[Future, str],
    inflight_owner: str,
    wakeup_event: threading.Event,
    config: LLMConfig,
    limiter: RateLimiter,
    verbose: bool,
    redis_client,
    redis_queue_key: str,
    spool_dir: Path,
) -> Tuple[int, bool]:
    _prune_inflight_futures(inflight_futures, inflight_owner_by_future)
    rss_inflight_now = int(len(inflight_futures))
    try:
        low_inflight_now = max(0, int(low_inflight_now_get()))
    except Exception:
        low_inflight_now = 0
    available = _compute_rss_available_slots(
        ai_cap=int(ai_cap),
        rss_inflight_now=int(rss_inflight_now),
        low_inflight_now=int(low_inflight_now),
    )
    if available <= 0:
        return 0, False

    try:
        redis_ai_move_due_delayed_to_ready(
            redis_client,
            redis_queue_key,
            now_epoch=int(time.time()),
            max_items=max(1, int(available) * 2),
            verbose=bool(verbose),
        )
        redis_ai_requeue_processing(
            redis_client,
            redis_queue_key,
            max_items=max(1, int(available)),
            verbose=bool(verbose),
        )
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        if verbose:
            print(
                f"[ai] redis_maintenance_error owner={inflight_owner} {type(e).__name__}: {e}",
                flush=True,
            )
        return 0, True

    scheduled = 0
    for _ in range(max(1, int(available))):
        if scheduled >= available:
            break
        try:
            msg = redis_ai_pop_to_processing(redis_client, redis_queue_key)
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
            if verbose:
                print(
                    f"[ai] redis_pop_error owner={inflight_owner} {type(e).__name__}: {e}",
                    flush=True,
                )
            return scheduled, True
        if not msg:
            break
        try:
            payload = json.loads(msg)
        except Exception as e:
            if verbose:
                print(f"[ai] redis_bad_payload {type(e).__name__}: {e}", flush=True)
            try:
                redis_ai_ack_processing(redis_client, redis_queue_key, str(msg))
            except Exception:
                pass
            continue
        if not isinstance(payload, dict):
            try:
                redis_ai_ack_processing(redis_client, redis_queue_key, str(msg))
            except Exception:
                pass
            continue
        if not str(payload.get("post_uid") or "").strip():
            try:
                redis_ai_ack_processing(redis_client, redis_queue_key, str(msg))
            except Exception:
                pass
            continue
        try:
            _upsert_pending_post_from_payload(engine=engine, payload=payload)
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
            _maybe_dispose_turso_engine_on_transient_error(
                engine=engine, err=e, verbose=bool(verbose)
            )
            if verbose:
                print(
                    f"[ai] redis_upsert_error owner={inflight_owner} {type(e).__name__}: {e}",
                    flush=True,
                )
            return scheduled, True

        fut = executor.submit(
            _process_one_redis_payload,
            engine=engine,
            payload=payload,
            processing_msg=str(msg),
            redis_client=redis_client,
            redis_queue_key=redis_queue_key,
            spool_dir=spool_dir,
            config=config,
            limiter=limiter,
            verbose=bool(verbose),
        )
        fut.add_done_callback(lambda _f: wakeup_event.set())
        inflight_futures.add(fut)
        inflight_owner_by_future[fut] = str(inflight_owner or "").strip()
        scheduled += 1

    return scheduled, False


def _schedule_ai(
    executor: ThreadPoolExecutor,
    *,
    engine: Optional[Engine],
    platform: str,
    ai_cap: int,
    low_inflight_now_get: Callable[[], int],
    inflight_futures: set[Future],
    inflight_owner_by_future: dict[Future, str],
    inflight_owner: str,
    wakeup_event: threading.Event,
    config: LLMConfig,
    limiter: RateLimiter,
    verbose: bool,
    redis_client=None,
    redis_queue_key: str = "",
    spool_dir: Path | None = None,
) -> Tuple[int, bool]:
    if engine is None:
        return 0, False
    if redis_client and str(redis_queue_key or "").strip() and spool_dir is not None:
        return _schedule_ai_from_redis(
            executor,
            engine=engine,
            ai_cap=ai_cap,
            low_inflight_now_get=low_inflight_now_get,
            inflight_futures=inflight_futures,
            inflight_owner_by_future=inflight_owner_by_future,
            inflight_owner=inflight_owner,
            wakeup_event=wakeup_event,
            config=config,
            limiter=limiter,
            verbose=bool(verbose),
            redis_client=redis_client,
            redis_queue_key=str(redis_queue_key),
            spool_dir=spool_dir,
        )
    # Keep inflight bounded: RSS + low-priority inflight must not exceed ai_cap.
    _prune_inflight_futures(inflight_futures, inflight_owner_by_future)
    rss_inflight_now = int(len(inflight_futures))
    try:
        low_inflight_now = max(0, int(low_inflight_now_get()))
    except Exception:
        low_inflight_now = 0
    available = _compute_rss_available_slots(
        ai_cap=int(ai_cap),
        rss_inflight_now=int(rss_inflight_now),
        low_inflight_now=int(low_inflight_now),
    )
    if available <= 0:
        return 0, False
    now_epoch = int(time.time())
    try:
        due = select_due_post_uids(
            engine,
            now_epoch=now_epoch,
            limit=max(1, int(available) * 2),
            platform=str(platform or "").strip().lower() or None,
        )
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(
            engine=engine, err=e, verbose=bool(verbose)
        )
        if verbose:
            print(
                f"[ai] select_due_error owner={inflight_owner} platform={platform} {type(e).__name__}: {e}",
                flush=True,
            )
        return 0, True

    raw_due = list(due or [])
    due = _dedup_post_uids(raw_due)
    if verbose and len(due) < len(raw_due):
        print(
            f"[ai] due_dedup owner={inflight_owner} platform={platform} "
            f"before={len(raw_due)} after={len(due)}",
            flush=True,
        )

    scheduled = 0
    for post_uid in due:
        if scheduled >= available:
            break
        try:
            ok = try_mark_ai_running(engine, post_uid=post_uid, now_epoch=now_epoch)
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
            _maybe_dispose_turso_engine_on_transient_error(
                engine=engine, err=e, verbose=bool(verbose)
            )
            if verbose:
                print(
                    f"[ai] mark_running_error owner={inflight_owner} platform={platform} {type(e).__name__}: {e}",
                    flush=True,
                )
            return scheduled, True
        if not ok:
            continue
        fut = executor.submit(
            _process_one_post_uid,
            engine=engine,
            post_uid=post_uid,
            config=config,
            limiter=limiter,
        )
        fut.add_done_callback(lambda _f: wakeup_event.set())
        inflight_futures.add(fut)
        inflight_owner_by_future[fut] = str(inflight_owner or "").strip()
        scheduled += 1
    return scheduled, False


def _dedup_post_uids(post_uids: Sequence[object]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in post_uids:
        post_uid = str(value or "").strip()
        if not post_uid or post_uid in seen:
            continue
        seen.add(post_uid)
        deduped.append(post_uid)
    return deduped


def _run_turso_maintenance(
    *,
    engine: Optional[Engine],
    platform: str,
    spool_dir: Path,
    redis_client,
    redis_queue_key: str,
    stuck_seconds: int,
    verbose: bool,
) -> Tuple[int, int, bool]:
    if engine is None:
        return 0, 0, False

    turso_error = False
    recovered = 0
    try:
        recovered = recover_stuck_ai_tasks(
            engine,
            now_epoch=int(time.time()),
            stuck_seconds=max(60, int(stuck_seconds)),
            platform=str(platform or "").strip().lower() or None,
            verbose=bool(verbose),
        )
        recovered += recover_done_without_processed_at(
            engine,
            platform=str(platform or "").strip().lower() or None,
            verbose=bool(verbose),
        )
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(
            engine=engine, err=e, verbose=bool(verbose)
        )
        turso_error = True
        if verbose:
            print(f"[ai] recover_error {type(e).__name__}: {e}", flush=True)

    flushed_redis = 0
    flush_redis_error = False
    del spool_dir
    if redis_client and str(redis_queue_key or "").strip():
        try:
            moved_due = redis_ai_move_due_delayed_to_ready(
                redis_client,
                redis_queue_key,
                now_epoch=int(time.time()),
                max_items=200,
                verbose=bool(verbose),
            )
            requeued = redis_ai_requeue_processing(
                redis_client,
                redis_queue_key,
                max_items=200,
                verbose=bool(verbose),
            )
            flushed_redis = int(moved_due) + int(requeued)
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
            flush_redis_error = True
            if verbose:
                print(f"[redis] flush_error {type(e).__name__}: {e}", flush=True)

    turso_error = bool(turso_error or flush_redis_error)
    return recovered, flushed_redis, turso_error


def main() -> None:
    args = parse_args()
    source_configs = [
        WorkerSourceConfig(
            name=cfg.name,
            platform=cfg.platform,
            rss_urls=list(cfg.rss_urls),
            author=cfg.author,
            user_id=cfg.user_id,
            database_url=cfg.database_url,
            auth_token=cfg.auth_token,
        )
        for cfg in resolve_rss_source_configs(args)
    ]
    worker_active_hours = _parse_worker_active_hours_from_args(args)
    worker_interval = _resolve_worker_interval_seconds(args)
    config = _build_config(args)
    limiter = RateLimiter(config.ai_rpm)
    ai_cap = max(1, int(args.ai_max_inflight or 1))

    rss_active_hours: Optional[tuple[int, int]] = None
    rss_active_hours_value = os.getenv(ENV_RSS_ACTIVE_HOURS, "").strip()
    if rss_active_hours_value:
        rss_active_hours = parse_active_hours(rss_active_hours_value)

    rss_interval_seconds = env_float(ENV_RSS_INTERVAL_SECONDS)
    if rss_interval_seconds is None or rss_interval_seconds <= 0:
        rss_interval_seconds = 600.0
    rss_interval_seconds = max(1.0, float(rss_interval_seconds))
    rss_feed_sleep_seconds = _resolve_rss_feed_sleep_seconds()

    multi_source = len(source_configs) > 1
    base_spool_dir = ensure_spool_dir()
    redis_client, base_redis_queue_key = try_get_redis()
    sources: list[WorkerSourceRuntime] = []
    for cfg in source_configs:
        source = WorkerSourceRuntime(
            config=cfg,
            engine=ensure_turso_engine(cfg.database_url, cfg.auth_token),
            spool_dir=_build_source_spool_dir(
                base_spool_dir=base_spool_dir,
                source_name=cfg.name,
                multi_source=multi_source,
            ),
            redis_queue_key=_build_source_redis_queue_key(
                base_queue_key=base_redis_queue_key,
                source_name=cfg.name,
                multi_source=multi_source,
            ),
            rss_next_ingest_at=0.0 if cfg.rss_urls else float("inf"),
        )
        sources.append(source)
        _log_source_runtime(
            verbose=bool(args.verbose),
            source=source,
            redis_client=redis_client,
            rss_interval_seconds=float(rss_interval_seconds),
            rss_feed_sleep_seconds=float(rss_feed_sleep_seconds),
        )
    limit = args.limit if args.limit and args.limit > 0 else None
    alias_sync_interval_seconds = _resolve_stock_alias_sync_interval_seconds()
    stock_hot_cache_interval_seconds = _resolve_stock_hot_cache_interval_seconds()
    alias_ai_runtime_config = _build_alias_ai_runtime_config(config)
    research_cache_interval_seconds = float(alias_sync_interval_seconds)
    low_priority_ai_gate: _LowPriorityAISlotGate | None = None

    def _submit_alias_sync_job(
        sync_engine: Engine,
        *,
        ai_max_inflight: int,
        should_continue: Callable[[], bool] | None = None,
    ) -> dict[str, int | bool]:
        effective_ai_max_inflight = max(1, int(ai_max_inflight))
        return sync_stock_alias_relations(
            sync_engine,
            ai_runtime_config=alias_ai_runtime_config,
            max_alias_keys_per_run=int(effective_ai_max_inflight),
            ai_max_inflight=int(effective_ai_max_inflight),
            should_continue=should_continue,
            acquire_low_priority_slot=(
                low_priority_ai_gate.try_acquire
                if low_priority_ai_gate is not None
                else None
            ),
            release_low_priority_slot=(
                low_priority_ai_gate.release
                if low_priority_ai_gate is not None
                else None
            ),
            verbose=bool(config.verbose),
        )

    def _submit_backfill_cache_job(
        sync_engine: Engine,
        *,
        max_stocks_per_run: int,
        should_continue: Callable[[], bool] | None = None,
    ) -> dict[str, int | bool]:
        return sync_stock_backfill_cache(
            sync_engine,
            max_stocks_per_run=max(1, int(max_stocks_per_run)),
            should_continue=should_continue,
            verbose=bool(config.verbose),
        )

    def _submit_relation_candidates_cache_job(
        sync_engine: Engine,
        *,
        ai_max_inflight: int,
        should_continue: Callable[[], bool] | None = None,
    ) -> dict[str, int | bool]:
        effective_ai_max_inflight = max(1, int(ai_max_inflight))
        return sync_relation_candidates_cache(
            sync_engine,
            limiter=limiter,
            ai_enabled=True,
            max_stocks_per_run=int(effective_ai_max_inflight),
            max_sectors_per_run=int(effective_ai_max_inflight),
            ai_max_inflight=int(effective_ai_max_inflight),
            should_continue=should_continue,
            acquire_low_priority_slot=(
                low_priority_ai_gate.try_acquire
                if low_priority_ai_gate is not None
                else None
            ),
            release_low_priority_slot=(
                low_priority_ai_gate.release
                if low_priority_ai_gate is not None
                else None
            ),
            verbose=bool(config.verbose),
        )

    def _submit_stock_hot_cache_job(
        sync_engine: Engine,
        *,
        should_continue: Callable[[], bool] | None = None,
    ) -> dict[str, int | bool]:
        return sync_stock_hot_cache(
            sync_engine,
            should_continue=should_continue,
            verbose=bool(config.verbose),
        )

    maintenance_next_at = 0.0

    with (
        ThreadPoolExecutor(max_workers=ai_cap) as executor,
        ThreadPoolExecutor(max_workers=max(1, len(sources))) as alias_executor,
        ThreadPoolExecutor(max_workers=max(1, len(sources))) as research_executor,
        ThreadPoolExecutor(max_workers=max(1, len(sources))) as backfill_executor,
        ThreadPoolExecutor(max_workers=max(1, len(sources))) as spool_executor,
        ThreadPoolExecutor(max_workers=max(1, len(sources))) as rss_executor,
    ):
        wakeup_event = threading.Event()
        inflight_futures: set[Future] = set()
        inflight_owner_by_future: dict[Future, str] = {}
        scheduler_state: dict[str, int] = {"rss_inflight_now": 0}

        def _rss_inflight_now() -> int:
            try:
                snapshot = tuple(inflight_futures)
            except RuntimeError:
                return int(scheduler_state.get("rss_inflight_now", 0))
            return sum(1 for fut in snapshot if not fut.done())

        low_priority_ai_gate = _LowPriorityAISlotGate(
            cap_getter=lambda: _compute_low_priority_budget(
                ai_cap=int(ai_cap),
                rss_inflight_now=int(_rss_inflight_now()),
            )
        )

        while True:
            verbose = bool(args.verbose)
            if worker_active_hours is not None:
                sleep_until_active(worker_active_hours, verbose=verbose)

            wakeup_event.clear()
            _prune_inflight_futures(inflight_futures, inflight_owner_by_future)
            scheduler_state["rss_inflight_now"] = int(_rss_inflight_now())
            now = time.time()
            do_maintenance = bool(now >= maintenance_next_at)
            if do_maintenance:
                maintenance_next_at = now + float(worker_interval)
            next_maintenance_in = max(0.0, maintenance_next_at - time.time())

            tick_sources: list[
                tuple[
                    WorkerSourceRuntime,
                    Optional[Engine],
                    bool,
                    str,
                ]
            ] = []
            for source in sources:
                do_ingest_rss = False
                rss_skip_reason = ""
                if not source.config.rss_urls:
                    rss_skip_reason = "no_sources"
                elif source.rss_ingest_future is not None:
                    rss_skip_reason = "inflight"
                else:
                    now_dt = datetime.now(CST)
                    if rss_active_hours is not None and not in_active_hours(
                        now_dt, rss_active_hours
                    ):
                        rss_skip_reason = "inactive"
                        source.rss_next_ingest_at = (
                            now
                            + _seconds_until_next_active_start(now_dt, rss_active_hours)
                        )
                    elif now < source.rss_next_ingest_at:
                        rss_skip_reason = "interval"
                    else:
                        do_ingest_rss = True
                        source.rss_next_ingest_at = now + float(rss_interval_seconds)

                force_maintenance = False
                if not source.turso_ready and now >= float(
                    source.turso_next_ready_check_at
                ):
                    source.turso_ready = _ensure_turso_ready(
                        engine=source.engine,
                        verbose=verbose,
                        turso_ready=source.turso_ready,
                        source_name=source.config.name,
                    )
                    if source.turso_ready:
                        source.turso_next_ready_check_at = 0.0
                        force_maintenance = True
                    else:
                        source.turso_next_ready_check_at = now + float(
                            TURSO_READY_RETRY_SECONDS
                        )
                active_engine: Optional[Engine] = (
                    source.engine if source.turso_ready else None
                )

                if do_ingest_rss and source.config.rss_urls:

                    def _on_item_ingested(src: WorkerSourceRuntime = source) -> None:
                        _mark_spool_item_ingested(
                            source=src,
                            wakeup_event=wakeup_event,
                        )

                    source.rss_ingest_future = rss_executor.submit(
                        ingest_rss_many_once,
                        rss_urls=source.config.rss_urls,
                        engine=active_engine,
                        spool_dir=source.spool_dir,
                        redis_client=redis_client,
                        redis_queue_key=source.redis_queue_key,
                        platform=source.config.platform,
                        author=source.config.author,
                        user_id=source.config.user_id,
                        limit=limit,
                        rss_timeout=float(args.rss_timeout),
                        rss_retries=int(args.rss_retries),
                        rss_feed_sleep_seconds=float(rss_feed_sleep_seconds),
                        on_item_ingested=_on_item_ingested,
                        verbose=verbose,
                    )
                    source.rss_ingest_future.add_done_callback(
                        lambda _f: wakeup_event.set()
                    )

                tick_sources.append(
                    (
                        source,
                        active_engine,
                        force_maintenance,
                        rss_skip_reason,
                    )
                )

            tick_runtime: list[dict[str, Any]] = []
            for (
                source,
                active_engine,
                force_maintenance,
                rss_skip_reason,
            ) in tick_sources:
                accepted = 0
                ingest_enqueue_error = False
                (
                    source.rss_ingest_future,
                    accepted,
                    _,
                    ingest_enqueue_error,
                ) = _collect_rss_ingest_result(
                    source_name=source.config.name,
                    future=source.rss_ingest_future,
                    engine=source.engine,
                    verbose=verbose,
                )

                spool_flushed = 0
                spool_has_more = False
                (
                    source.spool_flush_future,
                    spool_stats,
                    spool_flush_finished,
                    spool_flush_error,
                ) = _collect_periodic_job_result(
                    job_name=f"spool:{source.config.name}",
                    future=source.spool_flush_future,
                    engine=source.engine,
                    verbose=verbose,
                )
                spool_flushed = int(spool_stats.get("flushed", 0))
                spool_has_more = bool(spool_stats.get("has_more", False))
                spool_flush_error = bool(
                    spool_flush_error or bool(spool_stats.get("has_error", False))
                )
                _mark_spool_flush_retry(
                    source=source,
                    has_more=spool_has_more,
                    has_error=spool_flush_error,
                )
                if (
                    spool_flush_finished
                    and verbose
                    and (spool_flushed > 0 or spool_flush_error)
                ):
                    print(
                        f"[spool:{source.config.name}] flush_done flushed={spool_flushed} "
                        f"has_more={1 if spool_has_more else 0} error={1 if spool_flush_error else 0}",
                        flush=True,
                    )

                alias_resolved = 0
                alias_inserted = 0
                alias_sync_finished = False
                alias_has_more = False
                (
                    source.alias_sync_future,
                    alias_stats,
                    alias_sync_finished,
                    alias_sync_error,
                ) = _collect_periodic_job_result(
                    job_name=f"alias:{source.config.name}",
                    future=source.alias_sync_future,
                    engine=source.engine,
                    verbose=verbose,
                )
                alias_resolved = int(alias_stats.get("resolved", 0))
                alias_inserted = int(alias_stats.get("inserted", 0))
                alias_has_more = bool(alias_stats.get("has_more", False))
                if alias_sync_finished and verbose:
                    print(
                        f"[alias:{source.config.name}] sync_done resolved={alias_resolved} inserted={alias_inserted} "
                        f"attempted={int(alias_stats.get('attempted', 0))} "
                        f"queued={int(alias_stats.get('queued', 0))} "
                        f"remaining={int(alias_stats.get('remaining_aliases', 0))} "
                        f"has_more={1 if alias_has_more else 0} "
                        f"locked={1 if bool(alias_stats.get('locked', False)) else 0}",
                        flush=True,
                    )
                alias_fast_retry = _should_fast_retry_for_periodic_job(
                    has_more=alias_has_more
                )
                if alias_fast_retry:
                    source.alias_sync_next_at = 0.0

                backfill_processed = 0
                backfill_written = 0
                backfill_finished = False
                backfill_has_more = False
                (
                    source.backfill_cache_future,
                    backfill_stats,
                    backfill_finished,
                    backfill_cache_error,
                ) = _collect_periodic_job_result(
                    job_name=f"backfill_cache:{source.config.name}",
                    future=source.backfill_cache_future,
                    engine=source.engine,
                    verbose=verbose,
                )
                backfill_processed = int(backfill_stats.get("processed", 0))
                backfill_written = int(backfill_stats.get("written", 0))
                backfill_has_more = bool(backfill_stats.get("has_more", False))
                if backfill_finished and verbose:
                    print(
                        f"[backfill_cache:{source.config.name}] sync_done processed={backfill_processed} "
                        f"written={backfill_written} has_more={1 if backfill_has_more else 0} "
                        f"locked={1 if bool(backfill_stats.get('locked', False)) else 0}",
                        flush=True,
                    )
                backfill_fast_retry = _should_fast_retry_for_periodic_job(
                    has_more=backfill_has_more
                )
                if backfill_fast_retry:
                    source.backfill_cache_next_at = 0.0

                relation_cache_processed = 0
                relation_cache_upserted = 0
                relation_cache_deleted = 0
                relation_cache_finished = False
                relation_cache_has_more = False
                (
                    source.relation_cache_future,
                    relation_cache_stats,
                    relation_cache_finished,
                    relation_cache_error,
                ) = _collect_periodic_job_result(
                    job_name=f"relation_cache:{source.config.name}",
                    future=source.relation_cache_future,
                    engine=source.engine,
                    verbose=verbose,
                )
                relation_cache_processed = int(relation_cache_stats.get("processed", 0))
                relation_cache_upserted = int(relation_cache_stats.get("upserted", 0))
                relation_cache_deleted = int(relation_cache_stats.get("deleted", 0))
                relation_cache_has_more = bool(
                    relation_cache_stats.get("has_more", False)
                )
                if relation_cache_finished and verbose:
                    print(
                        f"[relation_cache:{source.config.name}] sync_done processed={relation_cache_processed} "
                        f"upserted={relation_cache_upserted} deleted={relation_cache_deleted} "
                        f"has_more={1 if relation_cache_has_more else 0} "
                        f"locked={1 if bool(relation_cache_stats.get('locked', False)) else 0}",
                        flush=True,
                    )
                relation_cache_fast_retry = _should_fast_retry_for_periodic_job(
                    has_more=relation_cache_has_more
                )
                if relation_cache_fast_retry:
                    source.relation_cache_next_at = 0.0

                stock_hot_processed = 0
                stock_hot_written = 0
                stock_hot_finished = False
                stock_hot_has_more = False
                (
                    source.stock_hot_cache_future,
                    stock_hot_stats,
                    stock_hot_finished,
                    stock_hot_error,
                ) = _collect_periodic_job_result(
                    job_name=f"stock_hot_cache:{source.config.name}",
                    future=source.stock_hot_cache_future,
                    engine=source.engine,
                    verbose=verbose,
                )
                stock_hot_processed = int(stock_hot_stats.get("processed", 0))
                stock_hot_written = int(stock_hot_stats.get("written", 0))
                stock_hot_has_more = bool(stock_hot_stats.get("has_more", False))
                if stock_hot_finished and verbose:
                    print(
                        f"[stock_hot_cache:{source.config.name}] sync_done processed={stock_hot_processed} "
                        f"written={stock_hot_written} extras_written={int(stock_hot_stats.get('extras_written', 0))} "
                        f"has_more={1 if stock_hot_has_more else 0} "
                        f"locked={1 if bool(stock_hot_stats.get('locked', False)) else 0}",
                        flush=True,
                    )
                stock_hot_fast_retry = _should_fast_retry_for_periodic_job(
                    has_more=stock_hot_has_more
                )
                if stock_hot_fast_retry:
                    source.stock_hot_cache_next_at = 0.0

                recovered = 0
                flushed_redis = 0
                maintenance_error = False
                if (do_maintenance or force_maintenance) and active_engine is not None:
                    if force_maintenance:
                        maintenance_next_at = now + float(worker_interval)
                        next_maintenance_in = max(
                            0.0, maintenance_next_at - time.time()
                        )
                    _request_spool_flush(source=source)
                    recovered, flushed_redis, maintenance_error = (
                        _run_turso_maintenance(
                            engine=active_engine,
                            platform=source.config.platform,
                            spool_dir=source.spool_dir,
                            redis_client=redis_client,
                            redis_queue_key=source.redis_queue_key,
                            stuck_seconds=int(args.ai_stuck_seconds),
                            verbose=verbose,
                        )
                    )

                tick_runtime.append(
                    {
                        "source": source,
                        "active_engine": active_engine,
                        "force_maintenance": bool(force_maintenance),
                        "rss_skip_reason": str(rss_skip_reason or ""),
                        "alias_resolved": int(alias_resolved),
                        "alias_inserted": int(alias_inserted),
                        "alias_has_more": bool(alias_has_more),
                        "alias_sync_finished": bool(alias_sync_finished),
                        "alias_sync_error": bool(alias_sync_error),
                        "backfill_processed": int(backfill_processed),
                        "backfill_written": int(backfill_written),
                        "backfill_has_more": bool(backfill_has_more),
                        "backfill_finished": bool(backfill_finished),
                        "backfill_cache_error": bool(backfill_cache_error),
                        "relation_cache_processed": int(relation_cache_processed),
                        "relation_cache_upserted": int(relation_cache_upserted),
                        "relation_cache_deleted": int(relation_cache_deleted),
                        "relation_cache_has_more": bool(relation_cache_has_more),
                        "relation_cache_finished": bool(relation_cache_finished),
                        "relation_cache_error": bool(relation_cache_error),
                        "stock_hot_processed": int(stock_hot_processed),
                        "stock_hot_written": int(stock_hot_written),
                        "stock_hot_has_more": bool(stock_hot_has_more),
                        "stock_hot_finished": bool(stock_hot_finished),
                        "stock_hot_error": bool(stock_hot_error),
                        "accepted": int(accepted),
                        "ingest_enqueue_error": bool(ingest_enqueue_error),
                        "spool_flushed": int(spool_flushed),
                        "spool_has_more": bool(spool_has_more),
                        "spool_flush_error": bool(spool_flush_error),
                        "recovered": int(recovered),
                        "flushed_redis": int(flushed_redis),
                        "maintenance_error": bool(maintenance_error),
                    }
                )

            for tick in tick_runtime:
                source = tick["source"]
                active_engine = tick["active_engine"]
                scheduled = 0
                schedule_error = False
                if active_engine is not None:
                    scheduled, schedule_error = _schedule_ai(
                        executor,
                        engine=active_engine,
                        platform=source.config.platform,
                        ai_cap=ai_cap,
                        low_inflight_now_get=(
                            low_priority_ai_gate.inflight
                            if low_priority_ai_gate is not None
                            else (lambda: 0)
                        ),
                        inflight_futures=inflight_futures,
                        inflight_owner_by_future=inflight_owner_by_future,
                        inflight_owner=source.config.name,
                        wakeup_event=wakeup_event,
                        config=config,
                        limiter=limiter,
                        verbose=verbose,
                        redis_client=redis_client,
                        redis_queue_key=source.redis_queue_key,
                        spool_dir=source.spool_dir,
                    )
                tick["scheduled"] = int(scheduled)
                tick["schedule_error"] = bool(schedule_error)

            for tick in tick_runtime:
                source = tick["source"]
                active_engine = tick["active_engine"]
                force_maintenance = bool(tick["force_maintenance"])
                rss_skip_reason = str(tick["rss_skip_reason"] or "")
                alias_resolved = int(tick["alias_resolved"])
                alias_inserted = int(tick["alias_inserted"])
                alias_has_more = bool(tick["alias_has_more"])
                alias_sync_finished = bool(tick["alias_sync_finished"])
                alias_sync_error = bool(tick["alias_sync_error"])
                backfill_processed = int(tick["backfill_processed"])
                backfill_written = int(tick["backfill_written"])
                backfill_has_more = bool(tick["backfill_has_more"])
                backfill_finished = bool(tick["backfill_finished"])
                backfill_cache_error = bool(tick["backfill_cache_error"])
                relation_cache_processed = int(tick["relation_cache_processed"])
                relation_cache_upserted = int(tick["relation_cache_upserted"])
                relation_cache_deleted = int(tick["relation_cache_deleted"])
                relation_cache_has_more = bool(tick["relation_cache_has_more"])
                relation_cache_finished = bool(tick["relation_cache_finished"])
                relation_cache_error = bool(tick["relation_cache_error"])
                stock_hot_processed = int(tick["stock_hot_processed"])
                stock_hot_written = int(tick["stock_hot_written"])
                stock_hot_has_more = bool(tick["stock_hot_has_more"])
                stock_hot_finished = bool(tick["stock_hot_finished"])
                stock_hot_error = bool(tick["stock_hot_error"])
                accepted = int(tick["accepted"])
                ingest_enqueue_error = bool(tick["ingest_enqueue_error"])
                spool_flushed = int(tick["spool_flushed"])
                spool_has_more = bool(tick["spool_has_more"])
                spool_flush_error = bool(tick["spool_flush_error"])
                recovered = int(tick["recovered"])
                flushed_redis = int(tick["flushed_redis"])
                maintenance_error = bool(tick["maintenance_error"])
                scheduled = int(tick["scheduled"])
                schedule_error = bool(tick["schedule_error"])

                scheduler_state["rss_inflight_now"] = int(_rss_inflight_now())
                rss_inflight_now = int(scheduler_state["rss_inflight_now"])
                low_budget = _compute_low_priority_budget(
                    ai_cap=int(ai_cap),
                    rss_inflight_now=int(rss_inflight_now),
                )
                backfill_run_cap = _compute_backfill_max_stocks_per_run(
                    low_budget=int(low_budget)
                )

                def _zero_low_inflight() -> int:
                    return 0

                low_inflight_now_getter: Callable[[], int] = (
                    low_priority_ai_gate.inflight
                    if low_priority_ai_gate is not None
                    else _zero_low_inflight
                )

                def _no_due_ai_pending() -> bool:
                    return False

                has_due_ai_pending_getter: Callable[[], bool] = _no_due_ai_pending
                if active_engine is not None:
                    engine_for_due = active_engine
                    platform_for_due = source.config.platform

                    def _has_due_ai_pending() -> bool:
                        return _has_due_ai_posts(
                            engine=engine_for_due,
                            platform=platform_for_due,
                            verbose=False,
                            redis_client=redis_client,
                            redis_queue_key=source.redis_queue_key,
                        )

                    has_due_ai_pending_getter = _has_due_ai_pending

                should_continue_low_priority = _build_low_priority_should_continue(
                    ai_cap=int(ai_cap),
                    rss_inflight_now_get=lambda: int(_rss_inflight_now()),
                    low_inflight_now_get=low_inflight_now_getter,
                    has_due_ai_pending_get=has_due_ai_pending_getter,
                )
                cycle_triggered = bool(do_maintenance or force_maintenance)
                if cycle_triggered and active_engine is not None:
                    if not bool(source.cycle_running):
                        source.cycle_started_at = now
                    source.cycle_running = True
                run_to_completion = bool(source.cycle_running)
                periodic_interval = (
                    0.0 if run_to_completion else research_cache_interval_seconds
                )
                alias_interval = (
                    0.0 if run_to_completion else alias_sync_interval_seconds
                )
                stock_hot_interval = (
                    0.0 if run_to_completion else stock_hot_cache_interval_seconds
                )

                spool_trigger = _should_start_spool_flush(source=source)
                (
                    source.spool_flush_future,
                    source.spool_flush_next_at,
                    start_spool_flush,
                ) = _maybe_start_periodic_job(
                    executor=spool_executor,
                    future=source.spool_flush_future,
                    active_engine=active_engine,
                    trigger=spool_trigger,
                    now=now,
                    next_run_at=source.spool_flush_next_at,
                    interval_seconds=float(SPOOL_FLUSH_RETRY_INTERVAL_SECONDS),
                    wakeup_event=wakeup_event,
                    submit_fn=_submit_spool_flush_job,
                    submit_kwargs={
                        "spool_dir": source.spool_dir,
                        "redis_client": redis_client,
                        "redis_queue_key": source.redis_queue_key,
                        "verbose": bool(verbose),
                    },
                )
                if start_spool_flush:
                    _mark_spool_flush_started(source=source)
                    if verbose:
                        print(
                            f"[spool:{source.config.name}] flush_start trigger=1",
                            flush=True,
                        )

                alias_trigger = bool(
                    (
                        do_maintenance
                        or force_maintenance
                        or run_to_completion
                        or alias_has_more
                    )
                    and int(low_budget) > 0
                )
                (
                    source.alias_sync_future,
                    source.alias_sync_next_at,
                    start_alias_sync,
                ) = _maybe_start_periodic_job(
                    executor=alias_executor,
                    future=source.alias_sync_future,
                    active_engine=active_engine,
                    trigger=alias_trigger,
                    now=now,
                    next_run_at=source.alias_sync_next_at,
                    interval_seconds=alias_interval,
                    wakeup_event=wakeup_event,
                    submit_fn=_submit_alias_sync_job,
                    submit_kwargs={
                        "ai_max_inflight": int(low_budget),
                        "should_continue": should_continue_low_priority,
                    },
                )
                if start_alias_sync and verbose:
                    print(
                        f"[alias:{source.config.name}] sync_start trigger={1 if alias_trigger else 0} "
                        f"low_budget={int(low_budget)} interval={int(alias_interval)}s",
                        flush=True,
                    )

                backfill_trigger = bool(
                    (
                        do_maintenance
                        or force_maintenance
                        or run_to_completion
                        or backfill_has_more
                    )
                    and int(low_budget) > 0
                )
                (
                    source.backfill_cache_future,
                    source.backfill_cache_next_at,
                    start_backfill_cache,
                ) = _maybe_start_periodic_job(
                    executor=backfill_executor,
                    future=source.backfill_cache_future,
                    active_engine=active_engine,
                    trigger=backfill_trigger,
                    now=now,
                    next_run_at=source.backfill_cache_next_at,
                    interval_seconds=periodic_interval,
                    wakeup_event=wakeup_event,
                    submit_fn=_submit_backfill_cache_job,
                    submit_kwargs={
                        "max_stocks_per_run": int(backfill_run_cap),
                        "should_continue": should_continue_low_priority,
                    },
                )
                if start_backfill_cache and verbose:
                    print(
                        f"[backfill_cache:{source.config.name}] sync_start trigger={1 if backfill_trigger else 0} "
                        f"backfill_run_cap={int(backfill_run_cap)} interval={int(periodic_interval)}s",
                        flush=True,
                    )

                relation_trigger = bool(
                    (
                        do_maintenance
                        or force_maintenance
                        or run_to_completion
                        or relation_cache_has_more
                    )
                    and int(low_budget) > 0
                )
                (
                    source.relation_cache_future,
                    source.relation_cache_next_at,
                    start_relation_cache,
                ) = _maybe_start_periodic_job(
                    executor=research_executor,
                    future=source.relation_cache_future,
                    active_engine=active_engine,
                    trigger=relation_trigger,
                    now=now,
                    next_run_at=source.relation_cache_next_at,
                    interval_seconds=periodic_interval,
                    wakeup_event=wakeup_event,
                    submit_fn=_submit_relation_candidates_cache_job,
                    submit_kwargs={
                        "ai_max_inflight": int(low_budget),
                        "should_continue": should_continue_low_priority,
                    },
                )
                if start_relation_cache and verbose:
                    print(
                        f"[relation_cache:{source.config.name}] sync_start trigger={1 if relation_trigger else 0} "
                        f"low_budget={int(low_budget)} interval={int(periodic_interval)}s",
                        flush=True,
                    )

                stock_hot_trigger = bool(
                    (
                        do_maintenance
                        or force_maintenance
                        or run_to_completion
                        or stock_hot_has_more
                    )
                    and int(low_budget) > 0
                )
                (
                    source.stock_hot_cache_future,
                    source.stock_hot_cache_next_at,
                    start_stock_hot_cache,
                ) = _maybe_start_periodic_job(
                    executor=research_executor,
                    future=source.stock_hot_cache_future,
                    active_engine=active_engine,
                    trigger=stock_hot_trigger,
                    now=now,
                    next_run_at=source.stock_hot_cache_next_at,
                    interval_seconds=stock_hot_interval,
                    wakeup_event=wakeup_event,
                    submit_fn=_submit_stock_hot_cache_job,
                    submit_kwargs={"should_continue": should_continue_low_priority},
                )
                if start_stock_hot_cache and verbose:
                    print(
                        f"[stock_hot_cache:{source.config.name}] sync_start trigger={1 if stock_hot_trigger else 0} "
                        f"low_budget={int(low_budget)} interval={int(stock_hot_interval)}s",
                        flush=True,
                    )

                turso_error = _build_source_turso_error(
                    maintenance_error=bool(maintenance_error),
                    spool_flush_error=bool(spool_flush_error),
                    schedule_error=bool(schedule_error),
                    alias_sync_error=bool(alias_sync_error),
                    backfill_cache_error=bool(backfill_cache_error),
                    relation_cache_error=bool(relation_cache_error),
                    stock_hot_error=bool(stock_hot_error),
                )
                if turso_error:
                    source.turso_ready = False
                    source.cycle_running = False
                    source.turso_next_ready_check_at = min(
                        float(source.turso_next_ready_check_at) or float("inf"),
                        time.time() + float(TURSO_READY_RETRY_SECONDS),
                    )
                inflight_for_source = _count_inflight_for_owner(
                    inflight_owner_by_future, owner=source.config.name
                )
                due_ai_pending = False
                if bool(source.cycle_running) and active_engine is not None:
                    can_try_finish_cycle = bool(
                        inflight_for_source <= 0
                        and source.alias_sync_future is None
                        and source.backfill_cache_future is None
                        and source.relation_cache_future is None
                        and source.stock_hot_cache_future is None
                        and (not alias_has_more)
                        and (not backfill_has_more)
                        and (not relation_cache_has_more)
                        and (not stock_hot_has_more)
                    )
                    if can_try_finish_cycle:
                        due_ai_pending = _has_due_ai_posts(
                            engine=active_engine,
                            platform=source.config.platform,
                            verbose=verbose,
                            redis_client=redis_client,
                            redis_queue_key=source.redis_queue_key,
                        )
                        if not due_ai_pending:
                            source.cycle_running = False
                            source.cycle_finished_at = time.time()
                if active_engine is not None and source.turso_ready:
                    cycle_status = (
                        WORKER_PROGRESS_STATUS_RUNNING
                        if source.cycle_running
                        else WORKER_PROGRESS_STATUS_IDLE
                    )
                    cycle_started_at = _format_epoch_to_cst(source.cycle_started_at)
                    cycle_finished_at = _format_epoch_to_cst(source.cycle_finished_at)
                    _save_worker_progress_state(
                        source=source,
                        stage=WORKER_PROGRESS_STAGE_CYCLE,
                        payload={
                            "status": cycle_status,
                            "started_at": cycle_started_at,
                            "finished_at": cycle_finished_at,
                            "next_run_at": _format_epoch_to_cst(maintenance_next_at),
                            "running": bool(source.cycle_running),
                            "rss_accepted": int(accepted),
                        },
                        verbose=verbose,
                    )
                    _save_worker_progress_state(
                        source=source,
                        stage=WORKER_PROGRESS_STAGE_AI,
                        payload={
                            "status": (
                                WORKER_PROGRESS_STATUS_RUNNING
                                if (inflight_for_source > 0 or due_ai_pending)
                                else WORKER_PROGRESS_STATUS_IDLE
                            ),
                            "inflight": int(inflight_for_source),
                            "scheduled": int(scheduled),
                            "recovered": int(recovered),
                            "due_pending": bool(due_ai_pending),
                            "next_run_at": _format_epoch_to_cst(maintenance_next_at),
                        },
                        verbose=verbose,
                    )
                    _save_worker_progress_state(
                        source=source,
                        stage=WORKER_PROGRESS_STAGE_ALIAS,
                        payload={
                            "status": (
                                WORKER_PROGRESS_STATUS_RUNNING
                                if (
                                    source.alias_sync_future is not None
                                    or alias_has_more
                                )
                                else WORKER_PROGRESS_STATUS_IDLE
                            ),
                            "inflight": bool(source.alias_sync_future is not None),
                            "resolved": int(alias_resolved),
                            "inserted": int(alias_inserted),
                            "has_more": bool(alias_has_more),
                            "next_run_at": _format_epoch_to_cst(
                                source.alias_sync_next_at
                            ),
                        },
                        verbose=verbose,
                    )
                    _save_worker_progress_state(
                        source=source,
                        stage=WORKER_PROGRESS_STAGE_BACKFILL,
                        payload={
                            "status": (
                                WORKER_PROGRESS_STATUS_RUNNING
                                if (
                                    source.backfill_cache_future is not None
                                    or backfill_has_more
                                )
                                else WORKER_PROGRESS_STATUS_IDLE
                            ),
                            "inflight": bool(source.backfill_cache_future is not None),
                            "processed": int(backfill_processed),
                            "written": int(backfill_written),
                            "has_more": bool(backfill_has_more),
                            "next_run_at": _format_epoch_to_cst(
                                source.backfill_cache_next_at
                            ),
                        },
                        verbose=verbose,
                    )
                    _save_worker_progress_state(
                        source=source,
                        stage=WORKER_PROGRESS_STAGE_RELATION,
                        payload={
                            "status": (
                                WORKER_PROGRESS_STATUS_RUNNING
                                if (
                                    source.relation_cache_future is not None
                                    or relation_cache_has_more
                                )
                                else WORKER_PROGRESS_STATUS_IDLE
                            ),
                            "inflight": bool(source.relation_cache_future is not None),
                            "processed": int(relation_cache_processed),
                            "upserted": int(relation_cache_upserted),
                            "deleted": int(relation_cache_deleted),
                            "has_more": bool(relation_cache_has_more),
                            "next_run_at": _format_epoch_to_cst(
                                source.relation_cache_next_at
                            ),
                        },
                        verbose=verbose,
                    )
                    _save_worker_progress_state(
                        source=source,
                        stage=WORKER_PROGRESS_STAGE_STOCK_HOT,
                        payload={
                            "status": (
                                WORKER_PROGRESS_STATUS_RUNNING
                                if (
                                    source.stock_hot_cache_future is not None
                                    or stock_hot_has_more
                                )
                                else WORKER_PROGRESS_STATUS_IDLE
                            ),
                            "inflight": bool(source.stock_hot_cache_future is not None),
                            "processed": int(stock_hot_processed),
                            "written": int(stock_hot_written),
                            "has_more": bool(stock_hot_has_more),
                            "next_run_at": _format_epoch_to_cst(
                                source.stock_hot_cache_next_at
                            ),
                        },
                        verbose=verbose,
                    )

                if verbose and (
                    do_maintenance
                    or start_spool_flush
                    or start_alias_sync
                    or alias_sync_finished
                    or alias_has_more
                    or start_backfill_cache
                    or backfill_finished
                    or backfill_has_more
                    or start_relation_cache
                    or relation_cache_finished
                    or relation_cache_has_more
                    or start_stock_hot_cache
                    or stock_hot_finished
                    or stock_hot_has_more
                    or accepted > 0
                    or ingest_enqueue_error
                    or recovered > 0
                    or flushed_redis > 0
                    or spool_flushed > 0
                    or alias_inserted > 0
                    or source.cycle_running
                ):
                    next_alias_sync_in = max(
                        0.0, source.alias_sync_next_at - time.time()
                    )
                    next_backfill_in = max(
                        0.0, source.backfill_cache_next_at - time.time()
                    )
                    next_relation_in = max(
                        0.0, source.relation_cache_next_at - time.time()
                    )
                    next_stock_hot_in = max(
                        0.0, source.stock_hot_cache_next_at - time.time()
                    )
                    next_rss_in = (
                        -1.0
                        if source.rss_next_ingest_at == float("inf")
                        else max(0.0, source.rss_next_ingest_at - time.time())
                    )
                    next_turso_in = -1.0
                    if not source.turso_ready:
                        next_turso_in = max(
                            0.0, float(source.turso_next_ready_check_at) - time.time()
                        )
                    print(
                        f"[tick:{source.config.name}] turso_ready={1 if active_engine is not None else 0} "
                        f"ai_cap={int(ai_cap)} rss_inflight_now={int(rss_inflight_now)} low_budget={int(low_budget)} "
                        f"backfill_budget={int(low_budget)} "
                        f"backfill_run_cap={int(backfill_run_cap)} yield_to_rss={1 if int(low_budget) <= 0 else 0} "
                        f"low_mode={LOW_PRIORITY_SCHEDULER_MODE} "
                        f"low_ai_inflight={low_priority_ai_gate.inflight() if low_priority_ai_gate is not None else 0} "
                        f"inflight={inflight_for_source} alias_inflight={1 if source.alias_sync_future is not None else 0} "
                        f"backfill_inflight={1 if source.backfill_cache_future is not None else 0} "
                        f"relation_inflight={1 if source.relation_cache_future is not None else 0} "
                        f"stock_hot_inflight={1 if source.stock_hot_cache_future is not None else 0} "
                        f"spool_inflight={1 if source.spool_flush_future is not None else 0} "
                        f"ai_scheduled={scheduled} "
                        f"ai_recovered={recovered} redis_flush={flushed_redis} spool_flush={spool_flushed} "
                        f"enqueue_error={1 if ingest_enqueue_error else 0} "
                        f"alias_resolved={alias_resolved} alias_inserted={alias_inserted} "
                        f"backfill_processed={backfill_processed} backfill_written={backfill_written} "
                        f"relation_processed={relation_cache_processed} relation_upserted={relation_cache_upserted} relation_deleted={relation_cache_deleted} "
                        f"stock_hot_processed={stock_hot_processed} stock_hot_written={stock_hot_written} "
                        f"rss_accepted={accepted} rss_skip={rss_skip_reason or '-'} "
                        f"next_maint={int(next_maintenance_in)}s "
                        f"next_alias={int(next_alias_sync_in)}s "
                        f"next_backfill={int(next_backfill_in)}s "
                        f"next_relation={int(next_relation_in)}s "
                        f"next_stock_hot={int(next_stock_hot_in)}s "
                        f"next_rss={int(next_rss_in) if next_rss_in >= 0 else -1}s "
                        f"next_turso={int(next_turso_in) if next_turso_in >= 0 else -1}s",
                        flush=True,
                    )

            next_deadline = maintenance_next_at
            any_alias_inflight = False
            any_backfill_inflight = False
            any_relation_inflight = False
            any_stock_hot_inflight = False
            any_rss_inflight = False
            any_spool_flush_inflight = False
            for source in sources:
                if source.alias_sync_future is not None:
                    any_alias_inflight = True
                if source.backfill_cache_future is not None:
                    any_backfill_inflight = True
                if source.relation_cache_future is not None:
                    any_relation_inflight = True
                if source.stock_hot_cache_future is not None:
                    any_stock_hot_inflight = True
                if source.rss_ingest_future is not None:
                    any_rss_inflight = True
                if source.spool_flush_future is not None:
                    any_spool_flush_inflight = True
                if source.alias_sync_next_at > 0:
                    next_deadline = min(next_deadline, source.alias_sync_next_at)
                if source.backfill_cache_next_at > 0:
                    next_deadline = min(next_deadline, source.backfill_cache_next_at)
                if source.relation_cache_next_at > 0:
                    next_deadline = min(next_deadline, source.relation_cache_next_at)
                if source.stock_hot_cache_next_at > 0:
                    next_deadline = min(next_deadline, source.stock_hot_cache_next_at)
                if source.spool_flush_next_at > 0:
                    next_deadline = min(next_deadline, source.spool_flush_next_at)
                if source.rss_next_ingest_at != float("inf"):
                    next_deadline = min(next_deadline, source.rss_next_ingest_at)
                if not source.turso_ready:
                    next_deadline = min(
                        next_deadline, float(source.turso_next_ready_check_at)
                    )
            timeout = max(0.0, next_deadline - time.time())
            if _should_wait_with_event(
                ai_inflight=bool(inflight_futures),
                any_alias_inflight=bool(any_alias_inflight),
                any_backfill_inflight=bool(any_backfill_inflight),
                any_relation_inflight=bool(any_relation_inflight),
                any_stock_hot_inflight=bool(any_stock_hot_inflight),
                any_rss_inflight=bool(any_rss_inflight),
                any_spool_flush_inflight=bool(any_spool_flush_inflight),
            ):
                wakeup_event.wait(timeout)
            else:
                time.sleep(timeout)


__all__ = ["main", "parse_args", "LLMConfig"]
