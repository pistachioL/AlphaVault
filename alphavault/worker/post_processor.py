from __future__ import annotations

import json
import time

from alphavault.ai.analyze import (
    AnalyzeResult,
    analyze_with_litellm,
    format_llm_error_one_line,
    validate_and_adjust_assertions,
)
from alphavault.ai.topic_prompt_v3 import TOPIC_PROMPT_VERSION
from alphavault.db.turso_db import TursoEngine
from alphavault.db.turso_queue import (
    CloudPost,
    load_cloud_post,
    mark_ai_error,
    write_assertions_and_mark_done,
)
from alphavault.rss.utils import (
    RateLimiter,
    build_analysis_context,
    build_row_meta,
    now_str,
)
from alphavault.research_backfill_cache import mark_stock_backfill_dirty_from_assertions
from alphavault.research_stock_cache import mark_stock_dirty_from_assertions
from alphavault.worker.backoff import backoff_seconds
from alphavault.worker.post_processor_topic_prompt_v3 import (
    map_topic_prompt_items_to_assertions,
    process_one_post_uid_topic_prompt_v3,
)
from alphavault.worker.post_processor_utils import (
    as_str_list,
    build_assertion_outbox_event_payload,
    ensure_prefetched_post_persisted,
    json_to_str_list,
    score_from_assertions,
)
from alphavault.worker.local_cache import (
    apply_outbox_event_payload,
    open_local_cache,
    resolve_local_cache_db_path,
)
from alphavault.worker.runtime_models import LLMConfig


def process_one_post_uid(
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
    try:
        if str(config.prompt_version or "").strip() == TOPIC_PROMPT_VERSION:
            return process_one_post_uid_topic_prompt_v3(
                engine=engine,
                post_uid=post_uid,
                config=config,
                limiter=limiter,
                prefetched_post=prefetched_post,
                prefetched_recent=prefetched_recent,
                source_name=str(source_name or "").strip(),
                outbox_source=outbox_source,
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
        processed_at = now_str()
        archived_at = now_str()
        if prefetched_post is not None:
            ensure_prefetched_post_persisted(
                engine=engine,
                post=prefetched_post,
                archived_at=archived_at,
                ingested_at=int(time.time()),
            )

        outbox_payload = build_assertion_outbox_event_payload(
            post=post,
            final_status=final_result.status,
            rows=assertions,
        )
        write_assertions_and_mark_done(
            engine,
            post_uid=post_uid,
            final_status=final_result.status,
            invest_score=float(final_result.invest_score),
            processed_at=processed_at,
            model=config.model,
            prompt_version=config.prompt_version,
            archived_at=archived_at,
            ai_result_json=None,
            assertions=assertions,
            outbox_source=str(outbox_source or "").strip(),
            outbox_author=str(post.author or "").strip(),
            outbox_event_json=json.dumps(outbox_payload, ensure_ascii=False),
        )
        try:
            db_path = resolve_local_cache_db_path(
                source_name=str(source_name or "").strip()
            )
            with open_local_cache(db_path=db_path) as cache_conn:
                apply_outbox_event_payload(cache_conn, payload=outbox_payload)
        except Exception as cache_err:
            if config.verbose:
                print(
                    f"[local_cache] write_error post_uid={post_uid} "
                    f"{type(cache_err).__name__}: {cache_err}",
                    flush=True,
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
            try:
                mark_stock_backfill_dirty_from_assertions(
                    engine,
                    assertions=assertions,
                    reason="ai_done",
                )
            except BaseException:
                if config.verbose:
                    print(
                        f"[backfill_cache] mark_dirty_failed post_uid={post_uid}",
                        flush=True,
                    )
        return True
    except Exception as err:
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
        msg = f"ai:{format_llm_error_one_line(err, limit=700)}{ctx}"
        now_epoch = int(time.time())
        retry_count = int(getattr(prefetched_post, "ai_retry_count", 1) or 1)
        next_retry = now_epoch + backoff_seconds(retry_count)
        try:
            mark_ai_error(
                engine,
                post_uid=post_uid,
                error=msg,
                next_retry_at=next_retry,
                archived_at=now_str(),
            )
        except Exception as mark_err:
            if config.verbose:
                print(
                    f"[llm] mark_error_failed {post_uid} {type(mark_err).__name__}: {mark_err}",
                    flush=True,
                )
        print(f"[llm] error {post_uid} {msg}", flush=True)
        return False


__all__ = [
    "as_str_list",
    "backoff_seconds",
    "ensure_prefetched_post_persisted",
    "json_to_str_list",
    "map_topic_prompt_items_to_assertions",
    "process_one_post_uid",
    "process_one_post_uid_topic_prompt_v3",
    "score_from_assertions",
]
