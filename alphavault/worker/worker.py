from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from sqlalchemy.engine import Engine

from alphavault.ai.analyze import (
    AI_MODE_COMPLETION,
    AI_MODE_RESPONSES,
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_AI_RETRY_COUNT,
    DEFAULT_AI_TEMPERATURE,
    DEFAULT_MODEL,
    DEFAULT_PROMPT_VERSION,
    AnalyzeResult,
    analyze_with_litellm,
    format_llm_error_one_line,
    validate_and_adjust_assertions,
)
from alphavault.db.turso_db import get_turso_engine_from_env
from alphavault.db.turso_queue import (
    ensure_cloud_queue_schema,
    load_cloud_post,
    mark_ai_error,
    recover_done_without_processed_at,
    recover_stuck_ai_tasks,
    select_due_post_uids,
    try_mark_ai_running,
    write_assertions_and_mark_done,
)
from alphavault.rss.utils import (
    RateLimiter,
    build_analysis_context,
    build_row_meta,
    env_bool,
    env_float,
    env_int,
    now_str,
    parse_active_hours,
    parse_rss_urls,
    sleep_until_active,
)
from alphavault.worker.ingest import _ingest_rss_many_once
from alphavault.worker.redis_queue import _flush_redis_to_turso, _try_get_redis
from alphavault.worker.spool import _ensure_spool_dir, _flush_spool_to_turso


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
    ai_stream_env = env_bool("AI_STREAM")
    ai_stream = True
    if ai_stream_env is not None:
        ai_stream = bool(ai_stream_env)
    elif args.ai_stream:
        ai_stream = True

    trace_out = args.trace_out
    trace_out_env = os.getenv("AI_TRACE_OUT", "").strip()
    if trace_out_env and trace_out is None:
        trace_out = Path(trace_out_env)

    base_url = str(args.base_url or "").strip()
    if bool(args.verbose) and base_url:
        if not base_url.rstrip("/").endswith("/v1"):
            print(f"[ai] warn base_url_maybe_missing_v1 base_url={base_url}", flush=True)

    api_key = ""
    if args.api_key:
        api_key = str(args.api_key).strip()
    else:
        api_key = os.getenv("AI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing AI_API_KEY. Set AI_API_KEY.")

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
        ai_reasoning_effort=str(args.ai_reasoning_effort or DEFAULT_AI_REASONING_EFFORT),
        ai_rpm=max(0.0, float(args.ai_rpm or 0.0)),
        ai_timeout_seconds=max(1.0, float(args.ai_timeout_sec)),
        trace_out=trace_out,
        verbose=bool(args.verbose),
    )


def _backoff_seconds(retry_count: int) -> int:
    n = max(1, int(retry_count))
    delay = 30 * (2 ** max(0, n - 1))
    return int(min(3600, delay))


def _process_one_post_uid(
    *,
    engine: Engine,
    post_uid: str,
    config: LLMConfig,
    limiter: RateLimiter,
) -> None:
    try:
        post = load_cloud_post(engine, post_uid)
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
            final_result = AnalyzeResult(status="irrelevant", invest_score=final_result.invest_score, assertions=[])
        else:
            final_result.assertions = validate_and_adjust_assertions(
                final_result.assertions,
                commentary_text=analysis_context["commentary_text"],
                quoted_text=analysis_context["quoted_text"],
            )

        assertions = final_result.assertions if final_result.status == "relevant" else []

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
        retry_count = 1
        try:
            retry_count = int(post.ai_retry_count or 1)  # type: ignore[name-defined]
        except Exception:
            retry_count = 1
        next_retry = now_epoch + _backoff_seconds(retry_count)
        try:
            mark_ai_error(engine, post_uid=post_uid, error=msg, next_retry_at=next_retry, archived_at=now_str())
        except Exception as mark_e:
            if config.verbose:
                print(f"[llm] mark_error_failed {post_uid} {type(mark_e).__name__}: {mark_e}", flush=True)
        print(f"[llm] error {post_uid} {msg}", flush=True)


def parse_args() -> argparse.Namespace:
    ai_retries_env = env_int("AI_RETRIES")
    ai_rpm_env = env_float("AI_RPM")
    ai_timeout_env = env_float("AI_TIMEOUT_SEC")
    ai_max_inflight_env = env_int("AI_MAX_INFLIGHT")

    parser = argparse.ArgumentParser(description="Weibo RSS -> Turso queue -> AI -> Turso")
    parser.add_argument("--rss-url", action="append", default=[], help="RSS 地址（可重复传多次）")
    parser.add_argument("--rss-urls", default="", help="多个 RSS 地址（逗号或换行分隔）")
    parser.add_argument("--author", default="", help="作者名（为空则从 RSS 里取）")
    parser.add_argument("--user-id", default="", help="微博用户ID（可为空，自动推断）")
    parser.add_argument("--active-hours", default="", help="只在这些小时运行（CST），格式: 6-22；为空=全天")
    parser.add_argument("--limit", type=int, default=0, help="最多处理多少条（0 表示不限）")
    parser.add_argument("--rss-timeout", type=float, default=15.0, help="RSS HTTP 超时秒数")
    parser.add_argument("--interval-seconds", type=float, default=0.0, help="轮询间隔（0=读 RSS_INTERVAL_SECONDS 或默认 600）")
    parser.add_argument("--worker-threads", type=int, default=0, help="后台 AI 线程数（0=自动）")
    parser.add_argument("--ai-stuck-seconds", type=int, default=600, help="running 超过多久算卡死（秒）")

    # AI config (litellm only; mostly via env)
    parser.add_argument("--model", default=os.getenv("AI_MODEL", DEFAULT_MODEL))
    parser.add_argument("--base-url", default=os.getenv("AI_BASE_URL", "").strip(), help="可选：OpenAI 兼容接口 base_url（也可用 AI_BASE_URL）")
    parser.add_argument("--api-key", default=None, help="可选：API Key（默认读 AI_API_KEY）")
    parser.add_argument("--api-mode", default=os.getenv("AI_API_MODE", DEFAULT_AI_MODE), choices=[AI_MODE_COMPLETION, AI_MODE_RESPONSES])
    parser.add_argument("--ai-stream", action="store_true")
    parser.add_argument("--prompt-version", default=os.getenv("AI_PROMPT_VERSION", DEFAULT_PROMPT_VERSION))
    parser.add_argument(
        "--ai-retries",
        type=int,
        default=ai_retries_env if ai_retries_env is not None else DEFAULT_AI_RETRY_COUNT,
    )
    parser.add_argument("--ai-temperature", type=float, default=float(os.getenv("AI_TEMPERATURE", str(DEFAULT_AI_TEMPERATURE))))
    parser.add_argument("--ai-reasoning-effort", default=os.getenv("AI_REASONING_EFFORT", DEFAULT_AI_REASONING_EFFORT), choices=["none", "minimal", "low", "medium", "high", "xhigh"])
    parser.add_argument(
        "--ai-rpm",
        type=float,
        default=ai_rpm_env if ai_rpm_env is not None else 12.0,
    )
    parser.add_argument(
        "--ai-timeout-sec",
        type=float,
        default=ai_timeout_env if ai_timeout_env is not None else 1000.0,
    )
    parser.add_argument(
        "--ai-max-inflight",
        type=int,
        default=ai_max_inflight_env if ai_max_inflight_env is not None else 12,
    )
    parser.add_argument("--trace-out", type=Path, default=None)
    parser.add_argument("--relevant-threshold", type=float, default=0.35, help="相关度阈值")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rss_urls = parse_rss_urls(args)
    if not rss_urls:
        raise RuntimeError("Missing RSS url(s). Set RSS_URLS/RSS_URL or pass --rss-url/--rss-urls.")
    if args.verbose:
        print(f"[rss] sources={len(rss_urls)}", flush=True)

    active_hours_value = args.active_hours.strip() if args.active_hours else os.getenv("RSS_ACTIVE_HOURS", "").strip()
    active_hours = None
    if active_hours_value:
        active_hours = parse_active_hours(active_hours_value)

    interval = float(args.interval_seconds or 0.0)
    if interval <= 0:
        interval = float(os.getenv("RSS_INTERVAL_SECONDS", "600") or "600")
    interval = max(1.0, interval)

    config = _build_config(args)
    limiter = RateLimiter(config.ai_rpm)

    if args.worker_threads and args.worker_threads > 0:
        worker_threads = int(args.worker_threads)
    elif args.ai_max_inflight and args.ai_max_inflight > 0:
        worker_threads = int(args.ai_max_inflight)
    else:
        worker_threads = 4
    if args.ai_max_inflight and args.ai_max_inflight > 0:
        worker_threads = min(worker_threads, int(args.ai_max_inflight))

    engine = get_turso_engine_from_env()
    turso_ready = False

    spool_dir = _ensure_spool_dir()
    redis_client, redis_queue_key = _try_get_redis()
    if args.verbose:
        print(f"[spool] dir={spool_dir}", flush=True)
        if redis_client:
            print(f"[redis] enabled key={redis_queue_key}", flush=True)

    def _ensure_turso() -> bool:
        nonlocal turso_ready
        if turso_ready:
            return True
        try:
            ensure_cloud_queue_schema(engine, verbose=bool(args.verbose))
            turso_ready = True
            print("[turso] ready", flush=True)
            return True
        except Exception as e:
            turso_ready = False
            if args.verbose:
                print(f"[turso] not_ready {type(e).__name__}: {e}", flush=True)
            return False

    def _schedule_ai(executor: ThreadPoolExecutor, *, engine: Optional[Engine]) -> Tuple[int, bool]:
        if engine is None:
            return 0, False
        now_epoch = int(time.time())
        try:
            due = select_due_post_uids(engine, now_epoch=now_epoch, limit=max(1, worker_threads * 2))
        except Exception as e:
            if args.verbose:
                print(f"[ai] select_due_error {type(e).__name__}: {e}", flush=True)
            return 0, True

        scheduled = 0
        for post_uid in due:
            try:
                ok = try_mark_ai_running(engine, post_uid=post_uid, now_epoch=now_epoch)
            except Exception as e:
                if args.verbose:
                    print(f"[ai] mark_running_error {type(e).__name__}: {e}", flush=True)
                return scheduled, True
            if not ok:
                continue
            executor.submit(_process_one_post_uid, engine=engine, post_uid=post_uid, config=config, limiter=limiter)
            scheduled += 1
        return scheduled, False

    limit = args.limit if args.limit and args.limit > 0 else None

    with ThreadPoolExecutor(max_workers=worker_threads) as executor:
        loop_idx = 0
        while True:
            loop_idx += 1
            if active_hours is not None:
                sleep_until_active(active_hours, verbose=bool(args.verbose))

            if not turso_ready:
                _ensure_turso()
            active_engine: Optional[Engine] = engine if turso_ready else None

            inserted, ingest_turso_error = _ingest_rss_many_once(
                rss_urls=rss_urls,
                engine=active_engine,
                spool_dir=spool_dir,
                redis_client=redis_client,
                redis_queue_key=redis_queue_key,
                author=args.author.strip(),
                user_id=(args.user_id.strip() or None),
                limit=limit,
                rss_timeout=float(args.rss_timeout),
                verbose=bool(args.verbose),
            )

            turso_error = bool(ingest_turso_error)
            recovered = 0
            flushed_redis = 0
            flushed_spool = 0
            scheduled = 0

            if active_engine is not None:
                try:
                    recovered = recover_stuck_ai_tasks(
                        active_engine,
                        now_epoch=int(time.time()),
                        stuck_seconds=max(60, int(args.ai_stuck_seconds)),
                        verbose=bool(args.verbose),
                    )
                    recovered += recover_done_without_processed_at(active_engine, verbose=bool(args.verbose))
                except Exception as e:
                    turso_error = True
                    if args.verbose:
                        print(f"[ai] recover_error {type(e).__name__}: {e}", flush=True)

                flushed_redis, flush_redis_turso_error = _flush_redis_to_turso(
                    client=redis_client,
                    queue_key=redis_queue_key,
                    spool_dir=spool_dir,
                    engine=active_engine,
                    max_items=200,
                    verbose=bool(args.verbose),
                )
                flushed_spool, flush_spool_turso_error = _flush_spool_to_turso(
                    spool_dir=spool_dir,
                    engine=active_engine,
                    max_items=200,
                    verbose=bool(args.verbose),
                )
                scheduled, schedule_turso_error = _schedule_ai(executor, engine=active_engine)

                turso_error = bool(turso_error or flush_redis_turso_error or flush_spool_turso_error or schedule_turso_error)

            if turso_error:
                turso_ready = False

            if args.verbose:
                print(
                    f"[loop] idx={loop_idx} turso_ready={int(bool(active_engine))} "
                    f"rss_inserted={inserted} redis_flush={flushed_redis} spool_flush={flushed_spool} "
                    f"ai_recovered={recovered} ai_scheduled={scheduled}",
                    flush=True,
                )

            time.sleep(interval)


__all__ = ["main", "parse_args", "LLMConfig"]
