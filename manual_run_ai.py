from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

from alphavault.env import load_dotenv_if_present

from alphavault.logging_config import (
    add_log_level_argument,
    configure_logging,
    get_logger,
)
from alphavault.ai.analyze import (
    AI_MODE_COMPLETION,
    AI_MODE_RESPONSES,
    DEFAULT_PROMPT_VERSION,
)
from alphavault.constants import (
    ENV_AI_PROMPT_VERSION,
    ENV_AI_STREAM,
    ENV_AI_TRACE_OUT,
)
from alphavault.db.postgres_db import PostgresEngine, ensure_postgres_engine
from alphavault.db.postgres_env import (
    require_postgres_source_platform,
    require_postgres_source_from_env,
)
from alphavault.infra.ai.runtime_config import (
    AI_REASONING_EFFORT_CHOICES,
    AI_TASK_POST_ANALYSIS,
    ai_task_runtime_config_from_env,
    apply_ai_runtime_config_overrides,
)
from alphavault.rss.utils import RateLimiter, env_bool
from alphavault.worker.post_processor import process_one_post_uid
from alphavault.worker.runtime_models import LLMConfig


DEFAULT_RELEVANT_THRESHOLD = 0.35
logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    env_config = ai_task_runtime_config_from_env(
        task_key=AI_TASK_POST_ANALYSIS,
        timeout_seconds_default=1000.0,
    )

    parser = argparse.ArgumentParser(
        description="Manual run AI for specific post_uid(s)"
    )
    parser.add_argument(
        "--post-uids",
        type=str,
        default="",
        help="要跑哪些 post_uid（逗号/换行分隔；纯数字会自动加 weibo:）",
    )

    # AI config (same meaning as worker)
    parser.add_argument("--model", default=env_config.model)
    parser.add_argument(
        "--base-url",
        default=env_config.base_url,
        help="可选：OpenAI 兼容接口 base_url（也可用 AI_BASE_URL）",
    )
    parser.add_argument(
        "--api-key", default=None, help="可选：API Key（默认读 AI_API_KEY）"
    )
    parser.add_argument(
        "--api-mode",
        default=env_config.api_mode,
        choices=[AI_MODE_COMPLETION, AI_MODE_RESPONSES],
    )
    parser.add_argument(
        "--ai-stream", action="store_true", help="可选：打开 stream（默认按环境变量）"
    )
    parser.add_argument(
        "--prompt-version",
        default=os.getenv(ENV_AI_PROMPT_VERSION, DEFAULT_PROMPT_VERSION),
    )
    parser.add_argument(
        "--ai-retries",
        type=int,
        default=int(env_config.retries),
    )
    parser.add_argument(
        "--ai-temperature",
        type=float,
        default=float(env_config.temperature),
    )
    parser.add_argument(
        "--ai-reasoning-effort",
        default=env_config.reasoning_effort,
        choices=AI_REASONING_EFFORT_CHOICES,
    )
    parser.add_argument(
        "--ai-rpm",
        type=float,
        default=float(env_config.ai_rpm),
        help="每分钟最多多少次（0=不等）",
    )
    parser.add_argument(
        "--ai-timeout-sec",
        type=float,
        default=float(env_config.timeout_seconds),
    )
    parser.add_argument("--trace-out", type=Path, default=None)
    parser.add_argument(
        "--relevant-threshold", type=float, default=DEFAULT_RELEVANT_THRESHOLD
    )
    add_log_level_argument(parser)
    return parser.parse_args()


def _parse_post_uids(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    items: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[,\n]+", raw):
        uid = str(part or "").strip()
        if not uid:
            continue
        if uid.isdigit():
            uid = f"weibo:{uid}"
        if uid in seen:
            continue
        seen.add(uid)
        items.append(uid)
    return items


def _build_config(args: argparse.Namespace) -> LLMConfig:
    env_config = ai_task_runtime_config_from_env(
        task_key=AI_TASK_POST_ANALYSIS,
        timeout_seconds_default=1000.0,
    )
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

    runtime_config = apply_ai_runtime_config_overrides(
        env_config,
        api_key=args.api_key,
        model=args.model,
        base_url=args.base_url,
        api_mode=args.api_mode,
        temperature=args.ai_temperature,
        reasoning_effort=args.ai_reasoning_effort,
        timeout_seconds=args.ai_timeout_sec,
        retries=args.ai_retries,
        ai_rpm=args.ai_rpm,
    )
    api_key = str(runtime_config.api_key or "").strip()
    if not api_key:
        raise RuntimeError("Missing AI task runtime api_key.")

    return LLMConfig(
        api_key=api_key,
        model=str(runtime_config.model or ""),
        prompt_version=str(args.prompt_version or DEFAULT_PROMPT_VERSION),
        relevant_threshold=max(0.0, min(1.0, float(args.relevant_threshold))),
        base_url=str(runtime_config.base_url or ""),
        api_mode=str(runtime_config.api_mode or ""),
        ai_stream=ai_stream,
        ai_retries=max(0, int(runtime_config.retries)),
        ai_temperature=float(runtime_config.temperature),
        ai_reasoning_effort=str(runtime_config.reasoning_effort or ""),
        ai_rpm=max(0.0, float(runtime_config.ai_rpm)),
        ai_timeout_seconds=max(1.0, float(runtime_config.timeout_seconds)),
        trace_out=trace_out,
    )


def _source_engine_for_platform(platform: str) -> PostgresEngine:
    wanted = require_postgres_source_platform(platform)
    source = require_postgres_source_from_env(wanted)
    return ensure_postgres_engine(source.dsn, schema_name=source.schema)


def main() -> None:
    load_dotenv_if_present()
    args = parse_args()
    configure_logging(level=args.log_level)
    post_uids = _parse_post_uids(args.post_uids)
    if not post_uids:
        raise SystemExit("缺参数：请传 --post-uids")

    config = _build_config(args)
    limiter = RateLimiter(config.ai_rpm)
    engine_by_platform: dict[str, PostgresEngine] = {}

    ok = 0
    skipped = 0
    for post_uid in post_uids:
        platform = require_postgres_source_platform(post_uid)
        engine = engine_by_platform.get(platform)
        if engine is None:
            engine = _source_engine_for_platform(platform)
            engine_by_platform[platform] = engine
        logger.info(
            "[manual] run post_uid=%s prompt_version=%s",
            post_uid,
            config.prompt_version,
        )
        if process_one_post_uid(
            engine=engine, post_uid=post_uid, config=config, limiter=limiter
        ):
            ok += 1
            continue
        skipped += 1
        logger.warning(
            "[manual] skip post_uid=%s reason=process_failed",
            post_uid,
        )

    logger.info("[manual] done ok=%s skipped=%s", ok, skipped)


if __name__ == "__main__":
    main()
