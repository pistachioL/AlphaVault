from __future__ import annotations

import argparse
import os
from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from alphavault.ai.analyze import DEFAULT_PROMPT_VERSION
from alphavault.constants import (
    ENV_AI_STREAM,
    ENV_AI_TRACE_OUT,
)
from alphavault.db.postgres_db import PostgresEngine
from alphavault.infra.ai.runtime_config import (
    AI_TASK_POST_ANALYSIS,
    ai_task_runtime_config_from_env,
    apply_ai_runtime_config_overrides,
)
from alphavault.logging_config import get_logger
from alphavault.rss.utils import env_bool

logger = get_logger(__name__)


@dataclass(frozen=True)
class WorkerSourceConfig:
    name: str
    platform: str
    rss_urls: list[str]
    author: str
    user_id: Optional[str]
    database_url: str
    schema_name: str


@dataclass
class WorkerSourceRuntime:
    config: WorkerSourceConfig
    engine: PostgresEngine
    spool_dir: Path
    redis_queue_key: str
    rss_next_ingest_at: float
    redis_due_maintenance_next_at: float = 0.0
    redis_due_maintenance_empty_checks: int = 0
    rss_ingest_future: Future | None = None
    source_db_ready: bool = False
    source_db_next_ready_check_at: float = 0.0
    stock_hot_cache_future: Future | None = None
    stock_hot_cache_next_at: float = 0.0
    progress_state_cache: dict[str, dict] = field(default_factory=dict)


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
    trace_out: Optional[Path] = None
    ai_max_inflight: int = 1


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

    base_url = str(args.base_url or "").strip()
    if base_url and not base_url.rstrip("/").endswith("/v1"):
        logger.warning("[ai] warn base_url_maybe_missing_v1 base_url=%s", base_url)

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
        ai_max_inflight=getattr(args, "ai_max_inflight", env_config.ai_max_inflight),
    )
    api_key = str(runtime_config.api_key or "").strip()
    if not api_key:
        raise RuntimeError("Missing AI task runtime api_key.")

    return LLMConfig(
        api_key=api_key,
        model=str(runtime_config.model or ""),
        prompt_version=str(args.prompt_version or DEFAULT_PROMPT_VERSION),
        relevant_threshold=max(0.0, min(1.0, float(args.relevant_threshold))),
        base_url=str(base_url or ""),
        api_mode=str(runtime_config.api_mode or ""),
        ai_stream=ai_stream,
        ai_retries=max(0, int(runtime_config.retries)),
        ai_temperature=float(runtime_config.temperature),
        ai_reasoning_effort=str(runtime_config.reasoning_effort or ""),
        ai_rpm=max(0.0, float(runtime_config.ai_rpm)),
        ai_timeout_seconds=max(1.0, float(runtime_config.timeout_seconds)),
        ai_max_inflight=max(1, int(runtime_config.ai_max_inflight)),
        trace_out=trace_out,
    )


__all__ = [
    "LLMConfig",
    "WorkerSourceConfig",
    "WorkerSourceRuntime",
    "_build_config",
    "_clamp_float",
    "_clamp_int",
    "_parse_int_or_default",
]
