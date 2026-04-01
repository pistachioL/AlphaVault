from __future__ import annotations

import argparse
import os
import threading
from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from alphavault.ai.analyze import (
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_MODEL,
    DEFAULT_PROMPT_VERSION,
)
from alphavault.db.turso_db import TursoEngine
from alphavault.constants import (
    ENV_AI_API_KEY,
    ENV_AI_STREAM,
    ENV_AI_TRACE_OUT,
)
from alphavault.rss.utils import env_bool


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
    engine: TursoEngine
    spool_dir: Path
    redis_queue_key: str
    rss_next_ingest_at: float
    redis_due_maintenance_next_at: float = 0.0
    redis_due_maintenance_empty_checks: int = 0
    maintenance_recovery_cycle_count: int = 0
    maintenance_recovery_force_next: bool = False
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
    local_cache_ready: bool = False
    local_cache_rebuild_next_at: float = 0.0
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
    trace_out: Optional[Path]
    verbose: bool


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


__all__ = [
    "LLMConfig",
    "WorkerSourceConfig",
    "WorkerSourceRuntime",
    "_build_config",
    "_clamp_float",
    "_clamp_int",
    "_parse_int_or_default",
]
