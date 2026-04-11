from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from alphavault.logging_config import add_log_level_argument, get_logger
from alphavault.ai.analyze import (
    AI_MODE_COMPLETION,
    AI_MODE_RESPONSES,
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_AI_RETRY_COUNT,
    DEFAULT_AI_TEMPERATURE,
    DEFAULT_MODEL,
    DEFAULT_PROMPT_VERSION,
)
from alphavault.constants import (
    DEFAULT_RSS_RETRIES,
    DEFAULT_RSS_TIMEOUT_SECONDS,
    ENV_AI_API_MODE,
    ENV_AI_BASE_URL,
    ENV_AI_MAX_INFLIGHT,
    ENV_AI_MODEL,
    ENV_AI_PROMPT_VERSION,
    ENV_AI_QUEUE_ACK_TIMEOUT_SEC,
    ENV_AI_REASONING_EFFORT,
    ENV_AI_RETRIES,
    ENV_AI_RPM,
    ENV_AI_TEMPERATURE,
    ENV_AI_TIMEOUT_SEC,
    ENV_POSTGRES_DSN,
    ENV_RSS_RETRIES,
    ENV_RSS_TIMEOUT_SECONDS,
    ENV_WEIBO_AUTHOR,
    ENV_WEIBO_RSS_URL,
    ENV_WEIBO_RSS_URLS,
    ENV_WEIBO_USER_ID,
    ENV_WORKER_ACTIVE_HOURS,
    ENV_WORKER_INTERVAL_SECONDS,
    ENV_XUEQIU_AUTHOR,
    ENV_XUEQIU_RSS_URL,
    ENV_XUEQIU_RSS_URLS,
    ENV_XUEQIU_USER_ID,
)
from alphavault.rss.utils import env_float, env_int, parse_active_hours

logger = get_logger(__name__)


@dataclass(frozen=True)
class RSSSourceConfig:
    name: str
    platform: str
    rss_urls: list[str]
    database_url: str
    author: str
    user_id: Optional[str]


def _split_rss_urls_value(value: str) -> list[str]:
    out: list[str] = []
    for item in re.split(r"[,\n]+", str(value or "")):
        url = item.strip()
        if url:
            out.append(url)
    return out


def _dedup_urls(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _env_text(name: str) -> str:
    return os.getenv(name, "").strip()


def _build_platform_source_config(
    *,
    platform: str,
    name: str,
    rss_urls_env: str,
    rss_url_env: str,
    postgres_dsn: str,
    author_env: str,
    user_id_env: str,
) -> Optional[RSSSourceConfig]:
    urls = _dedup_urls(
        _split_rss_urls_value(_env_text(rss_urls_env))
        + _split_rss_urls_value(_env_text(rss_url_env))
    )
    author = _env_text(author_env)
    user_id = _env_text(user_id_env) or None

    if not urls and not postgres_dsn:
        return None
    if not postgres_dsn:
        raise RuntimeError(f"Missing {ENV_POSTGRES_DSN} for {platform}")

    return RSSSourceConfig(
        name=name,
        platform=platform,
        rss_urls=urls,
        database_url=postgres_dsn,
        author=author,
        user_id=user_id,
    )


def parse_args() -> argparse.Namespace:
    ai_retries_env = env_int(ENV_AI_RETRIES)
    ai_rpm_env = env_float(ENV_AI_RPM)
    ai_timeout_env = env_float(ENV_AI_TIMEOUT_SEC)
    ai_max_inflight_env = env_int(ENV_AI_MAX_INFLIGHT)
    ai_queue_ack_timeout_env = env_int(ENV_AI_QUEUE_ACK_TIMEOUT_SEC)
    rss_timeout_env = env_float(ENV_RSS_TIMEOUT_SECONDS)
    rss_retries_env = env_int(ENV_RSS_RETRIES)

    parser = argparse.ArgumentParser(description="RSS -> Redis -> AI -> Postgres")
    parser.add_argument(
        "--active-hours",
        default="",
        help="worker 只在这些小时做事（CST），格式: 6-22；为空=全天",
    )
    parser.add_argument(
        "--limit", type=int, default=0, help="最多处理多少条（0 表示不限）"
    )
    parser.add_argument(
        "--rss-timeout",
        type=float,
        default=(
            float(rss_timeout_env)
            if rss_timeout_env is not None
            else float(DEFAULT_RSS_TIMEOUT_SECONDS)
        ),
        help="RSS HTTP 超时秒数",
    )
    parser.add_argument(
        "--rss-retries",
        type=int,
        default=(
            int(rss_retries_env)
            if rss_retries_env is not None
            else int(DEFAULT_RSS_RETRIES)
        ),
        help="RSS 失败后最多重试次数（不含首次）",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=0.0,
        help="worker 维护间隔秒数（0=读 WORKER_INTERVAL_SECONDS 或默认 600）",
    )
    parser.add_argument(
        "--ai-stuck-seconds",
        type=int,
        default=(
            int(ai_queue_ack_timeout_env)
            if ai_queue_ack_timeout_env is not None
            else 3600
        ),
        help="队列任务拿走后，多久没完成就算卡住（秒，可用 AI_QUEUE_ACK_TIMEOUT_SEC）",
    )

    # AI config (litellm only; mostly via env)
    parser.add_argument("--model", default=os.getenv(ENV_AI_MODEL, DEFAULT_MODEL))
    parser.add_argument(
        "--base-url",
        default=os.getenv(ENV_AI_BASE_URL, "").strip(),
        help="可选：OpenAI 兼容接口 base_url（也可用 AI_BASE_URL）",
    )
    parser.add_argument(
        "--api-key", default=None, help="可选：API Key（默认读 AI_API_KEY）"
    )
    parser.add_argument(
        "--api-mode",
        default=os.getenv(ENV_AI_API_MODE, DEFAULT_AI_MODE),
        choices=[AI_MODE_COMPLETION, AI_MODE_RESPONSES],
    )
    parser.add_argument("--ai-stream", action="store_true")
    parser.add_argument(
        "--prompt-version",
        default=os.getenv(ENV_AI_PROMPT_VERSION, DEFAULT_PROMPT_VERSION),
    )
    parser.add_argument(
        "--ai-retries",
        type=int,
        default=ai_retries_env
        if ai_retries_env is not None
        else DEFAULT_AI_RETRY_COUNT,
    )
    parser.add_argument(
        "--ai-temperature",
        type=float,
        default=float(os.getenv(ENV_AI_TEMPERATURE, str(DEFAULT_AI_TEMPERATURE))),
    )
    parser.add_argument(
        "--ai-reasoning-effort",
        default=os.getenv(ENV_AI_REASONING_EFFORT, DEFAULT_AI_REASONING_EFFORT),
        choices=["none", "minimal", "low", "medium", "high", "xhigh"],
    )
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
    parser.add_argument(
        "--relevant-threshold", type=float, default=0.35, help="相关度阈值"
    )
    add_log_level_argument(parser)
    args = parser.parse_args()
    args.rss_timeout = max(1.0, float(args.rss_timeout))
    args.rss_retries = max(0, int(args.rss_retries))
    return args


def resolve_rss_source_configs(args: argparse.Namespace) -> list[RSSSourceConfig]:
    configs: list[RSSSourceConfig] = []
    postgres_dsn = _env_text(ENV_POSTGRES_DSN)
    for cfg in (
        _build_platform_source_config(
            platform="weibo",
            name="weibo",
            rss_urls_env=ENV_WEIBO_RSS_URLS,
            rss_url_env=ENV_WEIBO_RSS_URL,
            postgres_dsn=postgres_dsn,
            author_env=ENV_WEIBO_AUTHOR,
            user_id_env=ENV_WEIBO_USER_ID,
        ),
        _build_platform_source_config(
            platform="xueqiu",
            name="xueqiu",
            rss_urls_env=ENV_XUEQIU_RSS_URLS,
            rss_url_env=ENV_XUEQIU_RSS_URL,
            postgres_dsn=postgres_dsn,
            author_env=ENV_XUEQIU_AUTHOR,
            user_id_env=ENV_XUEQIU_USER_ID,
        ),
    ):
        if cfg is not None:
            configs.append(cfg)

    if configs:
        if str(getattr(args, "log_level", "") or "").strip().lower() == "debug":
            for cfg in configs:
                logger.debug(
                    "[rss] source=%s platform=%s urls=%s",
                    cfg.name,
                    cfg.platform,
                    len(cfg.rss_urls),
                )
        return configs

    raise RuntimeError(f"missing {ENV_POSTGRES_DSN}")


def _parse_worker_active_hours_from_args(
    args: argparse.Namespace,
) -> Optional[tuple[int, int]]:
    active_hours_value = (
        args.active_hours.strip()
        if args.active_hours
        else os.getenv(ENV_WORKER_ACTIVE_HOURS, "").strip()
    )
    if not active_hours_value:
        return None
    return parse_active_hours(active_hours_value)


def _resolve_worker_interval_seconds(args: argparse.Namespace) -> float:
    interval = float(args.interval_seconds or 0.0)
    if interval <= 0:
        interval = float(os.getenv(ENV_WORKER_INTERVAL_SECONDS, "600") or "600")
    return max(1.0, interval)
