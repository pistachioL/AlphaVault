from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from alphavault.ai.analyze import AI_MODE_COMPLETION, AI_MODE_RESPONSES  # noqa: E402
from alphavault.env import load_dotenv_if_present  # noqa: E402
from alphavault.infra.ai.runtime_config import (  # noqa: E402
    AiRuntimeConfig,
    ai_runtime_config_from_env,
)
from alphavault.logging_config import (  # noqa: E402
    add_log_level_argument,
    configure_logging,
    get_logger,
)
from alphavault.research_workbench import (  # noqa: E402
    DEFAULT_ALIAS_AI_BATCH_SIZE,
    get_research_workbench_engine_from_env,
    run_alias_manual_pending_round,
)

DEFAULT_MAX_ROUNDS = 0
AI_REASONING_EFFORT_CHOICES = ["none", "minimal", "low", "medium", "high", "xhigh"]
logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env_config = ai_runtime_config_from_env(timeout_seconds_default=1000.0)
    parser = argparse.ArgumentParser(
        description="循环重跑 alias_manual 的 AI 预判和自动合并"
    )
    parser.add_argument(
        "--ai-batch-size",
        type=int,
        default=DEFAULT_ALIAS_AI_BATCH_SIZE,
        help=f"一次 AI 请求放多少条，默认 {DEFAULT_ALIAS_AI_BATCH_SIZE}",
    )
    parser.add_argument("--model", default=env_config.model, help="默认读 AI_MODEL")
    parser.add_argument(
        "--base-url",
        default=env_config.base_url,
        help="默认读 AI_BASE_URL",
    )
    parser.add_argument(
        "--api-key", default=env_config.api_key, help="默认读 AI_API_KEY"
    )
    parser.add_argument(
        "--api-mode",
        default=env_config.api_mode,
        choices=[AI_MODE_COMPLETION, AI_MODE_RESPONSES],
        help="默认读 AI_API_MODE",
    )
    parser.add_argument(
        "--ai-timeout-sec",
        type=float,
        default=env_config.timeout_seconds,
        help="默认读 AI_TIMEOUT_SEC",
    )
    parser.add_argument(
        "--ai-retries",
        type=int,
        default=env_config.retries,
        help="默认读 AI_RETRIES",
    )
    parser.add_argument(
        "--ai-temperature",
        type=float,
        default=env_config.temperature,
        help="默认读 AI_TEMPERATURE",
    )
    parser.add_argument(
        "--ai-reasoning-effort",
        default=env_config.reasoning_effort,
        choices=AI_REASONING_EFFORT_CHOICES,
        help="默认读 AI_REASONING_EFFORT",
    )
    parser.add_argument(
        "--ai-rpm",
        type=float,
        default=env_config.ai_rpm,
        help="每分钟最多发多少次 AI 请求；默认读 AI_RPM",
    )
    parser.add_argument(
        "--ai-max-inflight",
        type=int,
        default=env_config.ai_max_inflight,
        help="同时最多跑多少个 AI 请求；默认读 AI_MAX_INFLIGHT",
    )
    parser.add_argument(
        "--max-rounds",
        type=int,
        default=DEFAULT_MAX_ROUNDS,
        help="最多跑几轮；0 表示一直跑到空",
    )
    parser.add_argument(
        "--ai-enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否真的调用 AI，默认 true",
    )
    parser.add_argument(
        "--apply",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否真的写回并自动合并，默认 true",
    )
    add_log_level_argument(parser)
    return parser.parse_args(argv)


def _normalize_positive_int(value: object, *, default: int) -> int:
    try:
        resolved = int(str(value or "").strip() or "0")
    except (TypeError, ValueError):
        resolved = 0
    if resolved > 0:
        return resolved
    return max(1, int(default))


def _normalize_non_negative_int(value: object, *, default: int) -> int:
    try:
        resolved = int(str(value or "").strip() or "0")
    except (TypeError, ValueError):
        resolved = int(default)
    return max(0, resolved)


def _normalize_non_negative_float(value: object, *, default: float) -> float:
    try:
        resolved = float(str(value or "").strip() or "0")
    except (TypeError, ValueError):
        resolved = float(default)
    return max(0.0, resolved)


def _normalize_positive_float(value: object, *, default: float) -> float:
    try:
        resolved = float(str(value or "").strip() or "0")
    except (TypeError, ValueError):
        resolved = float(default)
    if resolved > 0:
        return resolved
    return max(1.0, float(default))


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_max_rounds(value: object) -> int:
    return _normalize_non_negative_int(value, default=DEFAULT_MAX_ROUNDS)


def _build_ai_runtime_config(args: argparse.Namespace) -> AiRuntimeConfig:
    env_config = ai_runtime_config_from_env(timeout_seconds_default=1000.0)
    return AiRuntimeConfig(
        api_key=_clean_text(args.api_key),
        model=_clean_text(args.model) or env_config.model,
        base_url=_clean_text(args.base_url),
        api_mode=_clean_text(args.api_mode) or AI_MODE_RESPONSES,
        temperature=float(args.ai_temperature),
        reasoning_effort=_clean_text(args.ai_reasoning_effort)
        or env_config.reasoning_effort,
        timeout_seconds=_normalize_positive_float(
            args.ai_timeout_sec,
            default=env_config.timeout_seconds,
        ),
        retries=_normalize_non_negative_int(
            args.ai_retries,
            default=env_config.retries,
        ),
        ai_rpm=_normalize_non_negative_float(args.ai_rpm, default=env_config.ai_rpm),
        ai_max_inflight=_normalize_positive_int(
            args.ai_max_inflight,
            default=env_config.ai_max_inflight,
        ),
    )


def _effective_max_rounds(*, ai_enabled: bool, apply: bool, max_rounds: int) -> int:
    resolved_max_rounds = _normalize_max_rounds(max_rounds)
    if resolved_max_rounds > 0:
        return resolved_max_rounds
    if ai_enabled and apply:
        return 0
    return 1


def run_alias_manual_ai(
    *,
    ai_batch_size: int,
    ai_enabled: bool,
    apply: bool,
    max_rounds: int,
    runtime_config: AiRuntimeConfig,
) -> dict[str, int]:
    engine = get_research_workbench_engine_from_env()
    resolved_ai_batch_size = _normalize_positive_int(
        ai_batch_size,
        default=DEFAULT_ALIAS_AI_BATCH_SIZE,
    )
    resolved_max_rounds = _effective_max_rounds(
        ai_enabled=ai_enabled,
        apply=apply,
        max_rounds=max_rounds,
    )
    total_fetched = 0
    total_auto_confirmed = 0
    rounds = 0
    while True:
        summary = run_alias_manual_pending_round(
            engine,
            ai_enabled=ai_enabled,
            apply=apply,
            ai_batch_size=resolved_ai_batch_size,
            runtime_config=runtime_config,
        )
        if int(summary["fetched"]) <= 0:
            logger.info(
                "[alias_manual_ai] complete rounds=%s total_fetched=%s "
                "total_auto_confirmed=%s",
                rounds,
                total_fetched,
                total_auto_confirmed,
            )
            return {
                "rounds": rounds,
                "total_fetched": total_fetched,
                "total_auto_confirmed": total_auto_confirmed,
            }
        rounds += 1
        total_fetched += int(summary["fetched"])
        total_auto_confirmed += int(summary["auto_confirmed"])
        logger.info(
            "[alias_manual_ai] round=%s fetched=%s enriched=%s "
            "auto_confirmed=%s remaining_sample=%s apply=%s ai_enabled=%s",
            rounds,
            int(summary["fetched"]),
            int(summary["enriched"]),
            int(summary["auto_confirmed"]),
            int(summary["remaining_sample"]),
            int(bool(apply)),
            int(bool(ai_enabled)),
        )
        if resolved_max_rounds > 0 and rounds >= resolved_max_rounds:
            logger.info(
                "[alias_manual_ai] stop_on_max_rounds rounds=%s total_fetched=%s "
                "total_auto_confirmed=%s",
                rounds,
                total_fetched,
                total_auto_confirmed,
            )
            return {
                "rounds": rounds,
                "total_fetched": total_fetched,
                "total_auto_confirmed": total_auto_confirmed,
            }


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_present()
    args = parse_args(argv)
    configure_logging(level=getattr(args, "log_level", ""))
    env_config = ai_runtime_config_from_env(timeout_seconds_default=1000.0)
    ai_batch_size = _normalize_positive_int(
        args.ai_batch_size,
        default=DEFAULT_ALIAS_AI_BATCH_SIZE,
    )
    ai_enabled = bool(args.ai_enabled)
    apply = bool(args.apply)
    max_rounds = _normalize_max_rounds(args.max_rounds)
    runtime_config = _build_ai_runtime_config(args)
    logger.info(
        "[alias_manual_ai] start ai_batch_size=%s ai_rpm=%s "
        "ai_max_inflight=%s max_rounds=%s apply=%s ai_enabled=%s "
        "model=%s api_mode=%s",
        ai_batch_size,
        runtime_config.ai_rpm,
        runtime_config.ai_max_inflight,
        max_rounds,
        int(apply),
        int(ai_enabled),
        runtime_config.model or env_config.model,
        runtime_config.api_mode,
    )
    run_alias_manual_ai(
        ai_batch_size=ai_batch_size,
        ai_enabled=ai_enabled,
        apply=apply,
        max_rounds=max_rounds,
        runtime_config=runtime_config,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
