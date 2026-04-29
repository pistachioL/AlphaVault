from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from alphavault.ai.analyze import AI_MODE_COMPLETION, AI_MODE_RESPONSES  # noqa: E402
from alphavault.domains.entity_match.alias_task_rules import (  # noqa: E402
    blocked_alias_task_reason,
)
from alphavault.env import load_dotenv_if_present  # noqa: E402
from alphavault.infra.ai.runtime_config import (  # noqa: E402
    AI_REASONING_EFFORT_CHOICES,
    AI_TASK_ALIAS_RESOLVE,
    AiRuntimeConfig,
    ai_task_runtime_config_from_env,
    apply_ai_runtime_config_overrides,
)
from alphavault.logging_config import (  # noqa: E402
    add_log_level_argument,
    configure_logging,
    get_logger,
)
from alphavault.research_workbench import (  # noqa: E402
    ALIAS_TASK_STATUS_BLOCKED,
    AliasAiRoundSummary,
    DEFAULT_ALIAS_AI_BATCH_SIZE,
    get_research_workbench_engine_from_env,
    list_pending_alias_resolve_tasks,
    run_alias_manual_pending_round,
    set_alias_resolve_task_status,
)

DEFAULT_MAX_ROUNDS = 0
logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env_config = ai_task_runtime_config_from_env(
        task_key=AI_TASK_ALIAS_RESOLVE,
        timeout_seconds_default=1000.0,
    )
    parser = argparse.ArgumentParser(
        description="循环处理 alias_manual 存量任务，默认复用库里已有 AI 结果并自动合并"
    )
    parser.add_argument(
        "--ai-batch-size",
        type=int,
        default=DEFAULT_ALIAS_AI_BATCH_SIZE,
        help=f"一次 AI 请求放多少条，默认 {DEFAULT_ALIAS_AI_BATCH_SIZE}",
    )
    parser.add_argument(
        "--model",
        default=env_config.model,
        help="默认读 alias_resolve 任务绑定的 AI 配置档",
    )
    parser.add_argument(
        "--base-url",
        default=env_config.base_url,
        help="默认读 alias_resolve 任务绑定的 AI 配置档",
    )
    parser.add_argument(
        "--api-key",
        default=env_config.api_key,
        help="默认读 alias_resolve 任务绑定的 AI 配置档",
    )
    parser.add_argument(
        "--api-mode",
        default=env_config.api_mode,
        choices=[AI_MODE_COMPLETION, AI_MODE_RESPONSES],
        help="默认读 alias_resolve 任务绑定的 AI 配置档",
    )
    parser.add_argument(
        "--ai-timeout-sec",
        type=float,
        default=env_config.timeout_seconds,
        help="默认读 alias_resolve 任务绑定的 AI 配置档",
    )
    parser.add_argument(
        "--ai-retries",
        type=int,
        default=env_config.retries,
        help="默认读 alias_resolve 任务绑定的 AI 配置档",
    )
    parser.add_argument(
        "--ai-temperature",
        type=float,
        default=env_config.temperature,
        help="默认读 alias_resolve 任务绑定的 AI 配置档",
    )
    parser.add_argument(
        "--ai-reasoning-effort",
        default=env_config.reasoning_effort,
        choices=AI_REASONING_EFFORT_CHOICES,
        help="默认读 alias_resolve 任务绑定的 AI 配置档",
    )
    parser.add_argument(
        "--ai-rpm",
        type=float,
        default=env_config.ai_rpm,
        help="每分钟最多发多少次 AI 请求；默认读默认限流组或 alias_resolve 绑定的限流组",
    )
    parser.add_argument(
        "--ai-max-inflight",
        type=int,
        default=env_config.ai_max_inflight,
        help="同时最多跑多少个 AI 请求；默认读默认限流组或 alias_resolve 绑定的限流组",
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
        default=False,
        help="是否真的重新调用 AI，默认 false；false 时复用库里已有 ai 字段",
    )
    parser.add_argument(
        "--apply",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否真的写回并自动合并，默认 true",
    )
    parser.add_argument(
        "--apply-block-rules",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否先用规则屏蔽明显非股票条目，默认 true",
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


def _dispose_engine_if_needed(engine: object) -> None:
    dispose = getattr(engine, "dispose", None)
    if callable(dispose):
        dispose()


def _normalize_max_rounds(value: object) -> int:
    return _normalize_non_negative_int(value, default=DEFAULT_MAX_ROUNDS)


def _build_ai_runtime_config(args: argparse.Namespace) -> AiRuntimeConfig:
    env_config = ai_task_runtime_config_from_env(
        task_key=AI_TASK_ALIAS_RESOLVE,
        timeout_seconds_default=1000.0,
    )
    return apply_ai_runtime_config_overrides(
        env_config,
        api_key=args.api_key,
        model=args.model,
        base_url=args.base_url,
        api_mode=args.api_mode or AI_MODE_RESPONSES,
        temperature=args.ai_temperature,
        reasoning_effort=args.ai_reasoning_effort,
        timeout_seconds=_normalize_positive_float(
            args.ai_timeout_sec,
            default=env_config.timeout_seconds,
        ),
        retries=_normalize_non_negative_int(
            args.ai_retries, default=env_config.retries
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
    if apply:
        return 0
    return 1


def _should_reuse_stored_ai(*, ai_enabled: bool) -> bool:
    return not ai_enabled


def _should_stop_on_no_progress(
    *,
    ai_enabled: bool,
    apply: bool,
    summary: AliasAiRoundSummary,
) -> bool:
    if ai_enabled or not apply:
        return False
    return int(summary["auto_confirmed"]) <= 0


def _block_pending_alias_tasks_by_rule(
    *,
    engine,
    limit: int,
) -> int:
    blocked_count = 0
    for row in list_pending_alias_resolve_tasks(engine, limit=limit):
        alias_key = _clean_text(row.get("alias_key"))
        reason = blocked_alias_task_reason(alias_key)
        if not alias_key or not reason:
            continue
        set_alias_resolve_task_status(
            engine,
            alias_key=alias_key,
            status=ALIAS_TASK_STATUS_BLOCKED,
            attempt_count=int(str(row.get("attempt_count") or "0") or 0),
            sample_post_uid=_clean_text(row.get("sample_post_uid")),
            sample_evidence=_clean_text(row.get("sample_evidence")),
            sample_raw_text_excerpt=_clean_text(row.get("sample_raw_text_excerpt")),
            ai_status=_clean_text(row.get("ai_status")),
            ai_stock_code=_clean_text(row.get("ai_stock_code")),
            ai_official_name=_clean_text(row.get("ai_official_name")),
            ai_confidence=_clean_text(row.get("ai_confidence")),
            ai_reason=reason,
            ai_uncertain=_clean_text(row.get("ai_uncertain")),
            ai_validation_status=_clean_text(row.get("ai_validation_status")),
        )
        blocked_count += 1
    return blocked_count


def run_alias_manual_ai(
    *,
    ai_batch_size: int,
    ai_enabled: bool,
    apply: bool,
    apply_block_rules: bool,
    max_rounds: int,
    runtime_config: AiRuntimeConfig,
) -> dict[str, int]:
    engine = get_research_workbench_engine_from_env()
    try:
        reuse_stored_ai = _should_reuse_stored_ai(ai_enabled=ai_enabled)
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
        total_rule_blocked = 0
        rounds = 0
        while True:
            round_fetch_limit = max(
                1,
                resolved_ai_batch_size * max(1, int(runtime_config.ai_max_inflight)),
            )
            rule_blocked = 0
            if apply and apply_block_rules:
                rule_blocked = _block_pending_alias_tasks_by_rule(
                    engine=engine,
                    limit=round_fetch_limit,
                )
                total_rule_blocked += rule_blocked
            summary = run_alias_manual_pending_round(
                engine,
                ai_enabled=ai_enabled,
                apply=apply,
                ai_batch_size=resolved_ai_batch_size,
                runtime_config=runtime_config,
                reuse_stored_ai=reuse_stored_ai,
            )
            if int(summary["fetched"]) <= 0:
                logger.info(
                    "[alias_manual_ai] complete rounds=%s total_fetched=%s "
                    "total_auto_confirmed=%s total_rule_blocked=%s",
                    rounds,
                    total_fetched,
                    total_auto_confirmed,
                    total_rule_blocked,
                )
                return {
                    "rounds": rounds,
                    "total_fetched": total_fetched,
                    "total_auto_confirmed": total_auto_confirmed,
                    "total_rule_blocked": total_rule_blocked,
                }
            rounds += 1
            total_fetched += int(summary["fetched"])
            total_auto_confirmed += int(summary["auto_confirmed"])
            logger.info(
                "[alias_manual_ai] round=%s fetched=%s enriched=%s "
                "auto_confirmed=%s rule_blocked=%s remaining_sample=%s apply=%s ai_enabled=%s",
                rounds,
                int(summary["fetched"]),
                int(summary["enriched"]),
                int(summary["auto_confirmed"]),
                rule_blocked,
                int(summary["remaining_sample"]),
                int(bool(apply)),
                int(bool(ai_enabled)),
            )
            if _should_stop_on_no_progress(
                ai_enabled=ai_enabled,
                apply=apply,
                summary=summary,
            ):
                logger.info(
                    "[alias_manual_ai] stop_on_no_progress rounds=%s fetched=%s "
                    "remaining_sample=%s reuse_stored_ai=%s",
                    rounds,
                    int(summary["fetched"]),
                    int(summary["remaining_sample"]),
                    int(reuse_stored_ai),
                )
                return {
                    "rounds": rounds,
                    "total_fetched": total_fetched,
                    "total_auto_confirmed": total_auto_confirmed,
                    "total_rule_blocked": total_rule_blocked,
                }
            if resolved_max_rounds > 0 and rounds >= resolved_max_rounds:
                logger.info(
                    "[alias_manual_ai] stop_on_max_rounds rounds=%s total_fetched=%s "
                    "total_auto_confirmed=%s total_rule_blocked=%s",
                    rounds,
                    total_fetched,
                    total_auto_confirmed,
                    total_rule_blocked,
                )
                return {
                    "rounds": rounds,
                    "total_fetched": total_fetched,
                    "total_auto_confirmed": total_auto_confirmed,
                    "total_rule_blocked": total_rule_blocked,
                }
    finally:
        _dispose_engine_if_needed(engine)


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_present()
    args = parse_args(argv)
    configure_logging(level=getattr(args, "log_level", ""))
    ai_batch_size = _normalize_positive_int(
        args.ai_batch_size,
        default=DEFAULT_ALIAS_AI_BATCH_SIZE,
    )
    ai_enabled = bool(args.ai_enabled)
    apply = bool(args.apply)
    apply_block_rules = bool(args.apply_block_rules)
    max_rounds = _normalize_max_rounds(args.max_rounds)
    runtime_config = _build_ai_runtime_config(args)
    logger.info(
        "[alias_manual_ai] start ai_batch_size=%s ai_rpm=%s "
        "ai_max_inflight=%s max_rounds=%s apply=%s apply_block_rules=%s ai_enabled=%s reuse_stored_ai=%s "
        "model=%s api_mode=%s",
        ai_batch_size,
        runtime_config.ai_rpm,
        runtime_config.ai_max_inflight,
        max_rounds,
        int(apply),
        int(apply_block_rules),
        int(ai_enabled),
        int(_should_reuse_stored_ai(ai_enabled=ai_enabled)),
        runtime_config.model,
        runtime_config.api_mode,
    )
    run_alias_manual_ai(
        ai_batch_size=ai_batch_size,
        ai_enabled=ai_enabled,
        apply=apply,
        apply_block_rules=apply_block_rules,
        max_rounds=max_rounds,
        runtime_config=runtime_config,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
