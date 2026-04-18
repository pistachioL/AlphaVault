from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
import sys
from typing import Any, Callable

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from alphavault.ai.analyze import AI_MODE_COMPLETION, AI_MODE_RESPONSES  # noqa: E402
from alphavault.env import load_dotenv_if_present  # noqa: E402
from alphavault.infra.ai import relation_candidate_ranker  # noqa: E402
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
    RELATION_TYPE_STOCK_ALIAS,
    auto_accept_relation_candidate_if_needed,
    get_official_names_by_stock_keys,
    get_research_workbench_engine_from_env,
    list_pending_candidates,
    should_auto_accept_relation_candidate_row,
    upsert_relation_candidate,
)
from alphavault.rss.utils import RateLimiter  # noqa: E402

DEFAULT_AI_BATCH_SIZE = int(relation_candidate_ranker.AI_RANK_BATCH_CAP)
DEFAULT_MAX_ROUNDS = 0
AI_REASONING_EFFORT_CHOICES = ["none", "minimal", "low", "medium", "high", "xhigh"]
logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env_config = ai_runtime_config_from_env(timeout_seconds_default=1000.0)
    parser = argparse.ArgumentParser(
        description="循环重跑 stock_alias 的 AI 排序和自动合并"
    )
    parser.add_argument(
        "--ai-batch-size",
        type=int,
        default=DEFAULT_AI_BATCH_SIZE,
        help=f"一次 AI 请求放多少条，默认 {DEFAULT_AI_BATCH_SIZE}",
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


def _clean_text(value: object) -> str:
    return str(value or "").strip()


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


def _chunk_rows(
    rows: list[dict[str, Any]],
    *,
    chunk_size: int,
) -> list[list[dict[str, Any]]]:
    resolved_chunk_size = max(1, int(chunk_size))
    return [
        rows[start : start + resolved_chunk_size]
        for start in range(0, len(rows), resolved_chunk_size)
    ]


def _round_fetch_limit(*, ai_batch_size: int, ai_max_inflight: int) -> int:
    return max(1, int(ai_batch_size) * max(1, int(ai_max_inflight)))


def _pending_stock_alias_rows(*, limit: int) -> list[dict[str, Any]]:
    engine = get_research_workbench_engine_from_env()
    pending_rows = list_pending_candidates(engine)
    out: list[dict[str, Any]] = []
    for row in pending_rows:
        if _clean_text(row.get("relation_type")) != RELATION_TYPE_STOCK_ALIAS:
            continue
        candidate_id = _clean_text(row.get("candidate_id"))
        if not candidate_id:
            continue
        out.append(dict(row))
        if len(out) >= limit:
            break
    return out


def _enrich_batch_rows(
    *,
    rows: list[dict[str, Any]],
    official_names_by_stock_key: dict[str, str],
    ai_enabled: bool,
    runtime_config: AiRuntimeConfig,
    request_gate: Callable[[], None] | None = None,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    return relation_candidate_ranker.enrich_candidates_with_ai(
        [
            {
                **dict(row),
                "left_official_name": official_names_by_stock_key.get(
                    _clean_text(row.get("left_key")),
                    "",
                ),
            }
            for row in rows
        ],
        relation_type=RELATION_TYPE_STOCK_ALIAS,
        ai_enabled=ai_enabled,
        runtime_config=runtime_config,
        request_gate=request_gate,
        stock_alias_batch_size=len(rows),
    )


def _persist_enriched_row(
    *,
    row: dict[str, Any],
    apply: bool,
) -> tuple[str, bool]:
    if not apply:
        return _clean_text(
            row.get("candidate_id")
        ), should_auto_accept_relation_candidate_row(row)
    engine = get_research_workbench_engine_from_env()
    persisted_candidate = upsert_relation_candidate(
        engine,
        candidate_id=_clean_text(row.get("candidate_id")),
        relation_type=_clean_text(row.get("relation_type")),
        left_key=_clean_text(row.get("left_key")),
        right_key=_clean_text(row.get("right_key")),
        relation_label=_clean_text(row.get("relation_label")),
        suggestion_reason=_clean_text(row.get("suggestion_reason")),
        evidence_summary=_clean_text(row.get("evidence_summary")),
        score=float(str(row.get("score") or "0") or 0),
        ai_status=_clean_text(row.get("ai_status")),
        ai_reason=_clean_text(row.get("ai_reason")),
        ai_confidence=_clean_text(row.get("ai_confidence")),
        sample_post_uid=_clean_text(row.get("sample_post_uid")),
        sample_evidence=_clean_text(row.get("sample_evidence")),
        sample_raw_text_excerpt=_clean_text(row.get("sample_raw_text_excerpt")),
    )
    accepted = auto_accept_relation_candidate_if_needed(
        engine,
        candidate_row={**dict(row), **persisted_candidate},
    )
    return _clean_text(persisted_candidate.get("candidate_id")), bool(accepted)


def _status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = _clean_text(row.get("ai_status")) or "(empty)"
        counts[status] = int(counts.get(status, 0)) + 1
    return counts


def _run_one_round(
    *,
    ai_batch_size: int,
    ai_enabled: bool,
    apply: bool,
    runtime_config: AiRuntimeConfig,
) -> dict[str, int]:
    fetch_limit = _round_fetch_limit(
        ai_batch_size=ai_batch_size,
        ai_max_inflight=runtime_config.ai_max_inflight,
    )
    target_rows = _pending_stock_alias_rows(limit=fetch_limit)
    if not target_rows:
        return {
            "fetched": 0,
            "enriched": 0,
            "auto_accepted": 0,
            "remaining_sample": 0,
        }

    engine = get_research_workbench_engine_from_env()
    official_names_by_stock_key = get_official_names_by_stock_keys(
        engine,
        [
            _clean_text(row.get("left_key"))
            for row in target_rows
            if _clean_text(row.get("left_key"))
        ],
    )

    limiter = RateLimiter(float(runtime_config.ai_rpm))
    batch_rows_list = _chunk_rows(target_rows, chunk_size=ai_batch_size)
    futures: list[Future[list[dict[str, Any]]]] = []
    enriched_rows: list[dict[str, Any]] = []
    auto_accept_count = 0
    with ThreadPoolExecutor(max_workers=runtime_config.ai_max_inflight) as executor:
        for batch_rows in batch_rows_list:
            futures.append(
                executor.submit(
                    _enrich_batch_rows,
                    rows=batch_rows,
                    official_names_by_stock_key=official_names_by_stock_key,
                    ai_enabled=ai_enabled,
                    runtime_config=runtime_config,
                    request_gate=limiter.wait,
                )
            )
        for future in futures:
            enriched_batch = future.result()
            enriched_rows.extend(enriched_batch)
            for row in enriched_batch:
                _candidate_id, accepted = _persist_enriched_row(
                    row=dict(row),
                    apply=apply,
                )
                if accepted:
                    auto_accept_count += 1

    remaining_sample = len(_pending_stock_alias_rows(limit=fetch_limit))
    status_counts = _status_counts(enriched_rows)
    logger.info(
        "[stock_alias_ai] round fetched=%s enriched=%s auto_accepted=%s "
        "apply=%s ai_enabled=%s statuses=%s remaining_sample=%s",
        len(target_rows),
        len(enriched_rows),
        auto_accept_count,
        int(bool(apply)),
        int(bool(ai_enabled)),
        status_counts,
        remaining_sample,
    )
    return {
        "fetched": len(target_rows),
        "enriched": len(enriched_rows),
        "auto_accepted": auto_accept_count,
        "remaining_sample": remaining_sample,
    }


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_present()
    args = parse_args(argv)
    configure_logging(level=args.log_level)

    env_config = ai_runtime_config_from_env(timeout_seconds_default=1000.0)
    ai_batch_size = _normalize_positive_int(
        args.ai_batch_size,
        default=DEFAULT_AI_BATCH_SIZE,
    )
    max_rounds = _normalize_max_rounds(args.max_rounds)
    ai_enabled = bool(args.ai_enabled)
    apply = bool(args.apply)
    runtime_config = _build_ai_runtime_config(args)

    logger.info(
        "[stock_alias_ai] start ai_batch_size=%s ai_rpm=%s "
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

    round_no = 0
    total_fetched = 0
    total_auto_accepted = 0
    while True:
        if max_rounds > 0 and round_no >= max_rounds:
            logger.info(
                "[stock_alias_ai] stop reason=max_rounds round_no=%s "
                "total_fetched=%s total_auto_accepted=%s",
                round_no,
                total_fetched,
                total_auto_accepted,
            )
            return 0

        round_no += 1
        summary = _run_one_round(
            ai_batch_size=ai_batch_size,
            ai_enabled=ai_enabled,
            apply=apply,
            runtime_config=runtime_config,
        )
        fetched = int(summary["fetched"])
        total_fetched += fetched
        total_auto_accepted += int(summary["auto_accepted"])
        if fetched <= 0:
            logger.info(
                "[stock_alias_ai] stop reason=empty round_no=%s "
                "total_fetched=%s total_auto_accepted=%s",
                round_no,
                total_fetched,
                total_auto_accepted,
            )
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
