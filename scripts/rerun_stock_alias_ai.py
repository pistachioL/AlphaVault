from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from alphavault.env import load_dotenv_if_present  # noqa: E402
from alphavault.infra.ai import relation_candidate_ranker  # noqa: E402
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

DEFAULT_LIMIT = int(relation_candidate_ranker.AI_RANK_BATCH_CAP)
DEFAULT_MAX_ROUNDS = 0
logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="循环重跑 stock_alias 的 AI 排序和自动合并"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"每轮最多处理多少条，默认 {DEFAULT_LIMIT}",
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


def _normalize_limit(value: object) -> int:
    try:
        resolved = int(str(value or "").strip() or "0")
    except (TypeError, ValueError):
        resolved = 0
    return max(1, resolved)


def _normalize_max_rounds(value: object) -> int:
    try:
        resolved = int(str(value or "").strip() or "0")
    except (TypeError, ValueError):
        resolved = 0
    return max(0, resolved)


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
    limit: int,
    ai_enabled: bool,
    apply: bool,
) -> dict[str, int]:
    target_rows = _pending_stock_alias_rows(limit=limit)
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

    enriched_rows: list[dict[str, Any]] = []
    auto_accept_count = 0
    for start in range(
        0, len(target_rows), int(relation_candidate_ranker.AI_RANK_BATCH_CAP)
    ):
        batch_rows = target_rows[
            start : start + int(relation_candidate_ranker.AI_RANK_BATCH_CAP)
        ]
        enriched_batch = _enrich_batch_rows(
            rows=batch_rows,
            official_names_by_stock_key=official_names_by_stock_key,
            ai_enabled=ai_enabled,
        )
        enriched_rows.extend(enriched_batch)
        for row in enriched_batch:
            _candidate_id, accepted = _persist_enriched_row(row=dict(row), apply=apply)
            if accepted:
                auto_accept_count += 1

    remaining_sample = len(_pending_stock_alias_rows(limit=limit))
    status_counts = _status_counts(enriched_rows)
    logger.info(
        "[stock_alias_ai] round fetched=%s enriched=%s auto_accepted=%s apply=%s ai_enabled=%s statuses=%s remaining_sample=%s",
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

    limit = _normalize_limit(args.limit)
    max_rounds = _normalize_max_rounds(args.max_rounds)
    ai_enabled = bool(args.ai_enabled)
    apply = bool(args.apply)

    logger.info(
        "[stock_alias_ai] start limit=%s max_rounds=%s apply=%s ai_enabled=%s batch_cap=%s",
        limit,
        max_rounds,
        int(apply),
        int(ai_enabled),
        int(relation_candidate_ranker.AI_RANK_BATCH_CAP),
    )

    round_no = 0
    total_fetched = 0
    total_auto_accepted = 0
    while True:
        if max_rounds > 0 and round_no >= max_rounds:
            logger.info(
                "[stock_alias_ai] stop reason=max_rounds round_no=%s total_fetched=%s total_auto_accepted=%s",
                round_no,
                total_fetched,
                total_auto_accepted,
            )
            return 0

        round_no += 1
        summary = _run_one_round(limit=limit, ai_enabled=ai_enabled, apply=apply)
        fetched = int(summary["fetched"])
        total_fetched += fetched
        total_auto_accepted += int(summary["auto_accepted"])
        if fetched <= 0:
            logger.info(
                "[stock_alias_ai] stop reason=empty round_no=%s total_fetched=%s total_auto_accepted=%s",
                round_no,
                total_fetched,
                total_auto_accepted,
            )
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
