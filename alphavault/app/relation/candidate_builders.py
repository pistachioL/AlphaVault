from __future__ import annotations

from typing import Callable

from alphavault.domains.relation.ids import make_candidate_id
from alphavault.domains.relation.relation_candidates import (
    RELATION_LABEL_RELATED,
    build_sector_relation_candidates,
    build_stock_alias_candidates,
    build_stock_sector_candidates,
)
from alphavault.domains.stock.object_index import StockObjectIndex
from alphavault.infra.ai.relation_candidate_ranker import enrich_candidates_with_ai

RELATION_TYPE_STOCK_ALIAS = "stock_alias"
RELATION_TYPE_STOCK_SECTOR = "stock_sector"


def build_stock_pending_candidates(
    assertions: list[dict[str, object]],
    *,
    stock_key: str,
    ai_enabled: bool,
    relation_type: str = "",
    stock_index: StockObjectIndex | None = None,
    should_continue: Callable[[], bool] | None = None,
) -> list[dict[str, str]]:
    wanted_relation_type = str(relation_type or "").strip()
    include_alias = wanted_relation_type in {"", RELATION_TYPE_STOCK_ALIAS}
    include_sector = wanted_relation_type in {"", RELATION_TYPE_STOCK_SECTOR}
    alias_rows = (
        build_stock_alias_candidates(
            assertions,
            stock_key=stock_key,
            stock_index=stock_index,
        )
        if include_alias
        else []
    )
    sector_rows = (
        build_stock_sector_candidates(
            assertions,
            stock_key=stock_key,
            stock_index=stock_index,
        )
        if include_sector
        else []
    )

    candidates: list[dict[str, str]] = []
    for row in alias_rows:
        alias_key = str(row.get("alias_key") or "").strip()
        if not alias_key:
            continue
        candidates.append(
            {
                **row,
                "relation_type": RELATION_TYPE_STOCK_ALIAS,
                "left_key": stock_key,
                "right_key": alias_key,
                "relation_label": "alias_of",
                "candidate_id": make_candidate_id(
                    relation_type=RELATION_TYPE_STOCK_ALIAS,
                    left_key=stock_key,
                    right_key=alias_key,
                    relation_label="alias_of",
                ),
                "candidate_key": alias_key,
                "suggestion_reason": str(row.get("evidence_summary") or "").strip(),
            }
        )
    for row in sector_rows:
        sector_key = str(row.get("sector_key") or "").strip()
        if not sector_key:
            continue
        candidates.append(
            {
                **row,
                "relation_type": RELATION_TYPE_STOCK_SECTOR,
                "left_key": stock_key,
                "right_key": f"cluster:{sector_key}",
                "relation_label": "member_of",
                "candidate_id": make_candidate_id(
                    relation_type=RELATION_TYPE_STOCK_SECTOR,
                    left_key=stock_key,
                    right_key=f"cluster:{sector_key}",
                    relation_label="member_of",
                ),
                "candidate_key": sector_key,
                "suggestion_reason": str(row.get("evidence_summary") or "").strip(),
            }
        )
    if not ai_enabled:
        return _finalize_candidate_rows(candidates)
    return _finalize_candidate_rows(
        enrich_candidates_with_ai(
            candidates,
            relation_type=wanted_relation_type or RELATION_TYPE_STOCK_SECTOR,
            ai_enabled=ai_enabled,
            should_continue=should_continue,
        )
    )


def build_sector_pending_candidates(
    assertions: list[dict[str, object]],
    *,
    sector_key: str,
    ai_enabled: bool,
    should_continue: Callable[[], bool] | None = None,
) -> list[dict[str, str]]:
    candidates = build_sector_relation_candidates(assertions, sector_key=sector_key)
    rows: list[dict[str, str]] = []
    for row in candidates:
        candidate_sector = str(row.get("sector_key") or "").strip()
        if not candidate_sector:
            continue
        rows.append(
            {
                **row,
                "relation_type": "sector_sector",
                "left_key": f"cluster:{sector_key}",
                "right_key": f"cluster:{candidate_sector}",
                "relation_label": RELATION_LABEL_RELATED,
                "candidate_id": make_candidate_id(
                    relation_type="sector_sector",
                    left_key=f"cluster:{sector_key}",
                    right_key=f"cluster:{candidate_sector}",
                    relation_label=RELATION_LABEL_RELATED,
                ),
                "candidate_key": candidate_sector,
                "suggestion_reason": str(row.get("evidence_summary") or "").strip(),
            }
        )
    if not ai_enabled:
        return _finalize_candidate_rows(rows)
    return _finalize_candidate_rows(
        enrich_candidates_with_ai(
            rows,
            relation_type="sector_sector",
            ai_enabled=ai_enabled,
            should_continue=should_continue,
        )
    )


def _finalize_candidate_rows(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                str(key): str(raw or "").strip()
                for key, raw in item.items()
                if str(key).strip()
            }
        )
    return out


__all__ = [
    "build_sector_pending_candidates",
    "build_stock_pending_candidates",
]
