from __future__ import annotations

from alphavault.research_stock_cache import mark_entity_page_dirty
from alphavault.research_workbench import (
    accept_relation_candidate,
    block_relation_candidate,
    get_research_workbench_engine_from_env,
    ignore_relation_candidate,
    upsert_relation_candidate,
)
from alphavault_reflex.services.source_read import load_source_engines_from_env


def apply_candidate_action(candidate_row: dict[str, str], action: str) -> None:
    engine = get_research_workbench_engine_from_env()
    left_key = str(candidate_row.get("left_key") or "").strip()
    upsert_relation_candidate(
        engine,
        candidate_id=str(candidate_row.get("candidate_id") or "").strip(),
        relation_type=str(candidate_row.get("relation_type") or "").strip(),
        left_key=left_key,
        right_key=str(candidate_row.get("right_key") or "").strip(),
        relation_label=str(candidate_row.get("relation_label") or "").strip(),
        suggestion_reason=str(candidate_row.get("suggestion_reason") or "").strip(),
        evidence_summary=str(candidate_row.get("evidence_summary") or "").strip(),
        score=float(str(candidate_row.get("score") or "0") or 0),
        ai_status=str(candidate_row.get("ai_status") or "").strip(),
    )
    action_name = str(action or "").strip()
    candidate_id = str(candidate_row.get("candidate_id") or "").strip()
    if action_name == "accept":
        accept_relation_candidate(engine, candidate_id=candidate_id, source="manual")
    elif action_name == "ignore":
        ignore_relation_candidate(engine, candidate_id=candidate_id)
    elif action_name == "block":
        block_relation_candidate(engine, candidate_id=candidate_id)
    if left_key.startswith("stock:"):
        for source_engine in load_source_engines_from_env():
            mark_entity_page_dirty(
                source_engine,
                stock_key=left_key,
                reason="candidate_action",
            )


def apply_candidate_action_by_id(
    *,
    rows: list[dict[str, str]],
    candidate_id: str,
    action: str,
) -> bool:
    target = str(candidate_id or "").strip()
    if not target:
        return False
    row = next(
        (
            item
            for item in rows
            if str(item.get("candidate_id") or "").strip() == target
        ),
        None,
    )
    if row is None:
        return False
    apply_candidate_action(row, action)
    return True


__all__ = ["apply_candidate_action", "apply_candidate_action_by_id"]
