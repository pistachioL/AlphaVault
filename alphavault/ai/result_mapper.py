from __future__ import annotations

from typing import Any

from alphavault.ai.contracts import AiAnalyzeOutput, AiAssertionDraft
from alphavault.ai._text import clean_text, clamp_float, clamp_int
from alphavault.domains.ai_draft.models import AnalyzeDecision, AssertionDraft


def ai_analyze_output_from_parsed(parsed: dict[str, Any]) -> AiAnalyzeOutput:
    from alphavault.ai.analyze import normalize_action

    status = str(parsed.get("status", "irrelevant")).strip().lower()
    if status not in {"relevant", "irrelevant"}:
        status = "irrelevant"
    invest_score = clamp_float(parsed.get("invest_score", 0.0), 0.0, 1.0, 0.0)
    raw_assertions = parsed.get("assertions") or []
    assertions = raw_assertions if isinstance(raw_assertions, list) else []
    if status == "irrelevant":
        assertions = []

    normalized_assertions: list[AiAssertionDraft] = []
    for item in assertions[:5]:
        if not isinstance(item, dict):
            continue
        source_type = clean_text(item.get("source_type", "commentary")).lower()
        if source_type not in {"commentary", "extension", "forward_only"}:
            source_type = "commentary"
        normalized_assertions.append(
            AiAssertionDraft(
                topic_key=clean_text(item.get("topic_key", "other:misc"))
                or "other:misc",
                action=normalize_action(clean_text(item.get("action", "view.bullish"))),
                action_strength=clamp_int(item.get("action_strength", 1), 0, 3, 1),
                summary=clean_text(item.get("summary", "")) or "未提供摘要",
                evidence=clean_text(item.get("evidence", "")),
                confidence=clamp_float(item.get("confidence", 0.5), 0.0, 1.0, 0.5),
                stock_codes=_to_text_tuple(item.get("stock_codes_json", [])),
                stock_names=_to_text_tuple(item.get("stock_names_json", [])),
                industries=_to_text_tuple(item.get("industries_json", [])),
                commodities=_to_text_tuple(item.get("commodities_json", [])),
                indices=_to_text_tuple(item.get("indices_json", [])),
                source_type=source_type,
            )
        )
    return AiAnalyzeOutput(
        status=status,
        invest_score=invest_score,
        assertions=tuple(normalized_assertions),
    )


def analyze_output_to_decision(output: AiAnalyzeOutput) -> AnalyzeDecision:
    return AnalyzeDecision(status=output.status, invest_score=output.invest_score)


def ai_assertion_to_draft(assertion: AiAssertionDraft) -> AssertionDraft:
    return AssertionDraft(
        topic_key=assertion.topic_key,
        action=assertion.action,
        action_strength=assertion.action_strength,
        summary=assertion.summary,
        evidence=assertion.evidence,
        confidence=assertion.confidence,
        source_type=assertion.source_type,
        stock_codes=assertion.stock_codes,
        stock_names=assertion.stock_names,
        industries=assertion.industries,
        commodities=assertion.commodities,
        indices=assertion.indices,
    )


def drafts_to_db_rows(drafts: list[AssertionDraft]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for draft in drafts:
        rows.append(
            {
                "topic_key": draft.topic_key,
                "action": draft.action,
                "action_strength": draft.action_strength,
                "summary": draft.summary,
                "evidence": draft.evidence,
                "confidence": draft.confidence,
                "source_type": draft.source_type,
                "stock_codes_json": list(draft.stock_codes),
                "stock_names_json": list(draft.stock_names),
                "industries_json": list(draft.industries),
                "commodities_json": list(draft.commodities),
                "indices_json": list(draft.indices),
            }
        )
    return rows


def _to_text_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, list):
        return tuple(clean_text(item) for item in value if clean_text(item))
    if isinstance(value, tuple):
        return tuple(clean_text(item) for item in value if clean_text(item))
    return ()


__all__ = [
    "ai_analyze_output_from_parsed",
    "ai_assertion_to_draft",
    "analyze_output_to_decision",
    "drafts_to_db_rows",
]
