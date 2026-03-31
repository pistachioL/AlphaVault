from __future__ import annotations

from typing import Any, Callable

from alphavault.ai.analyze import _call_ai_with_litellm
from alphavault.ai.topic_cluster_suggest import ai_is_configured
from alphavault.domains.relation.relation_candidates import (
    RELATION_LABEL_PARENT_CHILD,
    RELATION_LABEL_RELATED,
    classify_sector_relation_label,
)

from .runtime_config import ai_runtime_config_from_env


AI_STATUS_SKIPPED = "skipped"
AI_STATUS_RANKED = "ranked"
AI_STATUS_ERROR = "error"

AI_RANK_TIMEOUT_SECONDS_CAP = 20.0
AI_RANK_RETRY_CAP = 1


def enrich_candidates_with_ai(
    candidates: list[dict[str, Any]],
    *,
    relation_type: str,
    ai_enabled: bool,
    should_continue: Callable[[], bool] | None = None,
) -> list[dict[str, Any]]:
    if not candidates:
        return []
    if not ai_enabled:
        return [_with_ai_status(item, AI_STATUS_SKIPPED) for item in candidates]
    ok, _err = ai_is_configured()
    if not ok:
        return [_with_ai_status(item, AI_STATUS_SKIPPED) for item in candidates]
    try:
        return _rank_candidates_with_ai(
            candidates,
            relation_type=relation_type,
            should_continue=should_continue,
        )
    except Exception:
        return [_with_ai_status(item, AI_STATUS_ERROR) for item in candidates]


def _with_ai_status(item: dict[str, Any], status: str) -> dict[str, Any]:
    row = dict(item)
    row["ai_status"] = status
    if status != AI_STATUS_RANKED and "ai_reason" not in row:
        row["ai_reason"] = str(row.get("evidence_summary") or "").strip()
    return row


def _rank_candidates_with_ai(
    candidates: list[dict[str, Any]],
    *,
    relation_type: str,
    should_continue: Callable[[], bool] | None = None,
) -> list[dict[str, Any]]:
    if should_continue is not None:
        try:
            if not bool(should_continue()):
                return [_with_ai_status(item, AI_STATUS_SKIPPED) for item in candidates]
        except Exception:
            return [_with_ai_status(item, AI_STATUS_SKIPPED) for item in candidates]

    config = ai_runtime_config_from_env(timeout_seconds_default=1000.0)
    candidate_lines: list[str] = []
    for item in candidates[:10]:
        candidate_lines.append(
            f"- key={str(item.get('candidate_key') or '').strip()}; "
            f"score={str(item.get('score') or '').strip()}; "
            f"evidence={str(item.get('evidence_summary') or '').strip()}"
        )
    prompt = f"""
你是关系排序助手。请基于候选列表做保守排序，并输出严格 JSON。

relation_type: {relation_type}

候选列表：
{chr(10).join(candidate_lines)}

输出 JSON：
{{
  "ranked_candidates": [
    {{
      "candidate_key": "...",
      "ai_reason": "一句话理由",
      "relation_label": "related|parent_child（仅 sector_sector 需要）"
    }}
  ]
}}

规则：
- 只能使用候选列表里的 candidate_key，禁止编造。
- 只有 relation_type=sector_sector 时，才需要输出 relation_label；其他类型可以不输出。
- 没把握就保守，优先沿用 evidence。
""".strip()

    parsed = _call_ai_with_litellm(
        prompt=prompt,
        api_mode=config.api_mode,
        ai_stream=False,
        model_name=config.model,
        base_url=config.base_url,
        api_key=config.api_key,
        timeout_seconds=min(float(config.timeout_seconds), AI_RANK_TIMEOUT_SECONDS_CAP),
        retry_count=min(int(config.retries), AI_RANK_RETRY_CAP),
        temperature=float(config.temperature),
        reasoning_effort=str(config.reasoning_effort),
        trace_out=None,
        trace_label=f"relation_candidates:{relation_type}",
    )
    ranked_items = parsed.get("ranked_candidates") if isinstance(parsed, dict) else None
    if not isinstance(ranked_items, list):
        return [_with_ai_status(item, AI_STATUS_ERROR) for item in candidates]

    rank_map: dict[str, tuple[int, str, str]] = {}
    for index, item in enumerate(ranked_items):
        if not isinstance(item, dict):
            continue
        candidate_key = str(item.get("candidate_key") or "").strip()
        if not candidate_key:
            continue
        ai_reason = str(item.get("ai_reason") or "").strip()
        relation_label = str(item.get("relation_label") or "").strip()
        rank_map[candidate_key] = (index, ai_reason, relation_label)

    out: list[dict[str, Any]] = []
    for fallback_index, item in enumerate(candidates):
        row = dict(item)
        candidate_key = str(row.get("candidate_key") or "").strip()
        rank_index, ai_reason, relation_label = rank_map.get(
            candidate_key,
            (len(candidates) + fallback_index, "", ""),
        )
        row["_rank_index"] = rank_index
        row["ai_status"] = AI_STATUS_RANKED
        row["ai_reason"] = ai_reason or str(row.get("evidence_summary") or "").strip()
        if relation_type == "sector_sector":
            row["relation_label"] = (
                relation_label
                if relation_label
                in {RELATION_LABEL_RELATED, RELATION_LABEL_PARENT_CHILD}
                else classify_sector_relation_label(
                    ai_enabled=True,
                    explanation=row["ai_reason"],
                )
            )
        else:
            existing_label = str(row.get("relation_label") or "").strip()
            row["relation_label"] = existing_label or RELATION_LABEL_RELATED
        out.append(row)

    out.sort(key=lambda item: (int(item["_rank_index"]), -int(item.get("score") or 0)))
    for row in out:
        row.pop("_rank_index", None)
    return out


__all__ = [
    "AI_STATUS_ERROR",
    "AI_STATUS_RANKED",
    "AI_STATUS_SKIPPED",
    "enrich_candidates_with_ai",
]
