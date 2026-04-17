from __future__ import annotations

import logging
from typing import Any, Callable

from alphavault.ai._errors import format_llm_error_one_line
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
AI_STATUS_MERGE = "merge"
AI_STATUS_REJECT = "reject"
AI_STATUS_ERROR = "error"

AI_RANK_RETRY_CAP = 1
AI_RANK_BATCH_CAP = 10
AI_ERROR_REASON_PREFIX = "AI失败："
INVALID_RANKED_CANDIDATES_REASON = "AI失败：返回格式不对"

logger = logging.getLogger(__name__)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _clamp_confidence(value: object) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return max(0.0, min(float(value), 1.0))
    try:
        return max(0.0, min(float(_clean_text(value) or "0"), 1.0))
    except (TypeError, ValueError):
        return 0.0


def _format_confidence(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str) and not value.strip():
        return ""
    text = f"{_clamp_confidence(value):.2f}".rstrip("0").rstrip(".")
    return text or "0"


def _coerce_score_for_sort(value: object) -> float:
    try:
        return float(str(value or "").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def _format_ai_error_reason(err: BaseException) -> str:
    return f"{AI_ERROR_REASON_PREFIX}{format_llm_error_one_line(err, limit=160)}"


def _coerce_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    text = _clean_text(value).lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _stock_key_value(value: object) -> str:
    text = _clean_text(value)
    if text.startswith("stock:"):
        return text[len("stock:") :].strip()
    return text


def _split_stock_key(value: object) -> tuple[str, str]:
    stock_value = _stock_key_value(value).upper()
    if "." not in stock_value:
        return stock_value, ""
    code, market = stock_value.rsplit(".", 1)
    return code, market


def _with_error_reason(item: dict[str, Any], *, reason: str) -> dict[str, Any]:
    row = _with_ai_status(item, AI_STATUS_ERROR)
    row["ai_reason"] = str(reason or INVALID_RANKED_CANDIDATES_REASON).strip()
    return row


def _with_stock_alias_decision(
    item: dict[str, Any],
    *,
    can_merge: bool,
    reason: str,
    confidence: object,
) -> dict[str, Any]:
    row = dict(item)
    row["ai_status"] = AI_STATUS_MERGE if can_merge else AI_STATUS_REJECT
    row["ai_reason"] = _clean_text(reason) or _clean_text(row.get("evidence_summary"))
    row["ai_confidence"] = _format_confidence(confidence)
    return row


def _log_ai_error(
    *,
    relation_type: str,
    candidate_key: str,
    model: str,
    api_mode: str,
    err: BaseException,
) -> None:
    logger.warning(
        " ".join(
            [
                "[organizer_ai] stock_alias_error",
                f"relation_type={relation_type}",
                f"candidate_key={candidate_key or '(empty)'}",
                f"cfg_model={model or '(empty)'}",
                f"api_mode={api_mode or '(empty)'}",
                f"error={format_llm_error_one_line(err, limit=240)}",
            ]
        )
    )


def _log_ai_invalid_output(
    *,
    relation_type: str,
    candidate_key: str,
    model: str,
    api_mode: str,
    error: str,
) -> None:
    logger.warning(
        " ".join(
            [
                "[organizer_ai] stock_alias_invalid_output",
                f"relation_type={relation_type}",
                f"candidate_key={candidate_key or '(empty)'}",
                f"cfg_model={model or '(empty)'}",
                f"api_mode={api_mode or '(empty)'}",
                f"error={error or '(empty)'}",
            ]
        )
    )


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
        if relation_type == "stock_alias":
            return _judge_stock_alias_candidates_with_ai(
                candidates,
                relation_type=relation_type,
                should_continue=should_continue,
            )
        return _rank_candidates_with_ai(
            candidates,
            relation_type=relation_type,
            should_continue=should_continue,
        )
    except Exception as err:
        error_reason = _format_ai_error_reason(err)
        return [_with_error_reason(item, reason=error_reason) for item in candidates]


def _with_ai_status(item: dict[str, Any], status: str) -> dict[str, Any]:
    row = dict(item)
    row["ai_status"] = status
    row.setdefault("ai_confidence", "")
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
    for item in candidates[:AI_RANK_BATCH_CAP]:
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
      "ai_confidence": 0.0,
      "relation_label": "related|parent_child（仅 sector_sector 需要）"
    }}
  ]
}}

规则：
- 只能使用候选列表里的 candidate_key，禁止编造。
- 只有 relation_type=sector_sector 时，才需要输出 relation_label；其他类型可以不输出。
- 没把握就保守，优先沿用 evidence。
""".strip()

    try:
        parsed = _call_ai_with_litellm(
            prompt=prompt,
            api_mode=config.api_mode,
            ai_stream=False,
            model_name=config.model,
            base_url=config.base_url,
            api_key=config.api_key,
            timeout_seconds=float(config.timeout_seconds),
            retry_count=min(int(config.retries), AI_RANK_RETRY_CAP),
            temperature=float(config.temperature),
            reasoning_effort=str(config.reasoning_effort),
            trace_out=None,
            trace_label=f"relation_candidates:{relation_type}",
        )
    except Exception as err:
        candidate_key = (
            str(candidates[0].get("candidate_key") or "").strip() if candidates else ""
        )
        _log_ai_error(
            relation_type=relation_type,
            candidate_key=candidate_key,
            model=str(config.model or "").strip(),
            api_mode=str(config.api_mode or "").strip(),
            err=err,
        )
        error_reason = _format_ai_error_reason(err)
        return [_with_error_reason(item, reason=error_reason) for item in candidates]
    ranked_items = parsed.get("ranked_candidates") if isinstance(parsed, dict) else None
    if not isinstance(ranked_items, list):
        logger.warning(
            " ".join(
                [
                    "[organizer_ai] stock_alias_invalid_output",
                    f"relation_type={relation_type}",
                    f"candidate_key={str(candidates[0].get('candidate_key') or '').strip() if candidates else '(empty)'}",
                    f"cfg_model={str(config.model or '').strip() or '(empty)'}",
                    f"api_mode={str(config.api_mode or '').strip() or '(empty)'}",
                    "error=ranked_candidates_missing",
                ]
            )
        )
        return [
            _with_error_reason(item, reason=INVALID_RANKED_CANDIDATES_REASON)
            for item in candidates
        ]

    rank_map: dict[str, tuple[int, str, str, str]] = {}
    for index, item in enumerate(ranked_items):
        if not isinstance(item, dict):
            continue
        candidate_key = str(item.get("candidate_key") or "").strip()
        if not candidate_key:
            continue
        ai_reason = str(item.get("ai_reason") or "").strip()
        ai_confidence = _format_confidence(item.get("ai_confidence"))
        relation_label = str(item.get("relation_label") or "").strip()
        rank_map[candidate_key] = (
            index,
            ai_reason,
            ai_confidence,
            relation_label,
        )

    out: list[dict[str, Any]] = []
    for fallback_index, item in enumerate(candidates):
        row = dict(item)
        candidate_key = str(row.get("candidate_key") or "").strip()
        rank_index, ai_reason, ai_confidence, relation_label = rank_map.get(
            candidate_key,
            (len(candidates) + fallback_index, "", "", ""),
        )
        row["_rank_index"] = rank_index
        row["ai_status"] = AI_STATUS_RANKED
        row["ai_reason"] = ai_reason or str(row.get("evidence_summary") or "").strip()
        row["ai_confidence"] = ai_confidence
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

    out.sort(
        key=lambda item: (
            int(item["_rank_index"]),
            -_coerce_score_for_sort(item.get("score")),
        )
    )
    for row in out:
        row.pop("_rank_index", None)
    return out


def _build_stock_alias_prompt(item: dict[str, Any]) -> str:
    left_key = _clean_text(item.get("left_key"))
    right_key = _clean_text(item.get("right_key") or item.get("candidate_key"))
    left_code, left_market = _split_stock_key(left_key)
    return f"""
你是股票归并判断助手。请只判断当前这 1 对候选能不能合并成同一只股票，并输出严格 JSON。

当前候选：
- left_key={left_key}
- left_code={left_code}
- left_market={left_market}
- left_official_name={_clean_text(item.get("left_official_name"))}
- right_key={right_key}
- alias_text={_stock_key_value(right_key)}
- score={_clean_text(item.get("score"))}
- suggestion_reason={_clean_text(item.get("suggestion_reason"))}
- evidence_summary={_clean_text(item.get("evidence_summary"))}
- sample_post_uid={_clean_text(item.get("sample_post_uid"))}
- sample_evidence={_clean_text(item.get("sample_evidence"))}
- sample_raw_text_excerpt={_clean_text(item.get("sample_raw_text_excerpt"))}

输出 JSON：
{{
  "can_merge": true,
  "confidence": 0.0,
  "reason": "一句话理由"
}}

规则：
- 只能判断当前这对 left_key/right_key，禁止换成别的股票，禁止跨候选重新配对。
- 只有非常明确是同一只股票的别名、简称或代码时，can_merge=true。
- 只要市场、代码、名字或上下文明显对不上，就返回 can_merge=false。
- 没把握时也返回 can_merge=false。
- 不要输出 Markdown。
""".strip()


def _judge_stock_alias_candidates_with_ai(
    candidates: list[dict[str, Any]],
    *,
    relation_type: str,
    should_continue: Callable[[], bool] | None = None,
) -> list[dict[str, Any]]:
    config = ai_runtime_config_from_env(timeout_seconds_default=1000.0)
    out: list[dict[str, Any]] = []
    for index, item in enumerate(candidates):
        if should_continue is not None:
            try:
                if not bool(should_continue()):
                    out.extend(
                        _with_ai_status(rest, AI_STATUS_SKIPPED)
                        for rest in candidates[index:]
                    )
                    break
            except Exception:
                out.extend(
                    _with_ai_status(rest, AI_STATUS_SKIPPED)
                    for rest in candidates[index:]
                )
                break

        candidate_key = _clean_text(item.get("candidate_key") or item.get("right_key"))
        try:
            parsed = _call_ai_with_litellm(
                prompt=_build_stock_alias_prompt(item),
                api_mode=config.api_mode,
                ai_stream=False,
                model_name=config.model,
                base_url=config.base_url,
                api_key=config.api_key,
                timeout_seconds=float(config.timeout_seconds),
                retry_count=min(int(config.retries), AI_RANK_RETRY_CAP),
                temperature=float(config.temperature),
                reasoning_effort=str(config.reasoning_effort),
                trace_out=None,
                trace_label="relation_candidates:stock_alias",
            )
        except Exception as err:
            _log_ai_error(
                relation_type=relation_type,
                candidate_key=candidate_key,
                model=str(config.model or "").strip(),
                api_mode=str(config.api_mode or "").strip(),
                err=err,
            )
            out.append(_with_error_reason(item, reason=_format_ai_error_reason(err)))
            continue

        if not isinstance(parsed, dict):
            _log_ai_invalid_output(
                relation_type=relation_type,
                candidate_key=candidate_key,
                model=str(config.model or "").strip(),
                api_mode=str(config.api_mode or "").strip(),
                error="parsed_not_dict",
            )
            out.append(
                _with_error_reason(item, reason=INVALID_RANKED_CANDIDATES_REASON)
            )
            continue

        can_merge = _coerce_bool(parsed.get("can_merge"))
        if can_merge is None:
            _log_ai_invalid_output(
                relation_type=relation_type,
                candidate_key=candidate_key,
                model=str(config.model or "").strip(),
                api_mode=str(config.api_mode or "").strip(),
                error="can_merge_missing",
            )
            out.append(
                _with_error_reason(item, reason=INVALID_RANKED_CANDIDATES_REASON)
            )
            continue

        out.append(
            _with_stock_alias_decision(
                item,
                can_merge=can_merge,
                reason=_clean_text(parsed.get("reason")),
                confidence=parsed.get("confidence"),
            )
        )
    return out


__all__ = [
    "AI_RANK_BATCH_CAP",
    "AI_STATUS_ERROR",
    "AI_STATUS_MERGE",
    "AI_STATUS_RANKED",
    "AI_STATUS_REJECT",
    "AI_STATUS_SKIPPED",
    "enrich_candidates_with_ai",
]
