from __future__ import annotations

import logging
from typing import Any, Callable

from alphavault.ai._errors import format_llm_error_one_line
from alphavault.ai.analyze import _call_ai_with_litellm
from alphavault.ai.topic_cluster_suggest import ai_is_configured
from alphavault.domains.stock.key_match import (
    is_stock_code_value,
    normalize_stock_code,
)

from .runtime_config import AiRuntimeConfig, ai_runtime_config_from_env


AI_STATUS_SKIPPED = "skipped"
AI_STATUS_RANKED = "ranked"
AI_STATUS_ERROR = "error"

_ALIAS_AI_BATCH_CAP = 10
_ALIAS_AI_RETRY_CAP = 1
AI_ERROR_REASON_PREFIX = "AI失败："
INVALID_PREDICTIONS_REASON = "AI失败：返回格式不对"
MISSING_PREDICTION_REASON = "AI失败：未返回这条简称的结果"

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
    score = _clamp_confidence(value)
    text = f"{score:.2f}".rstrip("0").rstrip(".")
    return text or "0"


def _format_ai_error_reason(err: BaseException) -> str:
    return f"{AI_ERROR_REASON_PREFIX}{format_llm_error_one_line(err, limit=160)}"


def _log_ai_error(
    *,
    alias_key: str,
    model: str,
    api_mode: str,
    err: BaseException,
) -> None:
    logger.warning(
        " ".join(
            [
                "[organizer_ai] alias_manual_error",
                f"alias_key={alias_key or '(empty)'}",
                f"cfg_model={model or '(empty)'}",
                f"api_mode={api_mode or '(empty)'}",
                f"error={format_llm_error_one_line(err, limit=240)}",
            ]
        )
    )


def _normalize_bool_flag(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else ""
    raw = _clean_text(value).lower()
    return "true" if raw in {"1", "true", "yes", "y"} else ""


def _normalize_stock_code_or_empty(value: object) -> str:
    code = normalize_stock_code(value=str(value or ""))
    return code if is_stock_code_value(code) else ""


def _with_ai_fields(
    item: dict[str, Any],
    *,
    status: str,
    stock_code: str = "",
    official_name: str = "",
    confidence: str = "",
    reason: str = "",
    uncertain: str = "",
) -> dict[str, Any]:
    row = dict(item)
    row["ai_status"] = status
    row["ai_stock_code"] = _clean_text(stock_code)
    row["ai_official_name"] = _clean_text(official_name)
    row["ai_confidence"] = _clean_text(confidence)
    row["ai_reason"] = _clean_text(reason) or _clean_text(row.get("sample_evidence"))
    row["ai_uncertain"] = _clean_text(uncertain)
    return row


def _ensure_ai_fields(item: dict[str, Any]) -> dict[str, Any]:
    row = dict(item)
    for key in (
        "ai_status",
        "ai_stock_code",
        "ai_official_name",
        "ai_confidence",
        "ai_reason",
        "ai_uncertain",
    ):
        row.setdefault(key, "")
    return row


def _resolve_runtime_config(runtime_config: AiRuntimeConfig | None) -> AiRuntimeConfig:
    if runtime_config is not None:
        return runtime_config
    return ai_runtime_config_from_env(timeout_seconds_default=1000.0)


def _has_runtime_config(runtime_config: AiRuntimeConfig | None) -> bool:
    if runtime_config is None:
        ok, _err = ai_is_configured()
        return bool(ok)
    return bool(_clean_text(runtime_config.api_key))


def enrich_alias_tasks_with_ai(
    tasks: list[dict[str, Any]],
    *,
    ai_enabled: bool,
    limit: int = _ALIAS_AI_BATCH_CAP,
    should_continue: Callable[[], bool] | None = None,
    runtime_config: AiRuntimeConfig | None = None,
    request_gate: Callable[[], None] | None = None,
) -> list[dict[str, Any]]:
    rows = [_ensure_ai_fields(item) for item in tasks]
    if not rows:
        return []

    batch_size = max(0, min(int(limit), _ALIAS_AI_BATCH_CAP))
    attempted = rows[:batch_size]
    untouched = rows[batch_size:]
    if not attempted:
        return rows

    if should_continue is not None:
        try:
            if not bool(should_continue()):
                return [
                    _with_ai_fields(item, status=AI_STATUS_SKIPPED)
                    for item in attempted
                ] + untouched
        except Exception:
            return [
                _with_ai_fields(item, status=AI_STATUS_SKIPPED) for item in attempted
            ] + untouched

    if not ai_enabled:
        return [
            _with_ai_fields(item, status=AI_STATUS_SKIPPED) for item in attempted
        ] + untouched

    if not _has_runtime_config(runtime_config):
        return [
            _with_ai_fields(item, status=AI_STATUS_SKIPPED) for item in attempted
        ] + untouched

    try:
        ranked = _predict_alias_tasks_with_ai(
            attempted,
            runtime_config=runtime_config,
            request_gate=request_gate,
        )
    except Exception as err:
        error_reason = _format_ai_error_reason(err)
        ranked = [
            _with_ai_fields(item, status=AI_STATUS_ERROR, reason=error_reason)
            for item in attempted
        ]
    return ranked + untouched


def _predict_alias_tasks_with_ai(
    tasks: list[dict[str, Any]],
    *,
    runtime_config: AiRuntimeConfig | None = None,
    request_gate: Callable[[], None] | None = None,
) -> list[dict[str, Any]]:
    config = _resolve_runtime_config(runtime_config)
    task_lines: list[str] = []
    for item in tasks:
        alias_key = _clean_text(item.get("alias_key"))
        alias_text = (
            alias_key[len("stock:") :] if alias_key.startswith("stock:") else alias_key
        )
        task_lines.append(
            "\n".join(
                [
                    f"- alias_key={alias_key}",
                    f"  alias={alias_text}",
                    f"  sample_post_uid={_clean_text(item.get('sample_post_uid'))}",
                    f"  sample_evidence={_clean_text(item.get('sample_evidence'))}",
                    "  sample_raw_text_excerpt="
                    f"{_clean_text(item.get('sample_raw_text_excerpt'))}",
                ]
            )
        )

    prompt = f"""
你是股票简称预判助手。请根据每个简称的样例上下文，保守判断它最可能对应的股票代码和正式简称，并输出严格 JSON。

输入列表：
{chr(10).join(task_lines)}

输出 JSON：
{{
  "predictions": [
    {{
      "alias_key": "stock:...",
      "stock_code": "600519.SH",
      "official_name": "贵州茅台",
      "confidence": 0.0,
      "reason": "一句话理由",
      "is_uncertain": false
    }}
  ]
}}

规则：
- alias_key 只能从输入列表里选，禁止编造。
- 每个输入最多返回 1 条 prediction。
- 没把握就保守：stock_code 和 official_name 可以留空，并把 is_uncertain 设成 true。
- stock_code 必须是标准格式，比如 600519.SH、0005.HK、AAPL.US。
- 不要输出 Markdown。
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
            retry_count=min(int(config.retries), _ALIAS_AI_RETRY_CAP),
            temperature=float(config.temperature),
            reasoning_effort=str(config.reasoning_effort),
            trace_out=None,
            trace_label="alias_tasks:predict",
            request_gate=request_gate,
        )
    except Exception as err:
        alias_key = str(tasks[0].get("alias_key") or "").strip() if tasks else ""
        _log_ai_error(
            alias_key=alias_key,
            model=str(config.model or "").strip(),
            api_mode=str(config.api_mode or "").strip(),
            err=err,
        )
        raise
    predictions = parsed.get("predictions") if isinstance(parsed, dict) else None
    if not isinstance(predictions, list):
        logger.warning(
            " ".join(
                [
                    "[organizer_ai] alias_manual_invalid_output",
                    f"alias_key={str(tasks[0].get('alias_key') or '').strip() if tasks else '(empty)'}",
                    f"cfg_model={str(config.model or '').strip() or '(empty)'}",
                    f"api_mode={str(config.api_mode or '').strip() or '(empty)'}",
                    "error=predictions_missing",
                ]
            )
        )
        return [
            _with_ai_fields(
                item,
                status=AI_STATUS_ERROR,
                reason=INVALID_PREDICTIONS_REASON,
            )
            for item in tasks
        ]

    prediction_map: dict[str, dict[str, Any]] = {}
    for item in predictions:
        if not isinstance(item, dict):
            continue
        alias_key = _clean_text(item.get("alias_key"))
        if not alias_key:
            continue
        prediction_map[alias_key] = item

    out: list[dict[str, Any]] = []
    for item in tasks:
        alias_key = _clean_text(item.get("alias_key"))
        prediction = prediction_map.get(alias_key)
        if prediction is None:
            out.append(
                _with_ai_fields(
                    item,
                    status=AI_STATUS_ERROR,
                    reason=MISSING_PREDICTION_REASON,
                    uncertain="true",
                )
            )
            continue
        out.append(
            _with_ai_fields(
                item,
                status=AI_STATUS_RANKED,
                stock_code=_normalize_stock_code_or_empty(prediction.get("stock_code")),
                official_name=_clean_text(prediction.get("official_name")),
                confidence=_format_confidence(prediction.get("confidence")),
                reason=_clean_text(prediction.get("reason")),
                uncertain=_normalize_bool_flag(prediction.get("is_uncertain")),
            )
        )
    return out


__all__ = [
    "AI_STATUS_ERROR",
    "AI_STATUS_RANKED",
    "AI_STATUS_SKIPPED",
    "enrich_alias_tasks_with_ai",
]
