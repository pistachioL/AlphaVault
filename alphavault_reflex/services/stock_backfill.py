from __future__ import annotations

import json
import os
from typing import Any

from alphavault.ai.analyze import (
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_AI_RETRY_COUNT,
    DEFAULT_AI_TEMPERATURE,
    DEFAULT_MODEL,
    _call_ai_with_litellm,
    clamp_float,
    clamp_int,
    clean_text,
    normalize_action,
    validate_and_adjust_assertions,
)
from alphavault.ai.topic_cluster_suggest import ai_is_configured
from alphavault.constants import (
    ENV_AI_API_KEY,
    ENV_AI_API_MODE,
    ENV_AI_BASE_URL,
    ENV_AI_MODEL,
    ENV_AI_REASONING_EFFORT,
    ENV_AI_RETRIES,
    ENV_AI_TEMPERATURE,
    ENV_AI_TIMEOUT_SEC,
)
from alphavault.rss.utils import build_analysis_context, build_row_meta
from alphavault.domains.stock.key_match import is_stock_code_value

BACKFILL_PROMPT_VERSION = "stock_backfill_v1"
MAX_TARGETED_ASSERTIONS = 3


def run_targeted_stock_backfill(
    post_row: dict[str, str],
    *,
    stock_key: str,
    display_name: str,
) -> list[dict[str, Any]]:
    ok, err = ai_is_configured()
    if not ok:
        raise RuntimeError(err or "ai_not_configured")

    raw_text = str(post_row.get("raw_text") or "").strip()
    if not raw_text:
        return []
    analysis_context = build_analysis_context(raw_text)
    row_meta = build_row_meta(
        mid_or_bid=str(post_row.get("platform_post_id") or ""),
        bid="",
        link=str(post_row.get("url") or ""),
        title="",
        author=str(post_row.get("author") or ""),
        created_at=str(post_row.get("created_at") or ""),
        raw_text=raw_text,
    )
    target_name = _normalize_display_name(display_name, stock_key=stock_key)
    stock_code = _stock_code_from_key(stock_key)
    target_json = {
        "stock_key": stock_key,
        "stock_name": target_name,
        "stock_code": stock_code,
    }
    prompt = f"""
你是个股定向回补助手。你只判断：这篇文章是不是在讲目标个股；如果是，只抽这个目标个股的交易断言。

目标个股：
{json.dumps(target_json, ensure_ascii=False)}

输出严格 JSON：
{{
  "status": "relevant|irrelevant",
  "invest_score": 0.0,
  "assertions": [
    {{
      "action": "trade.buy|trade.add|trade.reduce|trade.sell|trade.hold|trade.watch",
      "action_strength": 0,
      "summary": "一句话摘要",
      "evidence": "必须是原文片段",
      "confidence": 0.0
    }}
  ]
}}

规则：
- 只看目标个股，不要抽别的票。
- 如果文章没明显在讲目标个股，返回 irrelevant 和空数组。
- 只能输出 trade.* 动作。
- evidence 必须来自原文，不要编造。
- 没把握就保守，返回空数组。

commentary_text:
{analysis_context.get("commentary_text", "")}

quoted_text:
{analysis_context.get("quoted_text", "")}

补充元信息:
{json.dumps(row_meta, ensure_ascii=False)}
""".strip()

    parsed = _call_ai_with_litellm(
        prompt=prompt,
        api_mode=_env_or_default(ENV_AI_API_MODE, DEFAULT_AI_MODE).lower(),
        ai_stream=False,
        model_name=_env_or_default(ENV_AI_MODEL, DEFAULT_MODEL),
        base_url=os.getenv(ENV_AI_BASE_URL, "").strip(),
        api_key=os.getenv(ENV_AI_API_KEY, "").strip(),
        timeout_seconds=float(os.getenv(ENV_AI_TIMEOUT_SEC, "60").strip() or 60),
        retry_count=int(
            os.getenv(ENV_AI_RETRIES, str(DEFAULT_AI_RETRY_COUNT)).strip()
            or DEFAULT_AI_RETRY_COUNT
        ),
        temperature=float(
            os.getenv(ENV_AI_TEMPERATURE, str(DEFAULT_AI_TEMPERATURE)).strip()
            or DEFAULT_AI_TEMPERATURE
        ),
        reasoning_effort=_env_or_default(
            ENV_AI_REASONING_EFFORT,
            DEFAULT_AI_REASONING_EFFORT,
        ),
        trace_out=None,
        trace_label=f"stock_backfill:{stock_key}:{post_row.get('post_uid', '')}",
    )
    status = str(parsed.get("status", "irrelevant")).strip().lower()
    if status != "relevant":
        return []
    assertions = parsed.get("assertions") or []
    if not isinstance(assertions, list):
        return []
    normalized = []
    for item in assertions[:MAX_TARGETED_ASSERTIONS]:
        if not isinstance(item, dict):
            continue
        action = normalize_action(clean_text(item.get("action", "trade.watch")))
        if not action.startswith("trade."):
            continue
        normalized.append(
            {
                "topic_key": stock_key,
                "action": action,
                "action_strength": clamp_int(item.get("action_strength", 1), 0, 3, 1),
                "summary": clean_text(item.get("summary", "")) or "未提供摘要",
                "evidence": clean_text(item.get("evidence", "")),
                "confidence": clamp_float(item.get("confidence", 0.5), 0.0, 1.0, 0.5),
                "stock_codes_json": json.dumps(
                    [stock_code] if stock_code else [], ensure_ascii=False
                ),
                "stock_names_json": json.dumps(
                    [target_name] if target_name else [], ensure_ascii=False
                ),
                "industries_json": "[]",
                "commodities_json": "[]",
                "indices_json": "[]",
                "keywords_json": "[]",
                "assertion_entities": [
                    {
                        "entity_key": stock_key,
                        "entity_type": "stock",
                        "source_mention_text": target_name or stock_code,
                        "source_mention_type": (
                            "stock_name" if target_name else "stock_code"
                        ),
                        "confidence": clamp_float(
                            item.get("confidence", 0.5), 0.0, 1.0, 0.5
                        ),
                    }
                ],
                "source_type": "commentary",
            }
        )
    fixed = validate_and_adjust_assertions(
        normalized,
        commentary_text=analysis_context.get("commentary_text", ""),
        quoted_text=analysis_context.get("quoted_text", ""),
    )
    for item in fixed:
        item.pop("source_type", None)
    return fixed


def merge_post_assertions(
    existing_assertions: list[dict[str, Any]],
    new_assertions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out = [dict(item) for item in existing_assertions]
    seen = {_assertion_key(item) for item in out}
    for item in new_assertions:
        key = _assertion_key(item)
        if key in seen:
            continue
        seen.add(key)
        out.append(dict(item))
    return out


def _assertion_key(item: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(item.get("topic_key") or "").strip(),
        str(item.get("action") or "").strip(),
        str(item.get("summary") or "").strip(),
        str(item.get("evidence") or "").strip(),
    )


def _stock_code_from_key(stock_key: str) -> str:
    value = str(stock_key or "").strip()
    if not value.startswith("stock:"):
        return ""
    stock_value = value[len("stock:") :].strip()
    return stock_value if is_stock_code_value(stock_value) else ""


def _normalize_display_name(display_name: str, *, stock_key: str) -> str:
    text = str(display_name or "").strip()
    code = _stock_code_from_key(stock_key)
    suffix = f"({code})"
    if code and text.endswith(suffix):
        text = text[: -len(suffix)].strip()
    return text


def _env_or_default(name: str, default: str) -> str:
    return str(os.getenv(name, default) or default).strip() or default


__all__ = [
    "BACKFILL_PROMPT_VERSION",
    "merge_post_assertions",
    "run_targeted_stock_backfill",
]
