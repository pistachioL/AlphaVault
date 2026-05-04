from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from alphavault.constants import (
    ENV_AI_API_MODE,
    ENV_AI_MODEL,
    ENV_AI_PROMPT_VERSION,
    ENV_AI_REASONING_EFFORT,
    ENV_AI_RETRIES,
    ENV_AI_TEMPERATURE,
)
from alphavault.ai._client import _call_ai_with_openai
from alphavault.ai._errors import extract_llm_error_details, format_llm_error_one_line
from alphavault.ai._text import clean_text, clamp_float, clamp_int, parse_json_text
from alphavault.ai.result_mapper import (
    ai_analyze_output_from_parsed,
    ai_assertion_to_draft,
)
from alphavault.ai.topic_prompt_v4 import TOPIC_PROMPT_VERSION
from alphavault.domains.ai_draft.models import AssertionDraft

AI_MODE_COMPLETION = "completion"
AI_MODE_RESPONSES = "responses"

DEFAULT_MODEL = os.getenv(ENV_AI_MODEL, "gpt-5.2")
DEFAULT_PROMPT_VERSION = os.getenv(ENV_AI_PROMPT_VERSION, TOPIC_PROMPT_VERSION)

DEFAULT_AI_MODE = os.getenv(ENV_AI_API_MODE, AI_MODE_RESPONSES)
DEFAULT_AI_TEMPERATURE = float(os.getenv(ENV_AI_TEMPERATURE, "0.1"))
DEFAULT_AI_RETRY_COUNT = int(os.getenv(ENV_AI_RETRIES, "11"))
DEFAULT_AI_REASONING_EFFORT = os.getenv(ENV_AI_REASONING_EFFORT, "xhigh")

ALLOWED_ACTIONS = {
    "trade.buy",
    "trade.add",
    "trade.reduce",
    "trade.sell",
    "trade.hold",
    "trade.watch",
    "view.bullish",
    "view.bearish",
    "valuation.cheap",
    "valuation.expensive",
    "risk.warning",
    "risk.event",
    "education.method",
    "education.mindset",
    "education.life",
}

LEGACY_ACTION_MAP = {
    "buy": "trade.buy",
    "sell": "trade.sell",
    "hold": "trade.hold",
    "risk_warning": "risk.warning",
    "valuation": "valuation.cheap",
    "macro_view": "view.bearish",
    "news_interpretation": "view.bullish",
    "method": "education.method",
}


@dataclass
class AnalyzeResult:
    status: str
    invest_score: float
    assertions: list[AssertionDraft]


def normalize_action(action: str) -> str:
    a = clean_text(action).lower()
    if a in ALLOWED_ACTIONS:
        return a
    if a in LEGACY_ACTION_MAP:
        return LEGACY_ACTION_MAP[a]
    if a.startswith("trade."):
        return a if a in ALLOWED_ACTIONS else "trade.watch"
    if a.startswith("risk."):
        return a if a in ALLOWED_ACTIONS else "risk.warning"
    if a.startswith("valuation."):
        return a if a in ALLOWED_ACTIONS else "valuation.cheap"
    if a.startswith("education."):
        return a if a in ALLOWED_ACTIONS else "education.method"
    if a.startswith("view."):
        return a if a in ALLOWED_ACTIONS else "view.bullish"
    return "view.bullish"


def analyze_with_openai(
    api_key: str,
    model: str,
    analysis_context: dict[str, str],
    row: dict[str, str],
    base_url: str,
    api_mode: str,
    ai_stream: bool,
    ai_retries: int,
    ai_temperature: float,
    ai_reasoning_effort: str,
    trace_out: Optional[Path],
    timeout_seconds: float = 1000.0,
) -> AnalyzeResult:
    base_url = (base_url or "").rstrip("/")
    prompt = f"""
你是金融内容分析助手。请分析一条微博，输出严格 JSON（不要 Markdown）。

任务:
1) 判断是否为投资相关: status 只能是 "relevant" 或 "irrelevant"
2) 给出 invest_score (0 到 1)
3) 如果 relevant，抽取观点 assertions（0~5 条）

JSON 结构:
{{
  "status": "relevant|irrelevant",
  "invest_score": 0.0,
  "assertions": [
    {{
      "topic_key": "industry:电力 或 stock:601225.SH 等",
      "action": "trade.buy|trade.add|trade.reduce|trade.sell|trade.hold|trade.watch|view.bullish|view.bearish|valuation.cheap|valuation.expensive|risk.warning|risk.event|education.method|education.mindset|education.life",
      "action_strength": 0,
      "summary": "一句话摘要",
      "evidence": "必须是原文片段",
      "source_type": "commentary|extension|forward_only",
      "confidence": 0.0,
      "stock_codes_json": ["600000.SH"],
      "stock_names_json": ["浦发银行"],
      "industries_json": ["银行"],
      "commodities_json": [],
      "indices_json": []
    }}
  ]
}}

要求:
- irrelevant 时 assertions 必须为空数组
- action_strength 为 0~3 的整数
- confidence 为 0~1
- evidence 必须优先来自 commentary_text，不要编造
- 无法确定时给更保守分数并减少 assertions 数量

commentary_text（博主自己的评论段，核心）:
{analysis_context.get("commentary_text", "")}

quoted_text（转发/引用上下文）:
{analysis_context.get("quoted_text", "")}

补充元信息:
{json.dumps(row, ensure_ascii=False)}
""".strip()

    resolved_api_mode = (api_mode or DEFAULT_AI_MODE).strip().lower()
    trace_label = (
        clean_text(row.get("id", "")) or clean_text(row.get("bid", "")) or "weibo"
    )
    parsed = _call_ai_with_openai(
        prompt=prompt,
        api_mode=resolved_api_mode,
        ai_stream=ai_stream,
        model_name=model,
        base_url=base_url,
        api_key=api_key,
        timeout_seconds=timeout_seconds,
        retry_count=ai_retries,
        temperature=ai_temperature,
        reasoning_effort=ai_reasoning_effort,
        trace_out=trace_out,
        trace_label=trace_label,
    )

    output = ai_analyze_output_from_parsed(parsed)

    return AnalyzeResult(
        status=output.status,
        invest_score=output.invest_score,
        assertions=[ai_assertion_to_draft(item) for item in output.assertions],
    )


def validate_and_adjust_assertions(
    assertions: list[AssertionDraft],
    commentary_text: str,
    quoted_text: str,
) -> list[AssertionDraft]:
    commentary = commentary_text or ""
    quoted = quoted_text or ""
    fallback_evidence = (
        commentary[:120] if commentary else (quoted[:120] if quoted else "")
    )

    fixed: list[AssertionDraft] = []
    for item in assertions:
        evidence = clean_text(item.evidence)
        summary = clean_text(item.summary or "未提供摘要") or "未提供摘要"
        confidence = clamp_float(item.confidence, 0.0, 1.0, 0.5)
        strength = clamp_int(item.action_strength, 0, 3, 1)
        source_type = clean_text(item.source_type or "commentary").lower()
        source_type = (
            source_type
            if source_type in {"commentary", "extension", "forward_only"}
            else "commentary"
        )

        if evidence and commentary and evidence in commentary:
            pass
        elif evidence and quoted and evidence in quoted:
            if source_type == "commentary":
                source_type = "forward_only"
            confidence = min(confidence, 0.45)
            strength = min(strength, 1)
            summary = f"[转发线索] {summary}"
        else:
            evidence = fallback_evidence
            confidence = min(confidence, 0.4)
            strength = min(strength, 1)
            summary = f"[弱证据] {summary}"

        fixed.append(
            AssertionDraft(
                topic_key=item.topic_key,
                action=item.action,
                action_strength=strength,
                summary=summary,
                evidence=evidence,
                confidence=confidence,
                source_type=source_type,
                stock_codes=item.stock_codes,
                stock_names=item.stock_names,
                industries=item.industries,
                commodities=item.commodities,
                indices=item.indices,
            )
        )
    return fixed


__all__ = [
    # constants
    "AI_MODE_COMPLETION",
    "AI_MODE_RESPONSES",
    "DEFAULT_MODEL",
    "DEFAULT_PROMPT_VERSION",
    "DEFAULT_AI_MODE",
    "DEFAULT_AI_TEMPERATURE",
    "DEFAULT_AI_RETRY_COUNT",
    "DEFAULT_AI_REASONING_EFFORT",
    "ALLOWED_ACTIONS",
    "LEGACY_ACTION_MAP",
    "AnalyzeResult",
    "analyze_with_openai",
    "validate_and_adjust_assertions",
    "normalize_action",
    "_call_ai_with_openai",
    "clean_text",
    "clamp_float",
    "clamp_int",
    "parse_json_text",
    "extract_llm_error_details",
    "format_llm_error_one_line",
]
