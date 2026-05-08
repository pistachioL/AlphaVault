from __future__ import annotations

from typing import TypedDict

from alphavault.ai.analyze import _call_ai_with_openai, clean_text, clamp_float
from alphavault.capabilities.stock_analysis import StockEvidencePack
from alphavault.infra.ai.runtime_config import (
    AI_TASK_STOCK_SUMMARY,
    ai_task_runtime_config_from_env,
)

DEFAULT_STOCK_SUMMARY_TIMEOUT_SECONDS = 1000.0
_DEFAULT_SUMMARY_STANCE = "unclear"
_SUMMARY_STANCE_CHOICES = frozenset(("bullish", "bearish", "mixed", "watch", "unclear"))
_MAX_BULL_POINT_COUNT = 4
_MAX_BEAR_POINT_COUNT = 4
_MAX_RISK_COUNT = 5
_MAX_POINT_TEXT_LENGTH = 120
_MAX_CORE_DISPUTE_LENGTH = 200
_MAX_SUMMARY_TEXT_LENGTH = 360
_MAX_EVIDENCE_TEXT_LENGTH = 220
_MAX_TREE_TEXT_LENGTH = 280
_MAX_ACTION_COUNT_ROWS = 12
_MAX_STANCE_COUNT_ROWS = 8
_MAX_AUTHOR_ROWS = 8


StockAiSummary = TypedDict(
    "StockAiSummary",
    {
        "stance": str,
        "confidence": float,
        "bull_points": list[str],
        "bear_points": list[str],
        "core_dispute": str,
        "risks": list[str],
        "summary_text": str,
    },
)


def empty_stock_ai_summary() -> StockAiSummary:
    return {
        "stance": _DEFAULT_SUMMARY_STANCE,
        "confidence": 0.0,
        "bull_points": [],
        "bear_points": [],
        "core_dispute": "",
        "risks": [],
        "summary_text": "",
    }


def _trim_text(value: object, *, limit: int) -> str:
    text = clean_text(value)
    if len(text) <= limit:
        return text
    return text[: max(limit - 3, 0)].rstrip() + "..."


def _dedupe_text_list(
    value: object,
    *,
    max_items: int,
    item_limit: int,
) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = _trim_text(item, limit=item_limit)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
        if len(out) >= max_items:
            break
    return out


def _normalize_summary_stance(value: object) -> str:
    stance = clean_text(value).lower()
    if stance in _SUMMARY_STANCE_CHOICES:
        return stance
    return _DEFAULT_SUMMARY_STANCE


def _fallback_summary_text(summary: StockAiSummary) -> str:
    core_dispute = clean_text(summary.get("core_dispute"))
    if core_dispute:
        return core_dispute
    bull_points = summary.get("bull_points") or []
    bear_points = summary.get("bear_points") or []
    if bull_points:
        return bull_points[0]
    if bear_points:
        return bear_points[0]
    return "当前证据还不够形成明确结论。"


def _coerce_stock_ai_summary(parsed: object) -> StockAiSummary:
    if not isinstance(parsed, dict):
        raise RuntimeError("ai_invalid_json")
    summary: StockAiSummary = {
        "stance": _normalize_summary_stance(parsed.get("stance")),
        "confidence": clamp_float(parsed.get("confidence"), 0.0, 1.0, 0.0),
        "bull_points": _dedupe_text_list(
            parsed.get("bull_points"),
            max_items=_MAX_BULL_POINT_COUNT,
            item_limit=_MAX_POINT_TEXT_LENGTH,
        ),
        "bear_points": _dedupe_text_list(
            parsed.get("bear_points"),
            max_items=_MAX_BEAR_POINT_COUNT,
            item_limit=_MAX_POINT_TEXT_LENGTH,
        ),
        "core_dispute": _trim_text(
            parsed.get("core_dispute"),
            limit=_MAX_CORE_DISPUTE_LENGTH,
        ),
        "risks": _dedupe_text_list(
            parsed.get("risks"),
            max_items=_MAX_RISK_COUNT,
            item_limit=_MAX_POINT_TEXT_LENGTH,
        ),
        "summary_text": _trim_text(
            parsed.get("summary_text"),
            limit=_MAX_SUMMARY_TEXT_LENGTH,
        ),
    }
    if not summary["summary_text"]:
        summary["summary_text"] = _fallback_summary_text(summary)
    return summary


def _format_count_rows(
    rows: object,
    *,
    label_key: str,
    limit: int,
) -> str:
    if not isinstance(rows, list):
        return "（空）"
    lines: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        label = clean_text(row.get(label_key))
        count = clean_text(row.get("count"))
        if not label:
            continue
        lines.append(f"- {label}: {count or '0'}")
        if len(lines) >= limit:
            break
    return "\n".join(lines) if lines else "（空）"


def _format_top_authors(rows: object) -> str:
    if not isinstance(rows, list):
        return "（空）"
    lines: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        author = clean_text(row.get("author"))
        signal_count = clean_text(row.get("signal_count"))
        bullish_count = clean_text(row.get("bullish_count"))
        bearish_count = clean_text(row.get("bearish_count"))
        watch_count = clean_text(row.get("watch_count"))
        if not author:
            continue
        lines.append(
            f"- {author}: 总样本 {signal_count or '0'}，偏多 {bullish_count or '0'}，"
            f"偏空 {bearish_count or '0'}，观察 {watch_count or '0'}"
        )
        if len(lines) >= _MAX_AUTHOR_ROWS:
            break
    return "\n".join(lines) if lines else "（空）"


def _format_evidence_rows(rows: object) -> str:
    if not isinstance(rows, list):
        return "（空）"
    lines: list[str] = []
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        created_at = clean_text(row.get("created_at"))
        author = clean_text(row.get("author"))
        stance = clean_text(row.get("stance"))
        action = clean_text(row.get("action"))
        action_strength = clean_text(row.get("action_strength"))
        summary = _trim_text(row.get("summary"), limit=_MAX_EVIDENCE_TEXT_LENGTH)
        raw_text = _trim_text(row.get("raw_text"), limit=_MAX_EVIDENCE_TEXT_LENGTH)
        tree_text = _trim_text(row.get("tree_text"), limit=_MAX_TREE_TEXT_LENGTH)
        trade_review = row.get("trade_review") if isinstance(row, dict) else {}
        review_status = clean_text(
            trade_review.get("review_status") if isinstance(trade_review, dict) else ""
        )
        position_phase = clean_text(
            trade_review.get("position_phase") if isinstance(trade_review, dict) else ""
        )
        copyability = clean_text(
            trade_review.get("copyability") if isinstance(trade_review, dict) else ""
        )
        reason_text = _trim_text(
            trade_review.get("reason_text") if isinstance(trade_review, dict) else "",
            limit=_MAX_EVIDENCE_TEXT_LENGTH,
        )
        lines.append(
            f"{index}. 时间={created_at or '未知'} | 作者={author or '未知'} | "
            f"立场={stance or '未知'} | 动作={action or '未知'} | 强度={action_strength or '0'}\n"
            f"   审查：状态={review_status or '未知'} | 阶段={position_phase or '未知'} | "
            f"可复制性={copyability or '未知'}\n"
            f"   审查依据：{reason_text or '（空）'}\n"
            f"   摘要：{summary or '（空）'}\n"
            f"   原文：{raw_text or '（空）'}\n"
            f"   对话：{tree_text or '（空）'}"
        )
    return "\n".join(lines) if lines else "（空）"


def _build_stock_summary_prompt(pack: StockEvidencePack) -> str:
    requested_stock = clean_text(pack.get("requested_stock"))
    resolved_stock_key = clean_text(pack.get("resolved_stock_key"))
    page_title = clean_text(pack.get("page_title"))
    covered_stock_keys = ", ".join(pack.get("covered_stock_keys") or []) or "（空）"
    same_company_rows = pack.get("same_company_stocks") or []
    same_company_text = (
        "、".join(
            clean_text(row.get("label"))
            for row in same_company_rows
            if isinstance(row, dict) and clean_text(row.get("label"))
        )
        or "（空）"
    )
    return f"""
你是股票研究总结助手。你会读取系统压缩后的公司级证据包，只根据输入证据给出单票研究摘要。

研究对象：
- requested_stock: {requested_stock}
- resolved_stock_key: {resolved_stock_key}
- page_title: {page_title}
- covered_stock_keys: {covered_stock_keys}
- same_company_stocks: {same_company_text}
- window_days: {int(pack.get("window_days") or 0)}
- signal_total: {int(pack.get("signal_total") or 0)}
- sampled_signal_total: {int(pack.get("sampled_signal_total") or 0)}
- evidence_row_total: {int(pack.get("evidence_row_total") or 0)}
- author_total: {int(pack.get("author_total") or 0)}
- latest_created_at: {clean_text(pack.get("latest_created_at")) or "（空）"}
- controversy_score: {float(pack.get("controversy_score") or 0.0):.1f}

动作分布：
{_format_count_rows(pack.get("action_counts"), label_key="action", limit=_MAX_ACTION_COUNT_ROWS)}

立场分布：
{_format_count_rows(pack.get("stance_counts"), label_key="stance", limit=_MAX_STANCE_COUNT_ROWS)}

重点作者：
{_format_top_authors(pack.get("top_authors"))}

压缩证据行：
{_format_evidence_rows(pack.get("evidence_rows"))}

请输出严格 JSON（不要 Markdown），结构：
{{
  "stance": "bullish|bearish|mixed|watch|unclear",
  "confidence": 0.0,
  "bull_points": ["..."],
  "bear_points": ["..."],
  "core_dispute": "...",
  "risks": ["..."],
  "summary_text": "..."
}}

规则：
- 只根据输入证据作答，不要补充外部事实，不要编造财务数字。
- `bull_points` 总结偏多逻辑，最多 {_MAX_BULL_POINT_COUNT} 条。
- `bear_points` 总结偏空或等待更好位置的逻辑，最多 {_MAX_BEAR_POINT_COUNT} 条。
- `risks` 写可能影响持仓判断的风险或不确定性，最多 {_MAX_RISK_COUNT} 条。
- `core_dispute` 用一句话概括主要分歧。
- `summary_text` 用 2 到 4 句中文，先给结论，再交代主要分歧与观察重点。
- 证据同时存在明确偏多和偏空时，优先输出 `mixed`。
- 证据主要是持有、观察、等待时，输出 `watch`。
- 证据不足时，输出 `unclear`，并降低 `confidence`。
""".strip()


def summarize_stock_evidence_pack(
    pack: StockEvidencePack,
    *,
    timeout_seconds: float | None = None,
    retries: int | None = None,
) -> StockAiSummary:
    config = ai_task_runtime_config_from_env(
        task_key=AI_TASK_STOCK_SUMMARY,
        timeout_seconds_default=DEFAULT_STOCK_SUMMARY_TIMEOUT_SECONDS,
    )
    if not clean_text(config.api_key):
        raise RuntimeError("ai_not_configured")
    effective_timeout_seconds = (
        float(timeout_seconds)
        if timeout_seconds is not None
        else float(config.timeout_seconds)
    )
    effective_timeout_seconds = max(1.0, effective_timeout_seconds)
    effective_retries = int(retries) if retries is not None else int(config.retries)
    effective_retries = max(0, effective_retries)
    trace_key = clean_text(pack.get("resolved_stock_key")) or clean_text(
        pack.get("requested_stock")
    )
    parsed = _call_ai_with_openai(
        prompt=_build_stock_summary_prompt(pack),
        api_mode=config.api_mode,
        ai_stream=False,
        model_name=config.model,
        base_url=config.base_url,
        api_key=config.api_key,
        timeout_seconds=float(effective_timeout_seconds),
        retry_count=int(effective_retries),
        temperature=float(config.temperature),
        reasoning_effort=str(config.reasoning_effort),
        trace_out=None,
        trace_label=f"stock_summary:{trace_key or 'stock'}",
    )
    return _coerce_stock_ai_summary(parsed)


__all__ = [
    "DEFAULT_STOCK_SUMMARY_TIMEOUT_SECONDS",
    "StockAiSummary",
    "empty_stock_ai_summary",
    "summarize_stock_evidence_pack",
]
