from __future__ import annotations

from typing import Any, Dict

from alphavault.ai.analyze import _call_ai_with_litellm, clean_text
from alphavault.infra.ai.runtime_config import (
    AI_TASK_FOLLOW_KEYWORDS_SUGGEST,
    ai_task_runtime_config_from_env,
)


def suggest_keywords_for_follow(
    *,
    follow_key: str,
    follow_label: str,
    seed_keywords: list[str],
    example_texts: list[str],
    max_keywords: int = 12,
    timeout_seconds: float | None = None,
    retries: int | None = None,
) -> Dict[str, Any]:
    """
    Ask AI to suggest OR keywords for a follow page.

    Output JSON:
    {"keywords": ["..."], "note": "..."}
    """
    config = ai_task_runtime_config_from_env(
        task_key=AI_TASK_FOLLOW_KEYWORDS_SUGGEST,
        timeout_seconds_default=1000.0,
    )
    if not str(config.api_key or "").strip():
        raise RuntimeError("ai_not_configured")

    follow_key = clean_text(follow_key)
    follow_label = clean_text(follow_label) or follow_key

    seed = [clean_text(x) for x in (seed_keywords or [])]
    seed = [x for x in seed if x]
    seed_text = ", ".join(seed[:30])

    examples = [clean_text(x) for x in (example_texts or [])]
    examples = [x for x in examples if x]
    examples = examples[:12]
    examples_text = "\n".join([f"- {x}" for x in examples])

    effective_timeout_seconds = (
        float(timeout_seconds)
        if timeout_seconds is not None
        else float(config.timeout_seconds)
    )
    effective_timeout_seconds = max(1.0, effective_timeout_seconds)

    effective_retries = int(retries) if retries is not None else int(config.retries)
    effective_retries = max(0, effective_retries)

    prompt = f"""
你是关键字助手。我要做一个“关注页”，用关键字 OR 来补漏（AI 可能漏打标签、也可能写简称/外号）。

关注对象：
- follow_key: {follow_key}
- follow_label: {follow_label}

已有种子关键字（可能不全）：
{seed_text or "（空）"}

下面是一些原文例子（可能包含简称/代码/外号）：
{examples_text or "（空）"}

请输出严格 JSON（不要 Markdown），结构：
{{
  "keywords": ["..."],
  "note": "一句话说明"
}}

规则：
- keywords 用“普通字符串”，不要正则，不要加括号/|/.* 这种符号。
- 词尽量短但别太短（不要 1 个字）。
- 目标是：少而准（一般 5~{int(max_keywords)} 个就够）。
- 去重；不要把 seed_keywords 原样重复一堆。
""".strip()

    parsed = _call_ai_with_litellm(
        prompt=prompt,
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
        trace_label=f"follow_keywords:{follow_key}",
    )
    if not isinstance(parsed, dict):
        raise RuntimeError("ai_invalid_json")

    kws = parsed.get("keywords")
    if not isinstance(kws, list):
        kws = []
    keywords = [clean_text(x) for x in kws if clean_text(x)]
    # keep small + unique
    out_keywords: list[str] = []
    seen: set[str] = set()
    for kw in keywords:
        if kw in seen:
            continue
        seen.add(kw)
        out_keywords.append(kw)
        if len(out_keywords) >= int(max(1, max_keywords)):
            break

    note = clean_text(parsed.get("note", ""))
    return {"keywords": out_keywords, "note": note}


__all__ = ["suggest_keywords_for_follow"]
