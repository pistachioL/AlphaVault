from __future__ import annotations

from typing import Any, Dict, Iterable, List

from alphavault.ai.analyze import _call_ai_with_openai, clean_text
from alphavault.infra.ai.runtime_config import (
    AI_TASK_TOPIC_CLUSTER_SUGGEST,
    ai_task_runtime_config_from_env,
    ai_task_runtime_config_is_configured,
    ai_task_runtime_config_summary,
)


def ai_is_configured() -> tuple[bool, str]:
    return ai_task_runtime_config_is_configured(
        task_key=AI_TASK_TOPIC_CLUSTER_SUGGEST,
        timeout_seconds_default=1000.0,
    )


def get_ai_config_summary() -> tuple[Dict[str, Any], str]:
    return ai_task_runtime_config_summary(
        task_key=AI_TASK_TOPIC_CLUSTER_SUGGEST,
        timeout_seconds_default=1000.0,
    )


def _format_topic_candidates(topics: Iterable[dict]) -> str:
    lines: List[str] = []
    for item in topics:
        key = clean_text(item.get("key", "")) or clean_text(item.get("topic_key", ""))
        if not key:
            continue
        count = item.get("count", None)
        hint = clean_text(item.get("hint", ""))
        hint_part = f" hint={hint}" if hint else ""
        if count is None:
            lines.append(f"- {key}{hint_part}")
        else:
            lines.append(f"- {key} (count={int(count)}){hint_part}")
    return "\n".join(lines).strip()


def suggest_keys_for_cluster(
    *,
    cluster_name: str,
    description: str,
    candidates: List[Dict[str, Any]],
    max_items_per_list: int = 200,
    timeout_seconds: float | None = None,
    retries: int | None = None,
) -> Dict[str, Any]:
    """
    Ask AI to classify key candidates into include/unsure.

    candidates: list of {key, count}
    Returns a parsed JSON dict from the model.
    """
    config = ai_task_runtime_config_from_env(
        task_key=AI_TASK_TOPIC_CLUSTER_SUGGEST,
        timeout_seconds_default=1000.0,
    )
    if not str(config.api_key or "").strip():
        raise RuntimeError("ai_not_configured")

    effective_timeout_seconds = (
        float(timeout_seconds)
        if timeout_seconds is not None
        else float(config.timeout_seconds)
    )
    effective_timeout_seconds = max(1.0, effective_timeout_seconds)

    effective_retries = int(retries) if retries is not None else int(config.retries)
    effective_retries = max(0, effective_retries)

    cluster_name = clean_text(cluster_name)
    description = clean_text(description)
    candidates_text = _format_topic_candidates(candidates)

    prompt = f"""
你是分类助手。我要做一个板块：{cluster_name}
说明：{description or "（空）"}

下面是系统已有的 key 候选列表（后面可能带 count/hint）。
请你从候选中筛选：哪些 key 属于这个板块。

输出严格 JSON（不要 Markdown），结构如下：
{{
  "include_keys": [{{"key": "...", "confidence": 0.0, "reason": "..."}}],
  "unsure_keys": [{{"key": "...", "confidence": 0.0, "reason": "..."}}],
}}

规则：
- key 只能从候选列表里选，禁止编造。
- include_keys 要保守；不确定就放 unsure_keys。
- 如果 key 是类似 stock:601225.SH 这种“代码”，请优先参考候选里的 hint（比如 stock_name/industry）。没有 hint 就更保守，放 unsure_keys。
- confidence 范围 0~1。
- include_keys / unsure_keys 每个最多 {int(max_items_per_list)} 条。

候选 key：
{candidates_text}
""".strip()

    parsed = _call_ai_with_openai(
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
        trace_label=f"cluster_suggest:{cluster_name}",
    )
    if not isinstance(parsed, dict):
        raise RuntimeError("ai_invalid_json")
    return parsed


def suggest_topics_for_cluster(*args, **kwargs) -> Dict[str, Any]:
    # Backward-compatible alias (older UI code). Prefer suggest_keys_for_cluster().
    return suggest_keys_for_cluster(*args, **kwargs)


__all__ = [
    "ai_is_configured",
    "get_ai_config_summary",
    "suggest_keys_for_cluster",
    "suggest_topics_for_cluster",
]
