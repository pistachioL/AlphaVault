from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
import os
from typing import Any, Callable

import pandas as pd

from alphavault.ai.analyze import (
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_AI_RETRY_COUNT,
    DEFAULT_AI_TEMPERATURE,
    DEFAULT_MODEL,
    _call_ai_with_litellm,
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
from alphavault_reflex.services.stock_objects import (
    build_stock_object_index,
    filter_assertions_for_stock_object,
)


RELATION_LABEL_RELATED = "related"
RELATION_LABEL_PARENT_CHILD = "parent_child"
AI_STATUS_SKIPPED = "skipped"
AI_STATUS_RANKED = "ranked"
AI_STATUS_ERROR = "error"
STOCK_KEY_PREFIX = "stock:"
AI_RANK_TIMEOUT_SECONDS_CAP = 20.0
AI_RANK_RETRY_CAP = 1


@dataclass(frozen=True)
class AiRuntimeConfig:
    api_key: str
    model: str
    base_url: str
    api_mode: str
    temperature: float
    reasoning_effort: str
    timeout_seconds: float
    retries: int


def build_stock_alias_candidates(
    assertions: pd.DataFrame,
    *,
    stock_key: str,
) -> list[dict[str, str]]:
    target = str(stock_key or "").strip()
    if assertions.empty or not target or "topic_key" not in assertions.columns:
        return []

    stock_index = build_stock_object_index(assertions)
    entity_key = stock_index.resolve(target)
    if not entity_key:
        return []
    stock_view = filter_assertions_for_stock_object(
        assertions,
        stock_key=entity_key,
        stock_index=stock_index,
    )
    if stock_view.empty:
        return []
    member_keys = stock_index.member_keys_by_object_key.get(entity_key, set())

    alias_scores: Counter[str] = Counter()
    for member_key in member_keys:
        alias_key = str(member_key or "").strip()
        if not alias_key or alias_key == entity_key:
            continue
        topic_hits = (
            stock_view["topic_key"].astype(str).str.strip().eq(alias_key).sum()
            if "topic_key" in stock_view.columns
            else 0
        )
        if topic_hits:
            alias_scores[alias_key] += int(topic_hits)

    for _, row in stock_view.iterrows():
        for name in _coerce_list(row.get("stock_names")):
            alias_key = f"{STOCK_KEY_PREFIX}{name}"
            if alias_key != entity_key:
                alias_scores[alias_key] += 1

    return [
        {
            "candidate_key": alias_key,
            "alias_key": alias_key,
            "score": str(score),
            "evidence_summary": f"同票名称共现 {score} 次",
            "reason_code": "stock_alias_overlap",
        }
        for alias_key, score in alias_scores.most_common()
    ]


def build_stock_sector_candidates(
    assertions: pd.DataFrame,
    *,
    stock_key: str,
) -> list[dict[str, str]]:
    target = str(stock_key or "").strip()
    if assertions.empty or not target or "topic_key" not in assertions.columns:
        return []

    stock_index = build_stock_object_index(assertions)
    entity_key = stock_index.resolve(target)
    if not entity_key:
        return []
    stock_view = filter_assertions_for_stock_object(
        assertions,
        stock_key=entity_key,
        stock_index=stock_index,
    )
    if stock_view.empty:
        return []

    sector_scores: Counter[str] = Counter()
    for _, row in stock_view.iterrows():
        for sector_key in _row_sector_keys(row):
            sector_scores[sector_key] += 1

    ranked = sorted(sector_scores.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return [
        {
            "candidate_key": sector_key,
            "sector_key": sector_key,
            "score": str(score),
            "evidence_summary": f"该个股与板块共现 {score} 次",
            "reason_code": "stock_sector_overlap",
        }
        for sector_key, score in ranked
    ]


def build_sector_relation_candidates(
    assertions: pd.DataFrame,
    *,
    sector_key: str,
) -> list[dict[str, str]]:
    target = str(sector_key or "").strip()
    if assertions.empty or not target:
        return []

    stocks_by_sector: dict[str, set[str]] = defaultdict(set)
    for _, row in assertions.iterrows():
        topic_key = str(row.get("topic_key") or "").strip()
        if not topic_key.startswith(STOCK_KEY_PREFIX):
            continue
        for member_sector in _row_sector_keys(row):
            stocks_by_sector[member_sector].add(topic_key)

    base_stocks = stocks_by_sector.get(target, set())
    if not base_stocks:
        return []

    out: list[dict[str, str]] = []
    for candidate_key, stock_keys in stocks_by_sector.items():
        if candidate_key == target:
            continue
        overlap = len(base_stocks & stock_keys)
        if overlap <= 0:
            continue
        out.append(
            {
                "candidate_key": candidate_key,
                "sector_key": candidate_key,
                "score": str(overlap),
                "evidence_summary": f"相关个股重合 {overlap} 个",
                "reason_code": "sector_sector_stock_overlap",
            }
        )
    return sorted(out, key=lambda row: (-int(row["score"]), row["sector_key"]))


def classify_sector_relation_label(*, ai_enabled: bool, explanation: str) -> str:
    if not ai_enabled:
        return RELATION_LABEL_RELATED
    text = str(explanation or "").strip().lower()
    if "parent" in text or "child" in text or "上下级" in text:
        return RELATION_LABEL_PARENT_CHILD
    return RELATION_LABEL_RELATED


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
    config = _get_ai_runtime_config()
    candidate_lines = []
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
      "relation_label": "related|parent_child"
    }}
  ]
}}

规则：
- 只能使用候选列表里的 candidate_key，禁止编造。
- relation_type 不是 sector_sector 时，relation_label 固定填 related。
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
            row["relation_label"] = RELATION_LABEL_RELATED
        out.append(row)

    out.sort(key=lambda item: (int(item["_rank_index"]), -int(item.get("score") or 0)))
    for row in out:
        row.pop("_rank_index", None)
    return out


def _get_ai_runtime_config() -> AiRuntimeConfig:
    return AiRuntimeConfig(
        api_key=os.getenv(ENV_AI_API_KEY, "").strip(),
        model=os.getenv(ENV_AI_MODEL, DEFAULT_MODEL).strip() or DEFAULT_MODEL,
        base_url=os.getenv(ENV_AI_BASE_URL, "").strip(),
        api_mode=os.getenv(ENV_AI_API_MODE, DEFAULT_AI_MODE).strip() or DEFAULT_AI_MODE,
        temperature=float(
            os.getenv(ENV_AI_TEMPERATURE, str(DEFAULT_AI_TEMPERATURE)).strip()
            or DEFAULT_AI_TEMPERATURE
        ),
        reasoning_effort=os.getenv(
            ENV_AI_REASONING_EFFORT,
            DEFAULT_AI_REASONING_EFFORT,
        ).strip()
        or DEFAULT_AI_REASONING_EFFORT,
        timeout_seconds=float(os.getenv(ENV_AI_TIMEOUT_SEC, "1000").strip() or "1000"),
        retries=int(
            os.getenv(ENV_AI_RETRIES, str(DEFAULT_AI_RETRY_COUNT)).strip()
            or DEFAULT_AI_RETRY_COUNT
        ),
    )


def _row_sector_keys(row: pd.Series) -> list[str]:
    keys = _coerce_list(row.get("cluster_keys"))
    if keys:
        return keys
    raw_key = str(row.get("cluster_key") or "").strip()
    if raw_key:
        return [raw_key]
    return []


def _coerce_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []
