from __future__ import annotations

import json

from alphavault.worker.runtime_models import _clamp_float, _clamp_int


def score_from_assertions(rows: list[dict[str, object]]) -> float:
    if not rows:
        return 0.0
    scores: list[float] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_mentions = row.get("assertion_mentions")
        mentions = raw_mentions if isinstance(raw_mentions, list) else []
        mention_values = [
            _clamp_float(item.get("confidence", 0.0), 0.0, 1.0, 0.0)
            for item in mentions
            if isinstance(item, dict)
        ]
        confidence = max(mention_values) if mention_values else 0.5
        strength = _clamp_int(row.get("action_strength", 1), 0, 3, 1)
        strength_weight = strength / 3.0
        scores.append(0.7 * confidence + 0.3 * strength_weight)
    return max(scores) if scores else 0.0


def as_str_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    return []


def json_to_str_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


__all__ = [
    "as_str_list",
    "json_to_str_list",
    "score_from_assertions",
]
