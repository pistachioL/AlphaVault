from __future__ import annotations

import json

from alphavault.db.turso_db import TursoEngine
from alphavault.db.turso_queue import CloudPost, upsert_pending_post
from alphavault.rss.utils import now_str
from alphavault.worker.runtime_models import _clamp_float, _clamp_int


def score_from_assertions(rows: list[dict[str, object]]) -> float:
    if not rows:
        return 0.0
    scores: list[float] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        confidence = _clamp_float(row.get("confidence", 0.0), 0.0, 1.0, 0.0)
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


def ensure_prefetched_post_persisted(
    *,
    engine: TursoEngine,
    post: CloudPost,
    archived_at: str,
    ingested_at: int,
) -> None:
    raw_text = str(post.raw_text or "")
    author = str(post.author or "")
    platform = str(post.platform or "").strip().lower() or "weibo"
    upsert_pending_post(
        engine,
        post_uid=str(post.post_uid or "").strip(),
        platform=platform,
        platform_post_id=str(post.platform_post_id or "").strip(),
        author=author,
        created_at=str(post.created_at or now_str()),
        url=str(post.url or "").strip(),
        raw_text=raw_text,
        archived_at=str(archived_at or now_str()),
        ingested_at=max(0, int(ingested_at)),
    )


__all__ = [
    "as_str_list",
    "ensure_prefetched_post_persisted",
    "json_to_str_list",
    "score_from_assertions",
]
