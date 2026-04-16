from __future__ import annotations

from dataclasses import dataclass, field

from alphavault.db.postgres_db import PostgresConnection, PostgresEngine
from alphavault.domains.common.assertion_entities import (
    build_assertion_entities,
    coerce_stock_code_entity_key,
)
from alphavault.infra.entity_match_redis import load_stock_dict_targets_best_effort
from alphavault.domains.relation.ids import make_candidate_id
from alphavault.research_workbench import (
    ALIAS_TASK_STATUS_BLOCKED,
    ALIAS_TASK_STATUS_MANUAL,
    ALIAS_TASK_STATUS_PENDING,
    ALIAS_TASK_STATUS_RESOLVED,
    AliasResolveTaskInfo,
    RESEARCH_RELATIONS_TABLE,
    get_stock_keys_by_official_names,
    get_alias_resolve_tasks_map,
    set_alias_resolve_task_status,
    upsert_relation_candidate,
)
from alphavault.research_workbench.schema import use_conn

_STOCK_ENTITY_TYPE = "stock"
_STOCK_ALIAS_RELATION_TYPE = "stock_alias"
_STOCK_ALIAS_RELATION_LABEL = "alias_of"
_AI_STATUS_SKIPPED = "skipped"
_RESOLVED_ALIAS_TASK_STATUSES = {
    ALIAS_TASK_STATUS_PENDING,
    ALIAS_TASK_STATUS_MANUAL,
    ALIAS_TASK_STATUS_BLOCKED,
    ALIAS_TASK_STATUS_RESOLVED,
}


@dataclass(frozen=True)
class EntityMatchResult:
    entities: list[dict[str, object]]
    relation_candidates: list[dict[str, object]]
    alias_task_keys: list[str]
    alias_task_samples: list[dict[str, str]] = field(default_factory=list)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _clamp_confidence(value: object) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return max(0.0, min(float(value), 1.0))
    try:
        return max(0.0, min(float(str(value or "").strip() or "0"), 1.0))
    except (TypeError, ValueError):
        return 0.0


def _clip_text(value: object, *, limit: int) -> str:
    text = _clean_text(value)
    if len(text) <= limit:
        return text
    return text[: max(0, int(limit))].rstrip()


def _clean_alias_task_sample(
    sample: dict[str, object] | None,
) -> dict[str, str]:
    raw = sample if isinstance(sample, dict) else {}
    return {
        "sample_post_uid": _clean_text(raw.get("sample_post_uid")),
        "sample_evidence": _clip_text(raw.get("sample_evidence"), limit=120),
        "sample_raw_text_excerpt": _clip_text(
            raw.get("sample_raw_text_excerpt"),
            limit=220,
        ),
    }


def _has_alias_task_sample(
    info: AliasResolveTaskInfo | dict[str, object] | None,
) -> bool:
    if not isinstance(info, dict):
        return False
    return any(
        _clean_text(info.get(key))
        for key in (
            "sample_post_uid",
            "sample_evidence",
            "sample_raw_text_excerpt",
        )
    )


def _clean_mentions(
    assertion_mentions: list[dict[str, object]],
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for item in assertion_mentions:
        if not isinstance(item, dict):
            continue
        mention_text = _clean_text(item.get("mention_text"))
        mention_type = _clean_text(item.get("mention_type"))
        if not mention_text or not mention_type:
            continue
        out.append(
            {
                "mention_text": mention_text,
                "mention_type": mention_type,
                "confidence": _clamp_confidence(item.get("confidence")),
            }
        )
    return out


def _unique_texts(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = _clean_text(raw)
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _single_stock_code_key(cleaned_mentions: list[dict[str, object]]) -> str:
    stock_code_keys = [
        coerce_stock_code_entity_key(item.get("mention_text"))
        for item in cleaned_mentions
        if str(item.get("mention_type") or "").strip() == "stock_code"
    ]
    unique_keys = [key for key in dict.fromkeys(stock_code_keys) if _clean_text(key)]
    return unique_keys[0] if len(unique_keys) == 1 else ""


def _entity_dedupe_key(item: dict[str, object]) -> tuple[str, str, str]:
    return (
        _clean_text(item.get("entity_key")),
        _clean_text(item.get("entity_type")),
        _clean_text(item.get("match_source")),
    )


def _append_entity(
    out: list[dict[str, object]],
    *,
    seen: set[tuple[str, str, str]],
    entity_key: str,
    entity_type: str,
    match_source: str,
) -> None:
    item = {
        "entity_key": entity_key,
        "entity_type": entity_type,
        "match_source": match_source,
        "is_primary": 0,
    }
    dedupe_key = _entity_dedupe_key(item)
    if not dedupe_key[0] or not dedupe_key[1] or dedupe_key in seen:
        return
    seen.add(dedupe_key)
    out.append(item)


def _has_entity(
    entities: list[dict[str, object]],
    *,
    entity_key: str,
    entity_type: str,
) -> bool:
    wanted_key = _clean_text(entity_key)
    wanted_type = _clean_text(entity_type)
    return any(
        _clean_text(item.get("entity_key")) == wanted_key
        and _clean_text(item.get("entity_type")) == wanted_type
        for item in entities
    )


def _mark_primary_entity(entities: list[dict[str, object]]) -> None:
    if not entities:
        return
    for idx, entity in enumerate(entities):
        entity["is_primary"] = 1 if idx == 0 else 0


def _select_unique_mapping(
    rows: list[tuple[str, str]],
) -> dict[str, str]:
    values_by_text: dict[str, set[str]] = {}
    for raw_text, raw_entity_key in rows:
        text = _clean_text(raw_text)
        entity_key = _clean_text(raw_entity_key)
        if not text or not entity_key:
            continue
        values_by_text.setdefault(text, set()).add(entity_key)
    return {
        text: next(iter(entity_keys))
        for text, entity_keys in values_by_text.items()
        if len(entity_keys) == 1
    }


def _load_confirmed_alias_targets(
    engine_or_conn,
    *,
    mention_texts: list[str],
) -> dict[str, str]:
    cleaned = [_clean_text(item) for item in mention_texts if _clean_text(item)]
    if not cleaned:
        return {}
    placeholders = ", ".join(["?"] * len(cleaned))
    sql = f"""
SELECT right_key, left_key
FROM {RESEARCH_RELATIONS_TABLE}
WHERE relation_type = '{_STOCK_ALIAS_RELATION_TYPE}'
  AND relation_label = '{_STOCK_ALIAS_RELATION_LABEL}'
  AND right_key IN ({placeholders})
"""
    rows: list[tuple[str, str]] = []
    with use_conn(engine_or_conn) as conn:
        try:
            raw_rows = conn.execute(
                sql,
                [f"stock:{text}" for text in cleaned],
            ).fetchall()
        except BaseException:
            return {}
    for row in raw_rows:
        if not row:
            continue
        right_key = _clean_text(row[0])
        left_key = _clean_text(row[1])
        if not right_key or not left_key or not right_key.startswith("stock:"):
            continue
        rows.append((right_key[len("stock:") :], left_key))
    return _select_unique_mapping(rows)


def _build_alias_candidate(
    *,
    stock_key: str,
    alias_key: str,
    confidence: float,
) -> dict[str, object]:
    return {
        "candidate_id": make_candidate_id(
            relation_type=_STOCK_ALIAS_RELATION_TYPE,
            left_key=stock_key,
            right_key=alias_key,
            relation_label=_STOCK_ALIAS_RELATION_LABEL,
        ),
        "relation_type": _STOCK_ALIAS_RELATION_TYPE,
        "left_key": stock_key,
        "right_key": alias_key,
        "relation_label": _STOCK_ALIAS_RELATION_LABEL,
        "suggestion_reason": "同条观点里代码和简称一起出现",
        "evidence_summary": "同条观点里代码和简称一起出现",
        "score": float(confidence),
        "ai_status": _AI_STATUS_SKIPPED,
    }


def _should_load_redis_shadow(
    engine_or_conn: object,
) -> bool:
    if isinstance(engine_or_conn, PostgresConnection):
        return engine_or_conn._pool is not None
    return isinstance(engine_or_conn, PostgresEngine)


def load_entity_match_lookup_maps(
    engine_or_conn,
    *,
    stock_name_texts: list[str],
    stock_alias_texts: list[str],
) -> tuple[dict[str, str], dict[str, str]]:
    unique_stock_names = _unique_texts(stock_name_texts)
    unique_stock_aliases = _unique_texts(stock_alias_texts)
    stock_like_texts = _unique_texts([*unique_stock_names, *unique_stock_aliases])
    redis_stock_names: dict[str, str] = {}
    redis_stock_aliases: dict[str, str] = {}
    if _should_load_redis_shadow(engine_or_conn):
        redis_stock_names, redis_stock_aliases = load_stock_dict_targets_best_effort(
            official_names=unique_stock_names,
            alias_texts=stock_like_texts,
        )

    stock_name_targets = dict(redis_stock_names)
    stock_alias_targets = dict(redis_stock_aliases)

    missing_stock_names = [
        text for text in unique_stock_names if text not in stock_name_targets
    ]
    if missing_stock_names:
        stock_name_targets.update(
            get_stock_keys_by_official_names(
                engine_or_conn,
                missing_stock_names,
            )
        )

    alias_candidate_texts = _unique_texts(
        [
            *unique_stock_aliases,
            *[text for text in unique_stock_names if text not in stock_name_targets],
        ]
    )
    missing_stock_aliases = [
        text for text in alias_candidate_texts if text not in stock_alias_targets
    ]
    if missing_stock_aliases:
        stock_alias_targets.update(
            _load_confirmed_alias_targets(
                engine_or_conn,
                mention_texts=missing_stock_aliases,
            )
        )

    return stock_name_targets, stock_alias_targets


def resolve_assertion_mentions(
    engine_or_conn,
    *,
    assertion_mentions: list[dict[str, object]],
    stock_name_targets: dict[str, str] | None = None,
    stock_alias_targets: dict[str, str] | None = None,
    alias_task_sample: dict[str, object] | None = None,
) -> EntityMatchResult:
    cleaned_mentions = _clean_mentions(assertion_mentions)
    if not cleaned_mentions:
        return EntityMatchResult([], [], [])

    base_entities = build_assertion_entities(cleaned_mentions)
    entities: list[dict[str, object]] = []
    seen_entities: set[tuple[str, str, str]] = set()
    for item in base_entities:
        if _clean_text(item.get("match_source")) == "stock_name":
            continue
        _append_entity(
            entities,
            seen=seen_entities,
            entity_key=_clean_text(item.get("entity_key")),
            entity_type=_clean_text(item.get("entity_type")),
            match_source=_clean_text(item.get("match_source")),
        )

    single_stock_code_key = _single_stock_code_key(cleaned_mentions)
    stock_name_texts = [
        _clean_text(item.get("mention_text"))
        for item in cleaned_mentions
        if _clean_text(item.get("mention_type")) == "stock_name"
    ]
    stock_alias_texts = [
        _clean_text(item.get("mention_text"))
        for item in cleaned_mentions
        if _clean_text(item.get("mention_type")) == "stock_alias"
    ]
    if stock_name_targets is None or stock_alias_targets is None:
        loaded_stock_name_targets, loaded_stock_alias_targets = (
            load_entity_match_lookup_maps(
                engine_or_conn,
                stock_name_texts=stock_name_texts,
                stock_alias_texts=stock_alias_texts,
            )
        )
        if stock_name_targets is None:
            stock_name_targets = loaded_stock_name_targets
        if stock_alias_targets is None:
            stock_alias_targets = loaded_stock_alias_targets

    relation_candidates: list[dict[str, object]] = []
    seen_candidate_ids: set[str] = set()
    alias_task_keys: list[str] = []
    alias_task_samples: list[dict[str, str]] = []
    seen_alias_task_keys: set[str] = set()
    cleaned_alias_task_sample = _clean_alias_task_sample(alias_task_sample)

    for item in cleaned_mentions:
        mention_text = _clean_text(item.get("mention_text"))
        mention_type = _clean_text(item.get("mention_type"))
        confidence = _clamp_confidence(item.get("confidence"))
        if mention_type not in {"stock_name", "stock_alias"}:
            continue

        already_resolved = any(
            _clean_text(entity.get("match_source")) == mention_type
            for entity in entities
        )
        if already_resolved:
            continue

        target_key = ""
        if mention_type == "stock_name":
            target_key = _clean_text(
                stock_name_targets.get(mention_text)
                or stock_alias_targets.get(mention_text)
            )
        else:
            target_key = _clean_text(stock_alias_targets.get(mention_text))

        if target_key:
            if _has_entity(
                entities,
                entity_key=target_key,
                entity_type=_STOCK_ENTITY_TYPE,
            ):
                continue
            _append_entity(
                entities,
                seen=seen_entities,
                entity_key=target_key,
                entity_type=_STOCK_ENTITY_TYPE,
                match_source=mention_type,
            )
            continue

        alias_key = f"stock:{mention_text}"
        if single_stock_code_key:
            candidate = _build_alias_candidate(
                stock_key=single_stock_code_key,
                alias_key=alias_key,
                confidence=confidence,
            )
            candidate_id = _clean_text(candidate.get("candidate_id"))
            if candidate_id and candidate_id not in seen_candidate_ids:
                seen_candidate_ids.add(candidate_id)
                relation_candidates.append(candidate)
            continue

        if alias_key not in seen_alias_task_keys:
            seen_alias_task_keys.add(alias_key)
            alias_task_keys.append(alias_key)
            alias_task_samples.append(
                {
                    "alias_key": alias_key,
                    **cleaned_alias_task_sample,
                }
            )

    alias_task_keys.sort()
    _mark_primary_entity(entities)
    return EntityMatchResult(
        entities=entities,
        relation_candidates=relation_candidates,
        alias_task_keys=alias_task_keys,
        alias_task_samples=alias_task_samples,
    )


def persist_entity_match_followups(engine_or_conn, result: EntityMatchResult) -> None:
    for item in result.relation_candidates:
        upsert_relation_candidate(
            engine_or_conn,
            candidate_id=_clean_text(item.get("candidate_id")),
            relation_type=_clean_text(item.get("relation_type")),
            left_key=_clean_text(item.get("left_key")),
            right_key=_clean_text(item.get("right_key")),
            relation_label=_clean_text(item.get("relation_label")),
            suggestion_reason=_clean_text(item.get("suggestion_reason")),
            evidence_summary=_clean_text(item.get("evidence_summary")),
            score=_clamp_confidence(item.get("score")),
            ai_status=_clean_text(item.get("ai_status")) or _AI_STATUS_SKIPPED,
        )

    tasks_map = get_alias_resolve_tasks_map(engine_or_conn, result.alias_task_keys)
    sample_map = {
        _clean_text(item.get("alias_key")): item
        for item in result.alias_task_samples
        if _clean_text(item.get("alias_key"))
    }
    for alias_key in result.alias_task_keys:
        info = tasks_map.get(alias_key)
        if (
            info
            and _clean_text(info.get("status")) in _RESOLVED_ALIAS_TASK_STATUSES
            and _has_alias_task_sample(info)
        ):
            continue
        sample = sample_map.get(alias_key, {})
        status = (
            _clean_text(info.get("status"))
            if isinstance(info, dict)
            else ALIAS_TASK_STATUS_PENDING
        )
        set_alias_resolve_task_status(
            engine_or_conn,
            alias_key=alias_key,
            status=status or ALIAS_TASK_STATUS_PENDING,
            attempt_count=0,
            sample_post_uid=_clean_text(sample.get("sample_post_uid")),
            sample_evidence=_clean_text(sample.get("sample_evidence")),
            sample_raw_text_excerpt=_clean_text(sample.get("sample_raw_text_excerpt")),
        )


__all__ = [
    "EntityMatchResult",
    "load_entity_match_lookup_maps",
    "persist_entity_match_followups",
    "resolve_assertion_mentions",
]
