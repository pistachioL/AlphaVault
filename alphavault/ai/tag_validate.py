"""
AI output tag validation.

This module intentionally does NOT try to "auto-fix" ambiguous values.
If something is invalid, we fail fast so the caller can retry/regenerate.
"""

from __future__ import annotations

from typing import Iterable, Mapping

from alphavault.ai.analyze import ALLOWED_ACTIONS

ALLOWED_RELATION_TO_TOPIC = {"new", "follow", "repeat", "forward"}

ALLOWED_SOURCE_KINDS = {"status", "comment", "topic_post", "talk_reply"}

ALLOWED_MENTION_TYPES = {
    "stock_code",
    "stock_name",
    "stock_alias",
    "industry_name",
    "commodity_name",
    "index_name",
    "keyword",
}

_BAD_SEPARATORS = {"，", "、"}


class AiTagValidationError(RuntimeError):
    pass


def _clean_str(value: object) -> str:
    return str(value or "").strip()


def _short(value: object, *, max_len: int = 80) -> str:
    s = _clean_str(value)
    if not s:
        return ""
    if len(s) <= int(max_len):
        return s
    return s[: max(0, int(max_len) - 3)] + "..."


def _has_bad_separator(value: str, *, separators: set[str] | None = None) -> bool:
    active_separators = separators or _BAD_SEPARATORS
    return any(sep in value for sep in active_separators)


def _must(condition: bool, msg: str) -> None:
    if condition:
        return
    raise AiTagValidationError(msg)


def _validate_optional_str_list(
    value: object,
    *,
    field: str,
    max_items: int = 50,
    max_item_len: int = 60,
    reject_separators: bool = True,
) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise AiTagValidationError(f"{field}_not_list type={type(value).__name__}")
    out: list[str] = []
    for i, item in enumerate(value):
        s = _clean_str(item)
        _must(bool(s), f"{field}[{i}]_empty")
        _must(len(s) <= int(max_item_len), f"{field}[{i}]_too_long value={_short(s)}")
        if reject_separators:
            _must(
                not _has_bad_separator(s),
                f"{field}[{i}]_has_separator value={_short(s)}",
            )
        out.append(s)
        if len(out) >= int(max_items):
            break
    return out


def _validate_action(action: object) -> None:
    a = _clean_str(action)
    _must(bool(a), "action_empty")
    _must(a in ALLOWED_ACTIONS, f"action_invalid value={_short(a)}")


def _validate_int_range(value: object, *, field: str, low: int, high: int) -> None:
    try:
        v = int(_clean_str(value))
    except Exception as exc:
        raise AiTagValidationError(f"{field}_not_int value={_short(value)}") from exc
    _must(int(low) <= v <= int(high), f"{field}_out_of_range value={v}")


def _validate_float_range(
    value: object, *, field: str, low: float, high: float
) -> float:
    try:
        v = float(_clean_str(value))
    except Exception as exc:
        raise AiTagValidationError(f"{field}_not_float value={_short(value)}") from exc
    _must(float(low) <= v <= float(high), f"{field}_out_of_range value={v}")
    return v


def _validate_required_text(value: object, *, field: str, max_len: int = 400) -> str:
    text = _clean_str(value)
    _must(bool(text), f"{field}_empty")
    _must(len(text) <= int(max_len), f"{field}_too_long value={_short(text)}")
    return text


def _validate_relation_to_topic(value: object) -> str:
    relation = _clean_str(value)
    _must(bool(relation), "relation_to_topic_empty")
    _must(
        relation in ALLOWED_RELATION_TO_TOPIC,
        f"relation_to_topic_invalid value={_short(relation)}",
    )
    return relation


def _validate_source_kind(value: object, *, field: str) -> str:
    source_kind = _clean_str(value)
    _must(bool(source_kind), f"{field}_empty")
    _must(
        source_kind in ALLOWED_SOURCE_KINDS,
        f"{field}_invalid value={_short(source_kind)}",
    )
    return source_kind


def _validate_evidence_refs(value: object, *, field: str) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise AiTagValidationError(f"{field}_not_list")
    _must(bool(value), f"{field}_empty")
    refs: list[dict[str, str]] = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            raise AiTagValidationError(f"{field}[{idx}]_not_object")
        source_kind = _validate_source_kind(
            item.get("source_kind"), field=f"{field}[{idx}].source_kind"
        )
        source_id = _validate_required_text(
            item.get("source_id"),
            field=f"{field}[{idx}].source_id",
            max_len=120,
        )
        quote = _validate_required_text(
            item.get("quote"),
            field=f"{field}[{idx}].quote",
            max_len=240,
        )
        refs.append(
            {
                "source_kind": source_kind,
                "source_id": source_id,
                "quote": quote,
            }
        )
    return refs


def _validate_top_level_mention(
    mention: object,
    *,
    mention_index: int,
) -> dict[str, object]:
    if not isinstance(mention, dict):
        raise AiTagValidationError(f"mentions[{mention_index}]_not_object")
    mention_text = _validate_required_text(
        mention.get("mention_text"),
        field=f"mentions[{mention_index}].mention_text",
        max_len=80,
    )
    mention_type = _clean_str(mention.get("mention_type"))
    _must(
        mention_type in ALLOWED_MENTION_TYPES,
        f"mentions[{mention_index}].mention_type_invalid value={_short(mention_type)}",
    )
    evidence = _validate_required_text(
        mention.get("evidence"),
        field=f"mentions[{mention_index}].evidence",
        max_len=240,
    )
    confidence = _validate_float_range(
        mention.get("confidence"),
        field=f"mentions[{mention_index}].confidence",
        low=0.0,
        high=1.0,
    )
    return {
        "mention_text": mention_text,
        "mention_type": mention_type,
        "evidence": evidence,
        "confidence": confidence,
    }


def validate_post_context_ai_result(parsed: Mapping[str, object]) -> None:
    _validate_required_text(
        parsed.get("topic_status_id"),
        field="topic_status_id",
        max_len=120,
    )
    mentions_raw = parsed.get("mentions")
    if not isinstance(mentions_raw, list):
        raise AiTagValidationError("mentions_not_list")
    mention_texts: set[str] = set()
    for idx, raw_mention in enumerate(mentions_raw):
        mention = _validate_top_level_mention(raw_mention, mention_index=idx)
        mention_text = str(mention["mention_text"])
        _must(mention_text not in mention_texts, f"mentions[{idx}]_duplicate_text")
        mention_texts.add(mention_text)


def validate_topic_prompt_v4_ai_result(parsed: Mapping[str, object]) -> None:
    _validate_required_text(
        parsed.get("topic_status_id"), field="topic_status_id", max_len=120
    )
    _validate_required_text(
        parsed.get("topic_summary"), field="topic_summary", max_len=400
    )

    mentions_raw = parsed.get("mentions")
    if not isinstance(mentions_raw, list):
        raise AiTagValidationError("mentions_not_list")

    mention_map: dict[str, dict[str, object]] = {}
    for idx, raw_mention in enumerate(mentions_raw):
        mention = _validate_top_level_mention(raw_mention, mention_index=idx)
        mention_text = str(mention["mention_text"])
        _must(mention_text not in mention_map, f"mentions[{idx}]_duplicate_text")
        mention_map[mention_text] = mention

    assertions = parsed.get("assertions")
    if not isinstance(assertions, list):
        raise AiTagValidationError("assertions_not_list")
    for idx, raw_assertion in enumerate(assertions):
        validate_topic_prompt_v4_assertion(
            raw_assertion,
            assertion_index=idx,
            mention_map=mention_map,
        )


def validate_topic_prompt_v4_assertion(
    item: object,
    *,
    assertion_index: int,
    mention_map: Mapping[str, dict[str, object]],
) -> None:
    if not isinstance(item, dict):
        raise AiTagValidationError(f"assertions[{assertion_index}]_not_object")
    _validate_required_text(
        item.get("speaker"),
        field=f"assertions[{assertion_index}].speaker",
        max_len=80,
    )
    _validate_relation_to_topic(item.get("relation_to_topic"))
    _validate_action(item.get("action"))
    _validate_int_range(
        item.get("action_strength"),
        field=f"assertions[{assertion_index}].action_strength",
        low=0,
        high=3,
    )
    _validate_required_text(
        item.get("summary"),
        field=f"assertions[{assertion_index}].summary",
        max_len=240,
    )
    _validate_evidence_refs(
        item.get("evidence_refs"), field=f"assertions[{assertion_index}].evidence_refs"
    )
    mention_refs = _validate_optional_str_list(
        item.get("mentions"),
        field=f"assertions[{assertion_index}].mentions",
        max_item_len=80,
        reject_separators=False,
    )
    _must(bool(mention_refs), f"assertions[{assertion_index}].mentions_empty")
    for mention_text in mention_refs:
        _must(
            mention_text in mention_map,
            f"assertions[{assertion_index}].mention_not_defined value={_short(mention_text)}",
        )


def validate_assertion_row(row: dict[str, object], *, prompt_version: str) -> None:
    del prompt_version
    assertion_id = _validate_required_text(
        row.get("assertion_id"), field="assertion_id", max_len=160
    )
    post_uid = _validate_required_text(
        row.get("post_uid"), field="post_uid", max_len=120
    )
    idx_text = _clean_str(row.get("idx"))
    _must(bool(idx_text), "idx_empty")
    _validate_int_range(row.get("idx"), field="idx", low=1, high=1_000_000)
    idx = int(idx_text)
    _validate_action(row.get("action"))
    _validate_int_range(
        row.get("action_strength"), field="action_strength", low=0, high=3
    )
    _validate_required_text(row.get("summary"), field="summary", max_len=240)
    _validate_required_text(row.get("evidence"), field="evidence", max_len=400)
    _validate_required_text(row.get("created_at"), field="created_at", max_len=80)
    _must(
        assertion_id == f"{post_uid}#{idx}",
        f"assertion_id_mismatch value={_short(assertion_id)}",
    )


def validate_many_assertion_rows(
    rows: Iterable[dict[str, object]], *, prompt_version: str
) -> None:
    for row in rows:
        validate_assertion_row(row, prompt_version=prompt_version)


__all__ = [
    "AiTagValidationError",
    "validate_post_context_ai_result",
    "validate_topic_prompt_v4_ai_result",
    "validate_topic_prompt_v4_assertion",
    "validate_assertion_row",
    "validate_many_assertion_rows",
]
