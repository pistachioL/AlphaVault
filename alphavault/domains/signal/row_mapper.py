from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Iterable

from alphavault.domains.common.json_list import parse_json_list
from alphavault.domains.signal.models import Assertion


def _coerce_text(value: object) -> str:
    return str(value or "").strip()


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = _coerce_text(value)
    if not text:
        return 0
    try:
        return int(text)
    except ValueError:
        return 0


def _coerce_float(value: object) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    text = _coerce_text(value)
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _coerce_datetime_or_text(value: object) -> datetime | str:
    if isinstance(value, datetime):
        return value
    return _coerce_text(value)


def _coerce_list(value: object) -> tuple[str, ...]:
    if isinstance(value, list):
        return tuple(str(item).strip() for item in value if str(item).strip())
    if isinstance(value, tuple):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return tuple(parse_json_list(value))


def assertion_from_row(row: dict[str, object]) -> Assertion:
    return Assertion(
        post_uid=_coerce_text(row.get("post_uid")),
        idx=_coerce_int(row.get("idx")),
        entity_key=_coerce_text(row.get("entity_key")),
        action=_coerce_text(row.get("action")),
        action_strength=_coerce_int(row.get("action_strength")),
        summary=_coerce_text(row.get("summary")),
        evidence=_coerce_text(row.get("evidence")),
        confidence=_coerce_float(row.get("confidence")),
        stock_codes=_coerce_list(row.get("stock_codes")),
        stock_names=_coerce_list(row.get("stock_names")),
        industries=_coerce_list(
            row.get("industries")
            if row.get("industries") is not None
            else row.get("industries_json")
        ),
        commodities=_coerce_list(
            row.get("commodities")
            if row.get("commodities") is not None
            else row.get("commodities_json")
        ),
        indices=_coerce_list(
            row.get("indices")
            if row.get("indices") is not None
            else row.get("indices_json")
        ),
        cluster_keys=_coerce_list(
            row.get("cluster_keys")
            if row.get("cluster_keys") is not None
            else row.get("cluster_keys_json")
        ),
        keywords=_coerce_list(
            row.get("keywords")
            if row.get("keywords") is not None
            else row.get("keywords_json")
        ),
        author=_coerce_text(row.get("author")),
        created_at=_coerce_datetime_or_text(row.get("created_at")),
        url=_coerce_text(row.get("url")),
        raw_text=_coerce_text(row.get("raw_text")),
        source=_coerce_text(row.get("source")),
        resolved_entity_key=_coerce_text(row.get("resolved_entity_key")),
        stock_key=_coerce_text(row.get("stock_key")),
    )


def map_assertions(rows: Iterable[dict[str, object]]) -> list[Assertion]:
    return [assertion_from_row(row) for row in rows]


def assertion_to_row(assertion: Assertion) -> dict[str, object]:
    row = asdict(assertion)
    row["stock_codes"] = list(assertion.stock_codes)
    row["stock_names"] = list(assertion.stock_names)
    row["industries"] = list(assertion.industries)
    row["commodities"] = list(assertion.commodities)
    row["indices"] = list(assertion.indices)
    row["cluster_keys"] = list(assertion.cluster_keys)
    row["keywords"] = list(assertion.keywords)
    return row


def map_assertion_rows(assertions: Iterable[Assertion]) -> list[dict[str, object]]:
    return [assertion_to_row(assertion) for assertion in assertions]


__all__ = [
    "assertion_from_row",
    "assertion_to_row",
    "map_assertion_rows",
    "map_assertions",
]
