from __future__ import annotations

from dataclasses import asdict
from datetime import datetime

from alphavault.domains.content.row_mapper import map_posts
from alphavault.domains.signal.aggregator import (
    MAX_SIGNAL_ROWS,
    attach_signal_tree_context,
    build_related_stock_rows as _build_related_stock_rows,
    coerce_signal_timestamp,
    default_signal_reference_time,
    filter_signals_for_sector,
    format_signal_created_at_line,
    format_signal_timestamp,
    merge_assertions_with_posts,
    sort_signals_by_created_at,
)
from alphavault.domains.signal.models import Signal
from alphavault.domains.signal.row_mapper import map_assertions
from alphavault.domains.signal.view_rows import build_signal_row_views


def merge_post_fields(
    assertions: list[dict[str, object]],
    posts: list[dict[str, object]],
) -> list[dict[str, object]]:
    post_models = map_posts(posts)
    assertion_models = map_assertions(assertions)
    signals = merge_assertions_with_posts(assertion_models, post_models)
    signals = attach_signal_tree_context(signals, posts=post_models)
    return [
        _merge_signal_row(original_row, signal)
        for original_row, signal in zip(assertions, signals)
    ]


def build_signal_rows(
    view: list[dict[str, object]],
    *,
    posts: list[dict[str, object]],
    now: datetime | None = None,
) -> list[dict[str, str]]:
    del posts
    signals = [_signal_from_row(row) for row in view]
    signals = sort_signals_by_created_at(signals)[:MAX_SIGNAL_ROWS]
    return build_signal_row_views(signals, now=now)


def build_related_stock_rows(view: list[dict[str, object]]) -> list[dict[str, str]]:
    return _build_related_stock_rows([_signal_from_row(row) for row in view])


def sector_filter_rows(
    assertions: list[dict[str, object]],
    sector_key: str,
) -> list[dict[str, object]]:
    signals = [_signal_from_row(row) for row in assertions]
    return [
        _signal_to_row(signal)
        for signal in filter_signals_for_sector(signals, sector_key=sector_key)
    ]


def _signal_from_row(row: dict[str, object]) -> Signal:
    created_at = _coerce_datetime_or_text(row.get("created_at"))
    return Signal(
        post_uid=str(row.get("post_uid") or "").strip(),
        entity_key=str(row.get("entity_key") or "").strip(),
        resolved_entity_key=str(row.get("resolved_entity_key") or "").strip(),
        stock_key=str(row.get("stock_key") or "").strip(),
        action=str(row.get("action") or "").strip(),
        action_strength=_coerce_int(row.get("action_strength")),
        summary=str(row.get("summary") or "").strip(),
        evidence=str(row.get("evidence") or "").strip(),
        confidence=_coerce_float(row.get("confidence")),
        author=str(row.get("author") or "").strip(),
        created_at=created_at,
        url=str(row.get("url") or "").strip(),
        raw_text=str(row.get("raw_text") or "").strip(),
        cluster_keys=tuple(_coerce_list(row.get("cluster_keys"))),
        tree_label=str(row.get("tree_label") or "").strip(),
        tree_text=str(row.get("tree_text") or "").strip(),
    )


def _signal_to_row(signal: Signal) -> dict[str, object]:
    row = asdict(signal)
    row["cluster_keys"] = list(signal.cluster_keys)
    return row


def _merge_signal_row(
    original_row: dict[str, object],
    signal: Signal,
) -> dict[str, object]:
    row = dict(original_row)
    row.update(_signal_to_row(signal))
    return row


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value or "").strip()
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
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _coerce_datetime_or_text(value: object) -> datetime | str:
    if isinstance(value, datetime):
        return value
    return str(value or "").strip()


def _coerce_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


__all__ = [
    "MAX_SIGNAL_ROWS",
    "build_related_stock_rows",
    "build_signal_rows",
    "coerce_signal_timestamp",
    "default_signal_reference_time",
    "format_signal_created_at_line",
    "format_signal_timestamp",
    "merge_post_fields",
    "sector_filter_rows",
]
