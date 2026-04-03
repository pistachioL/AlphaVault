from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypedDict

from alphavault_reflex.services.research_data import _coerce_signal_timestamp
from alphavault_reflex.services.research_data import _default_signal_reference_time
from alphavault_reflex.services.research_data import _format_signal_created_at_line
from alphavault_reflex.services.thread_tree_lines import build_tree_render_lines


RELATED_FILTER_ALL = "all"
RELATED_FILTER_SIGNAL = "signal"
RELATED_FILTERS = {RELATED_FILTER_ALL, RELATED_FILTER_SIGNAL}

DEFAULT_RELATED_LIMIT = 20
RELATED_LIMIT_STEP = 20
MAX_RELATED_LIMIT = 500


class StockRelatedPostRow(TypedDict):
    post_uid: str
    title: str
    is_signal: str
    signal_badge: str
    action: str
    author: str
    created_at_sort: str
    created_at_line: str
    url: str
    raw_text: str
    display_md: str
    preview: str
    tree_text: str
    tree_lines: list[dict[str, str]]


def normalize_related_filter(value: object) -> str:
    text = str(value or "").strip().lower()
    return text if text in RELATED_FILTERS else RELATED_FILTER_ALL


def normalize_related_limit(value: object) -> int:
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        parsed = DEFAULT_RELATED_LIMIT
    if parsed <= 0:
        parsed = DEFAULT_RELATED_LIMIT
    return max(1, min(int(parsed), int(MAX_RELATED_LIMIT)))


def _signal_badge(action: str) -> str:
    a = str(action or "").strip().lower()
    if a.startswith("trade.buy"):
        return "买"
    if a.startswith("trade.sell"):
        return "卖"
    if a.startswith("trade."):
        return "信号"
    return ""


def _as_time_sort_text(created_at: object) -> str:
    text = str(created_at or "").strip()
    return text


def _coerce_row_dict(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): str(raw or "").strip()
        for key, raw in value.items()
        if str(key or "").strip()
    }


@dataclass(frozen=True)
class RelatedFeed:
    rows: list[StockRelatedPostRow]
    total: int


def _ensure_created_at_line(row: Mapping[str, object], *, now) -> str:
    """Return a stable `created_at_line` with relative age ("· xx小时前")."""
    existing = str(row.get("created_at_line") or "").strip()
    if existing and "·" in existing:
        return existing

    created_at = str(row.get("created_at") or "").strip()
    source = created_at or existing
    if not source:
        return existing

    filled = _format_signal_created_at_line(source, now=now)
    return filled or existing


def build_related_feed(
    *,
    signals: list[dict[str, str]] | list[object],
    backfill_posts: list[dict[str, str]] | list[object],
    related_filter: object,
    limit: object,
    now: object | None = None,
) -> RelatedFeed:
    """
    Merge stock signals + backfill candidates into one feed.

    This is UI-focused: keep it deterministic and cheap (no DB calls).
    """
    wanted_filter = normalize_related_filter(related_filter)
    wanted_limit = normalize_related_limit(limit)
    reference_now = _coerce_signal_timestamp(now) or _default_signal_reference_time()

    items: list[StockRelatedPostRow] = []
    seen: set[str] = set()

    for raw in signals or []:
        row = _coerce_row_dict(raw)
        post_uid = str(row.get("post_uid") or "").strip()
        if not post_uid or post_uid in seen:
            continue
        seen.add(post_uid)
        action = str(row.get("action") or "").strip()
        badge = _signal_badge(action)
        tree_text = str(row.get("tree_text") or "").strip()
        tree_lines = build_tree_render_lines(tree_text)
        items.append(
            {
                "post_uid": post_uid,
                "is_signal": "1",
                "signal_badge": badge,
                "created_at_line": _ensure_created_at_line(row, now=reference_now),
                "created_at_sort": _as_time_sort_text(row.get("created_at")),
                "title": str(row.get("summary") or "").strip(),
                "action": action,
                "author": str(row.get("author") or "").strip(),
                "preview": "",
                "tree_text": tree_text,
                "tree_lines": tree_lines,
                "url": str(row.get("url") or "").strip(),
                "raw_text": str(row.get("raw_text") or "").strip(),
                "display_md": str(row.get("display_md") or "").strip(),
            }
        )

    for raw in backfill_posts or []:
        row = _coerce_row_dict(raw)
        post_uid = str(row.get("post_uid") or "").strip()
        if not post_uid or post_uid in seen:
            continue
        seen.add(post_uid)
        created_at = str(row.get("created_at") or "").strip()
        title = str(row.get("matched_terms") or "").strip() or "相关帖子"
        tree_text = str(row.get("tree_text") or "").strip()
        tree_lines = build_tree_render_lines(tree_text)
        preview = str(row.get("preview") or "").strip()
        items.append(
            {
                "post_uid": post_uid,
                "is_signal": "",
                "signal_badge": "",
                "created_at_sort": _as_time_sort_text(created_at),
                "created_at_line": _ensure_created_at_line(row, now=reference_now),
                "title": title,
                "action": "",
                "author": str(row.get("author") or "").strip(),
                "url": str(row.get("url") or "").strip(),
                "raw_text": "",
                "display_md": "",
                "preview": preview,
                "tree_text": tree_text,
                "tree_lines": tree_lines,
            }
        )

    filtered = (
        [row for row in items if str(row.get("is_signal") or "") == "1"]
        if wanted_filter == RELATED_FILTER_SIGNAL
        else items
    )

    filtered.sort(
        key=lambda row: (
            str(row.get("created_at_sort") or ""),
            str(row.get("post_uid") or ""),
        ),
        reverse=True,
    )
    sliced = filtered[: max(1, int(wanted_limit))]
    return RelatedFeed(rows=sliced, total=int(len(filtered)))


__all__ = [
    "DEFAULT_RELATED_LIMIT",
    "MAX_RELATED_LIMIT",
    "RELATED_FILTER_ALL",
    "RELATED_FILTER_SIGNAL",
    "RELATED_LIMIT_STEP",
    "RelatedFeed",
    "build_related_feed",
    "normalize_related_filter",
    "normalize_related_limit",
]
