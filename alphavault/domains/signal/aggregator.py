from __future__ import annotations

from datetime import UTC, datetime

from alphavault.domains.content.models import Post
from alphavault.domains.content.row_mapper import index_posts_by_uid, post_to_row
from alphavault.domains.signal.models import Assertion, Signal
from alphavault.domains.thread_tree.service import build_post_tree_map

MINUTES_PER_HOUR = 60
MINUTES_PER_DAY = 24 * MINUTES_PER_HOUR
MAX_SIGNAL_ROWS = 60


def coerce_signal_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        ts = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            ts = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                ts = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    ts = datetime.strptime(text, "%Y-%m-%d %H:%M")
                except ValueError:
                    return None
    if ts.tzinfo is not None:
        return ts.astimezone(UTC).replace(tzinfo=None)
    return ts


def default_signal_reference_time() -> datetime:
    return datetime.now(tz=UTC).replace(tzinfo=None)


def format_signal_created_at_line(value: object, *, now: datetime) -> str:
    created_at_text = format_signal_timestamp(value)
    if not created_at_text:
        return ""
    age_text = _format_signal_age(value, now=now)
    if not age_text:
        return created_at_text
    return f"{created_at_text} · {age_text}"


def format_signal_timestamp(value: object) -> str:
    ts = coerce_signal_timestamp(value)
    if ts is None:
        return ""
    return ts.strftime("%Y-%m-%d %H:%M")


def merge_assertions_with_posts(
    assertions: list[Assertion],
    posts: list[Post],
) -> list[Signal]:
    posts_by_uid = index_posts_by_uid(posts)
    merged: list[Signal] = []
    for assertion in assertions:
        post = posts_by_uid.get(assertion.post_uid)
        merged.append(
            Signal(
                post_uid=assertion.post_uid,
                entity_key=assertion.entity_key,
                resolved_entity_key=assertion.resolved_entity_key,
                stock_key=assertion.stock_key,
                action=assertion.action,
                action_strength=assertion.action_strength,
                summary=assertion.summary,
                evidence=assertion.evidence,
                confidence=assertion.confidence,
                author=assertion.author or (post.author if post is not None else ""),
                created_at=assertion.created_at
                if assertion.created_at
                else (post.created_at if post is not None else ""),
                url=assertion.url or (post.url if post is not None else ""),
                raw_text=assertion.raw_text
                or (post.raw_text if post is not None else ""),
                cluster_keys=assertion.cluster_keys,
            )
        )
    return merged


def attach_signal_tree_context(
    signals: list[Signal],
    *,
    posts: list[Post],
) -> list[Signal]:
    if not signals:
        return []
    post_rows = [post_to_row(post) for post in posts]
    tree_map = build_post_tree_map(
        post_uids=[
            str(signal.post_uid or "").strip()
            for signal in signals
            if str(signal.post_uid or "").strip()
        ],
        posts=post_rows,
    )
    out: list[Signal] = []
    for signal in signals:
        tree_label, tree_text = tree_map.get(signal.post_uid, ("", ""))
        out.append(
            Signal(
                post_uid=signal.post_uid,
                entity_key=signal.entity_key,
                resolved_entity_key=signal.resolved_entity_key,
                stock_key=signal.stock_key,
                action=signal.action,
                action_strength=signal.action_strength,
                summary=signal.summary,
                evidence=signal.evidence,
                confidence=signal.confidence,
                author=signal.author,
                created_at=signal.created_at,
                url=signal.url,
                raw_text=signal.raw_text,
                cluster_keys=signal.cluster_keys,
                tree_label=tree_label,
                tree_text=tree_text,
            )
        )
    return out


def sort_signals_by_created_at(signals: list[Signal]) -> list[Signal]:
    return sorted(signals, key=_signal_sort_key)


def slice_signals(
    signals: list[Signal],
    *,
    page: int,
    page_size: int,
) -> tuple[list[Signal], int, int]:
    if not signals:
        return [], 0, 1
    safe_page_size = _clamp_page_size(page_size)
    safe_page = _coerce_positive_int(page, default=1)
    rows = sort_signals_by_created_at(signals)
    total = len(rows)
    total_pages = max(1, (total + safe_page_size - 1) // safe_page_size)
    safe_page = min(safe_page, total_pages)
    start = (safe_page - 1) * safe_page_size
    end = start + safe_page_size
    return rows[start:end], total, safe_page


def build_related_stock_rows(signals: list[Signal]) -> list[dict[str, str]]:
    counts: dict[str, int] = {}
    for signal in signals:
        stock_key = (
            str(signal.stock_key or "").strip()
            or str(signal.resolved_entity_key or "").strip()
            or str(signal.entity_key or "").strip()
        )
        if not stock_key.startswith("stock:"):
            continue
        counts[stock_key] = int(counts.get(stock_key, 0)) + 1
    ranked = sorted(counts.items(), key=lambda item: (-int(item[1]), str(item[0])))
    return [
        {"stock_key": stock_key, "mention_count": str(count)}
        for stock_key, count in ranked
    ]


def build_related_sector_rows(signals: list[Signal]) -> list[dict[str, str]]:
    counts: dict[str, int] = {}
    for signal in signals:
        for sector_key in signal.cluster_keys:
            text = str(sector_key or "").strip()
            if not text:
                continue
            counts[text] = int(counts.get(text, 0)) + 1
    ranked = sorted(counts.items(), key=lambda item: (-int(item[1]), str(item[0])))
    return [
        {"sector_key": sector_key, "mention_count": str(count)}
        for sector_key, count in ranked
    ]


def filter_signals_for_sector(
    signals: list[Signal],
    *,
    sector_key: str,
) -> list[Signal]:
    target = str(sector_key or "").strip()
    if not target:
        return []
    return [
        signal
        for signal in signals
        if target in {str(item or "").strip() for item in signal.cluster_keys}
    ]


def _signal_sort_key(signal: Signal) -> tuple[int, float]:
    ts = coerce_signal_timestamp(signal.created_at)
    if ts is None:
        return (1, 0.0)
    return (0, -ts.timestamp())


def _clamp_page_size(value: object) -> int:
    size = _coerce_positive_int(value, default=MAX_SIGNAL_ROWS)
    return max(1, min(size, MAX_SIGNAL_ROWS))


def _coerce_positive_int(value: object, *, default: int) -> int:
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        return int(default)
    if parsed <= 0:
        return int(default)
    return int(parsed)


def _format_signal_age(value: object, *, now: datetime) -> str:
    ts = coerce_signal_timestamp(value)
    if ts is None:
        return ""
    delta_seconds = max((now - ts).total_seconds(), 0)
    minutes = int(delta_seconds // MINUTES_PER_HOUR)
    if minutes < MINUTES_PER_HOUR:
        return f"{minutes}分钟前"
    if minutes < MINUTES_PER_DAY:
        return f"{minutes // MINUTES_PER_HOUR}小时前"
    return f"{minutes // MINUTES_PER_DAY}天前"


__all__ = [
    "MAX_SIGNAL_ROWS",
    "attach_signal_tree_context",
    "build_related_sector_rows",
    "build_related_stock_rows",
    "coerce_signal_timestamp",
    "default_signal_reference_time",
    "filter_signals_for_sector",
    "format_signal_created_at_line",
    "format_signal_timestamp",
    "merge_assertions_with_posts",
    "slice_signals",
    "sort_signals_by_created_at",
]
