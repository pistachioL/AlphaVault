from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import math
import re
from typing import cast

from alphavault.domains.thread_tree.service import build_post_tree
from alphavault.domains.thread_tree.service import normalize_tree_lookup_post_uid
from alphavault_reflex.services.homework_time_range import coerce_homework_timestamp

TRADE_BUY_ACTIONS = frozenset({"trade.buy", "trade.add"})
TRADE_SELL_ACTIONS = frozenset({"trade.sell", "trade.reduce"})
TRADE_HOLD_ACTIONS = frozenset({"trade.hold", "trade.watch"})

TRADE_FILTER_ALL = "全部"
TRADE_FILTER_BUY = "买"
TRADE_FILTER_SELL = "卖"
TRADE_FILTER_HOLD = "只看"

TRADE_FILTER_OPTIONS = [
    TRADE_FILTER_ALL,
    TRADE_FILTER_BUY,
    TRADE_FILTER_SELL,
    TRADE_FILTER_HOLD,
]

TRADE_FILTER_VALUES = {
    TRADE_FILTER_BUY: TRADE_BUY_ACTIONS,
    TRADE_FILTER_SELL: TRADE_SELL_ACTIONS,
    TRADE_FILTER_HOLD: TRADE_HOLD_ACTIONS,
}


@dataclass(frozen=True)
class BoardResult:
    caption: str
    rows: list[dict[str, str]]


def format_age_label(max_ts: datetime, ts: datetime) -> str:
    if not isinstance(ts, datetime) or not isinstance(max_ts, datetime):
        return ""
    delta = max_ts - ts
    if delta.total_seconds() < 0:
        delta = timedelta(seconds=0)
    minutes = int(delta.total_seconds() // 60)
    if minutes < 60:
        return f"{minutes}分钟"
    hours = int(minutes // 60)
    if hours < 48:
        return f"{hours}小时"
    days = int(hours // 24)
    return f"{days}天"


def trade_action_badge(action: str, strength: object) -> str:
    action_str = str(action or "").strip()
    strength_num = _coerce_strength(strength)
    strength_num = max(0, min(3, strength_num))

    strength_text = "很弱"
    if strength_num == 1:
        strength_text = "偏弱"
    elif strength_num == 2:
        strength_text = "中等偏强"
    elif strength_num >= 3:
        strength_text = "很强"

    parts = [
        str(action_str),
        str(strength_text),
        f"强度 {strength_num}",
    ]
    return " · ".join([p for p in parts if str(p).strip()])


def _build_latest_post_uid_candidates(
    board_view: list[dict[str, object]], *, group_col: str
) -> dict[str, list[str]]:
    if not board_view:
        return {}
    ordered = sorted(board_view, key=_row_recency_key, reverse=True)

    candidates: dict[str, list[str]] = {}
    for row in ordered:
        topic = str(row.get(group_col) or "").strip()
        if not topic:
            continue
        uid = normalize_tree_lookup_post_uid(row.get("post_uid"))
        if not uid:
            continue
        bucket = candidates.setdefault(topic, [])
        if uid not in bucket:
            bucket.append(uid)
    return candidates


def _bucket_int(bucket: dict[str, object], key: str) -> int:
    return _coerce_strength(bucket.get(key))


def _bucket_recent_time(bucket: dict[str, object]) -> datetime | None:
    recent_time = bucket.get("recent_time")
    if isinstance(recent_time, datetime):
        return recent_time
    return None


def _bucket_recent_row(bucket: dict[str, object]) -> dict[str, object]:
    recent_row = bucket.get("recent_row")
    if isinstance(recent_row, dict):
        return cast(dict[str, object], recent_row)
    return {}


def _bucket_authors(bucket: dict[str, object]) -> set[str]:
    authors = bucket.get("authors")
    if isinstance(authors, set):
        return cast(set[str], authors)
    return set()


def _bucket_recent_key(bucket: dict[str, object]) -> tuple[float, str, int]:
    return _row_recency_key(_bucket_recent_row(bucket))


def build_board(
    assertions: list[dict[str, object]],
    posts: list[dict[str, object]],
    *,
    group_col: str,
    group_label: str,
    range_start_utc: datetime,
    range_end_exclusive_utc: datetime,
    range_caption: str,
    age_reference_utc: datetime,
    trade_filter: str,
) -> BoardResult:
    del posts, group_label
    empty_result = BoardResult(caption=str(range_caption or "").strip(), rows=[])
    if not assertions:
        return empty_result

    trade_rows = [
        dict(row)
        for row in assertions
        if str(row.get("action") or "").strip().startswith("trade.")
    ]
    if not trade_rows:
        return empty_result

    if not any(group_col in row for row in trade_rows):
        if any("entity_key" in row for row in trade_rows):
            group_col = "entity_key"

    board_rows: list[dict[str, object]] = []
    for row in trade_rows:
        topic = str(row.get(group_col) or "").strip()
        created_at = coerce_homework_timestamp(row.get("created_at"))
        if not topic or created_at is None:
            continue
        if created_at < range_start_utc or created_at >= range_end_exclusive_utc:
            continue
        payload = dict(row)
        payload["_created_at_ts"] = created_at
        board_rows.append(payload)
    if not board_rows:
        return empty_result

    grouped: dict[str, dict[str, object]] = {}
    for row in board_rows:
        topic = str(row.get(group_col) or "").strip()
        if not topic:
            continue
        action = str(row.get("action") or "").strip()
        strength = _coerce_strength(row.get("action_strength"))
        created_at_value = row.get("_created_at_ts")
        created_at = (
            created_at_value if isinstance(created_at_value, datetime) else datetime.min
        )
        bucket = grouped.setdefault(
            topic,
            {
                "buy_strength_sum": 0,
                "sell_strength_sum": 0,
                "hold_mentions_sum": 0,
                "mentions": 0,
                "authors": set(),
                "recent_time": created_at,
                "recent_row": dict(row),
            },
        )
        bucket["mentions"] = _bucket_int(bucket, "mentions") + 1
        authors = _bucket_authors(bucket)
        author = str(row.get("author") or "").strip()
        if author:
            authors.add(author)
        if action in TRADE_BUY_ACTIONS:
            bucket["buy_strength_sum"] = (
                _bucket_int(bucket, "buy_strength_sum") + strength
            )
        if action in TRADE_SELL_ACTIONS:
            bucket["sell_strength_sum"] = (
                _bucket_int(bucket, "sell_strength_sum") + strength
            )
        if action in TRADE_HOLD_ACTIONS:
            bucket["hold_mentions_sum"] = _bucket_int(bucket, "hold_mentions_sum") + 1
        if _row_recency_key(row) >= _bucket_recent_key(bucket):
            bucket["recent_time"] = created_at
            bucket["recent_row"] = dict(row)

    agg_sorted = sorted(
        grouped.items(),
        key=lambda item: (_bucket_recent_key(item[1]), str(item[0])),
        reverse=True,
    )

    allowed_actions = TRADE_FILTER_VALUES.get(str(trade_filter or "").strip())
    if allowed_actions:
        agg_sorted = [
            item
            for item in agg_sorted
            if str(_bucket_recent_row(item[1]).get("action") or "").strip()
            in allowed_actions
        ].copy()
    key_set = {
        str(topic or "").strip() for topic, _ in agg_sorted if str(topic or "").strip()
    }
    board_view = [
        dict(row)
        for row in board_rows
        if str(row.get(group_col) or "").strip() in key_set
    ]

    candidates_by_topic = _build_latest_post_uid_candidates(
        board_view, group_col=group_col
    )

    rows: list[dict[str, str]] = []
    for topic, stats in agg_sorted:
        recent_row = _bucket_recent_row(stats)
        post_uid = normalize_tree_lookup_post_uid(recent_row.get("post_uid"))
        tree_post_uid = post_uid if post_uid else ""
        if not tree_post_uid:
            for cand in candidates_by_topic.get(topic, []):
                cand_uid = normalize_tree_lookup_post_uid(cand)
                if cand_uid:
                    tree_post_uid = cand_uid
                    break

        buy_strength_value = _bucket_int(stats, "buy_strength_sum")
        sell_strength_value = _bucket_int(stats, "sell_strength_sum")
        net_strength = _to_int_text(buy_strength_value - sell_strength_value)
        buy_strength = _to_int_text(buy_strength_value)
        sell_strength = _to_int_text(sell_strength_value)
        mentions = _to_int_text(_bucket_int(stats, "mentions"))
        author_count = _to_int_text(len(_bucket_authors(stats)))
        recent_dt = _bucket_recent_time(stats)

        rows.append(
            {
                "topic": topic,
                "summary": _normalize_summary(recent_row.get("summary")),
                "url": str(recent_row.get("url") or "").strip(),
                "recent_action": trade_action_badge(
                    str(recent_row.get("action") or "").strip(),
                    recent_row.get("action_strength"),
                ),
                "recent_age": (
                    format_age_label(age_reference_utc, recent_dt) if recent_dt else ""
                ),
                "recent_author": str(recent_row.get("author") or "").strip(),
                "net_strength": net_strength,
                "buy_strength": buy_strength,
                "sell_strength": sell_strength,
                "mentions": mentions,
                "author_count": author_count,
                "tree_post_uid": tree_post_uid,
            }
        )

    return BoardResult(
        caption=str(range_caption or "").strip(),
        rows=rows,
    )


def build_tree(*, post_uid: str, posts: list[dict[str, object]]) -> tuple[str, str]:
    return build_post_tree(post_uid=post_uid, posts=posts)


def _coerce_strength(value: object) -> int:
    try:
        parsed = int(float(str(value or "").strip() or 0))
    except (TypeError, ValueError):
        return 0
    return max(0, min(parsed, 3))


def _sort_row_desc(row: dict[str, object]) -> tuple[int, float]:
    recency = _row_recency_key(row)
    if recency[0] == float("-inf"):
        return (1, 0.0)
    return (0, -recency[0])


def _row_recency_key(row: dict[str, object]) -> tuple[float, str, int]:
    ts = _row_timestamp(row)
    if ts is None:
        return (float("-inf"), "", -1)
    return (
        ts.timestamp(),
        str(row.get("post_uid") or "").strip(),
        _row_idx(row.get("idx")),
    )


def _row_timestamp(row: dict[str, object]) -> datetime | None:
    cached = row.get("_created_at_ts")
    if isinstance(cached, datetime):
        return cached
    return coerce_homework_timestamp(row.get("created_at"))


def _row_idx(value: object) -> int:
    try:
        return int(str(value or "").strip() or 0)
    except (TypeError, ValueError):
        return 0


def _to_int_text(value: object) -> str:
    try:
        parsed = float(str(value or "").strip() or 0)
    except (TypeError, ValueError):
        return "0"
    if not math.isfinite(parsed):
        return "0"
    return str(int(parsed))


def _normalize_summary(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())
