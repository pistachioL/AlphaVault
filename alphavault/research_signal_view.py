from __future__ import annotations

from datetime import UTC, datetime

from alphavault.domains.thread_tree.service import build_post_tree_map


MINUTES_PER_HOUR = 60
MINUTES_PER_DAY = 24 * MINUTES_PER_HOUR
MAX_SIGNAL_ROWS = 60


def merge_post_fields(
    assertions: list[dict[str, object]],
    posts: list[dict[str, object]],
) -> list[dict[str, object]]:
    if not assertions or not posts:
        return [dict(row) for row in assertions]

    posts_by_uid = {
        _clean_text(row.get("post_uid")): row
        for row in posts
        if _clean_text(row.get("post_uid"))
    }
    if not posts_by_uid:
        return [dict(row) for row in assertions]

    merged: list[dict[str, object]] = []
    for row in assertions:
        payload = dict(row)
        post_uid = _clean_text(payload.get("post_uid"))
        post_row = posts_by_uid.get(post_uid) or {}
        if not _clean_text(payload.get("raw_text")):
            payload["raw_text"] = _clean_text(post_row.get("raw_text"))
        else:
            payload["raw_text"] = _clean_text(payload.get("raw_text"))
        if not _clean_text(payload.get("author")):
            payload["author"] = _clean_text(post_row.get("author"))
        else:
            payload["author"] = _clean_text(payload.get("author"))
        if not _clean_text(payload.get("url")):
            payload["url"] = _clean_text(post_row.get("url"))
        else:
            payload["url"] = _clean_text(payload.get("url"))
        if not payload.get("created_at"):
            payload["created_at"] = post_row.get("created_at", "")
        merged.append(payload)
    return merged


def build_signal_rows(
    view: list[dict[str, object]],
    *,
    posts: list[dict[str, object]],
    now: datetime | None = None,
) -> list[dict[str, str]]:
    if not view:
        return []
    rows = _sort_rows_by_created_at(view)[:MAX_SIGNAL_ROWS]
    out: list[dict[str, str]] = []
    tree_map = build_post_tree_map(
        post_uids=[
            _clean_text(row.get("post_uid"))
            for row in rows
            if _clean_text(row.get("post_uid"))
        ],
        posts=posts,
    )
    reference_now = coerce_signal_timestamp(now) or default_signal_reference_time()
    for row in rows:
        created = row.get("created_at")
        created_text = _format_signal_timestamp(created)
        created_at_line = format_signal_created_at_line(created, now=reference_now)
        post_uid = _clean_text(row.get("post_uid"))
        tree_label = ""
        tree_text = ""
        if post_uid:
            tree_label, tree_text = tree_map.get(post_uid, ("", ""))
        out.append(
            {
                "post_uid": post_uid,
                "summary": _clean_text(row.get("summary")),
                "action": _clean_text(row.get("action")),
                "action_strength": _clean_text(row.get("action_strength")),
                "author": _clean_text(row.get("author")),
                "created_at": created_text,
                "created_at_line": created_at_line,
                "raw_text": _clean_text(row.get("raw_text")),
                "tree_label": tree_label,
                "tree_text": tree_text,
            }
        )
    return out


def build_related_stock_rows(view: list[dict[str, object]]) -> list[dict[str, str]]:
    counts: dict[str, int] = {}
    raw_values: list[object] = []
    for row in view:
        if "stock_key" in row:
            raw_values.append(row.get("stock_key"))
            continue
        if "resolved_entity_key" in row:
            raw_values.append(row.get("resolved_entity_key"))
            continue
        raw_values.append(row.get("entity_key"))
    for raw_key in raw_values:
        stock_key = _clean_text(raw_key)
        if not stock_key.startswith("stock:"):
            continue
        counts[stock_key] = int(counts.get(stock_key, 0)) + 1
    ranked = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return [
        {"stock_key": stock_key, "mention_count": str(count)}
        for stock_key, count in ranked
    ]


def sector_filter_rows(
    assertions: list[dict[str, object]], sector_key: str
) -> list[dict[str, object]]:
    return [
        dict(row)
        for row in assertions
        if _row_matches_sector(row, sector_key=sector_key)
    ]


def coerce_signal_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        ts = value
    else:
        text = _clean_text(value)
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


def _format_signal_timestamp(value: object) -> str:
    ts = coerce_signal_timestamp(value)
    if ts is None:
        return ""
    return ts.strftime("%Y-%m-%d %H:%M")


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


def format_signal_created_at_line(value: object, *, now: datetime) -> str:
    created_at_text = _format_signal_timestamp(value)
    if not created_at_text:
        return ""
    age_text = _format_signal_age(value, now=now)
    if not age_text:
        return created_at_text
    return f"{created_at_text} · {age_text}"


def _coerce_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _row_matches_sector(row: dict[str, object], *, sector_key: str) -> bool:
    cluster_keys = row.get("cluster_keys")
    if sector_key in _coerce_list(cluster_keys):
        return True
    return _clean_text(row.get("cluster_key")) == sector_key


def _sort_rows_by_created_at(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    def _sort_key(row: dict[str, object]) -> tuple[int, float]:
        ts = coerce_signal_timestamp(row.get("created_at"))
        if ts is None:
            return (1, 0.0)
        return (0, -ts.timestamp())

    return [dict(row) for row in sorted(rows, key=_sort_key)]


__all__ = [
    "MAX_SIGNAL_ROWS",
    "build_related_stock_rows",
    "build_signal_rows",
    "coerce_signal_timestamp",
    "default_signal_reference_time",
    "format_signal_created_at_line",
    "merge_post_fields",
    "sector_filter_rows",
]
