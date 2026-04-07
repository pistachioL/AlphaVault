from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import math

import pandas as pd

from alphavault_reflex.services.homework_constants import (
    TRADE_BOARD_DEFAULT_WINDOW_DAYS,
    TRADE_BOARD_MAX_WINDOW_DAYS,
)
from alphavault.domains.thread_tree.service import build_post_tree
from alphavault.domains.thread_tree.service import normalize_tree_lookup_post_uid

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
    window_max_days: int
    used_window_days: int
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
    strength_val = pd.to_numeric(strength, errors="coerce")
    strength_num = 0 if pd.isna(strength_val) else int(strength_val)
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


def _trade_data_coverage(
    assertions: pd.DataFrame,
) -> tuple[int, datetime | None, datetime | None]:
    if assertions.empty or "created_at" not in assertions.columns:
        return 1, None, None
    created = pd.to_datetime(assertions["created_at"], errors="coerce").dropna()
    if created.empty:
        return 1, None, None
    min_ts = created.min()
    max_ts = created.max()
    try:
        days = int((max_ts - min_ts).days) + 1
    except Exception:
        days = 1
    return max(1, days), min_ts.to_pydatetime(), max_ts.to_pydatetime()


def _build_latest_post_uid_candidates(
    board_view: pd.DataFrame, *, group_col: str
) -> dict[str, list[str]]:
    if (
        board_view.empty
        or group_col not in board_view.columns
        or "post_uid" not in board_view.columns
    ):
        return {}
    view = board_view[[group_col, "created_at", "post_uid"]].copy()
    view["post_uid"] = view["post_uid"].apply(
        lambda value: "" if pd.isna(value) else normalize_tree_lookup_post_uid(value)
    )
    view = view[view["post_uid"].ne("")]
    if view.empty:
        return {}
    if "created_at" in view.columns:
        view = view.sort_values(by="created_at", ascending=False)

    candidates: dict[str, list[str]] = {}
    for key, group in view.groupby(group_col, sort=False):
        topic = str(key or "").strip()
        if not topic:
            continue
        seen: set[str] = set()
        uids: list[str] = []
        for uid in group["post_uid"].tolist():
            s = str(uid or "").strip()
            if not s or s in seen:
                continue
            seen.add(s)
            uids.append(s)
        if uids:
            candidates[topic] = uids
    return candidates


def build_board(
    assertions: pd.DataFrame,
    posts: pd.DataFrame,
    *,
    group_col: str,
    group_label: str,
    window_days: int,
    trade_filter: str,
) -> BoardResult:
    if assertions.empty or "action" not in assertions.columns:
        return BoardResult(caption="", window_max_days=1, used_window_days=1, rows=[])

    action_series = assertions["action"].astype(str)
    trade_mask = action_series.str.startswith("trade.", na=False)
    trade_df = assertions[trade_mask].copy()
    if trade_df.empty:
        return BoardResult(caption="", window_max_days=1, used_window_days=1, rows=[])

    coverage_days, _min_ts, max_ts_full = _trade_data_coverage(trade_df)
    window_max_days = max(1, min(int(coverage_days), TRADE_BOARD_MAX_WINDOW_DAYS))
    used_window_days = max(
        1, min(int(window_days or TRADE_BOARD_DEFAULT_WINDOW_DAYS), window_max_days)
    )

    caption = ""
    if max_ts_full:
        window_start = max_ts_full - timedelta(days=max(0, used_window_days - 1))
        window_line = f"窗口：最近 {used_window_days} 天：{window_start.date()} ~ {max_ts_full.date()}"
        caption = window_line

    if "created_at" not in trade_df.columns:
        return BoardResult(
            caption=caption,
            window_max_days=window_max_days,
            used_window_days=used_window_days,
            rows=[],
        )

    board_df = trade_df.dropna(subset=["created_at"]).copy()
    if group_col not in board_df.columns:
        group_col = "entity_key" if "entity_key" in board_df.columns else group_col
    if group_col in board_df.columns:
        board_df = board_df[board_df[group_col].astype(str).str.strip().ne("")].copy()
    if board_df.empty:
        return BoardResult(
            caption=caption,
            window_max_days=window_max_days,
            used_window_days=used_window_days,
            rows=[],
        )

    max_ts = board_df["created_at"].max()
    cutoff_day = max_ts.date() - timedelta(days=max(0, int(used_window_days) - 1))
    cutoff = datetime.combine(cutoff_day, datetime.min.time())
    board_df = board_df[board_df["created_at"] >= cutoff].copy()
    if board_df.empty:
        return BoardResult(
            caption=caption,
            window_max_days=window_max_days,
            used_window_days=used_window_days,
            rows=[],
        )

    strength = (
        pd.to_numeric(board_df.get("action_strength"), errors="coerce")
        .fillna(0)
        .astype(int)
        .clip(lower=0, upper=3)
    )
    board_df["strength"] = strength
    board_df["buy_strength"] = board_df["strength"].where(
        board_df["action"].isin(TRADE_BUY_ACTIONS), 0
    )
    board_df["sell_strength"] = board_df["strength"].where(
        board_df["action"].isin(TRADE_SELL_ACTIONS), 0
    )
    board_df["hold_mentions"] = board_df["action"].isin(TRADE_HOLD_ACTIONS).astype(int)

    agg = board_df.groupby(group_col, as_index=False).agg(
        buy_strength_sum=("buy_strength", "sum"),
        sell_strength_sum=("sell_strength", "sum"),
        hold_mentions_sum=("hold_mentions", "sum"),
        mentions=(group_col, "count"),
        author_count=("author", "nunique"),
        recent_time=("created_at", "max"),
    )
    agg["net_strength"] = agg["buy_strength_sum"] - agg["sell_strength_sum"]

    last_rows = (
        board_df.sort_values(by="created_at")
        .groupby(group_col)
        .tail(1)
        .set_index(group_col)
    )
    last_badge = last_rows.apply(
        lambda row: trade_action_badge(row.get("action"), row.get("strength")), axis=1
    )

    agg["post_uid"] = agg[group_col].map(last_rows.get("post_uid"))
    agg["recent_author"] = agg[group_col].map(last_rows.get("author")).fillna("")
    agg["recent_action"] = agg[group_col].map(last_badge).fillna("")
    agg["summary"] = (
        agg[group_col]
        .map(last_rows.get("summary"))
        .fillna("")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
    )
    if "url" in last_rows.columns:
        agg["url"] = agg[group_col].map(last_rows.get("url")).fillna("")
    else:
        agg["url"] = ""
    agg["recent_age"] = agg["recent_time"].apply(
        lambda ts: format_age_label(max_ts, ts)
    )

    agg["recent_trade_action"] = (
        agg[group_col].map(last_rows.get("action")).fillna("").astype(str).str.strip()
    )

    agg_sorted = agg.sort_values(by="recent_time", ascending=False)

    allowed_actions = TRADE_FILTER_VALUES.get(str(trade_filter or "").strip())
    if allowed_actions:
        agg_sorted = agg_sorted[
            agg_sorted["recent_trade_action"].astype(str).isin(allowed_actions)
        ].copy()

    keys = (
        agg_sorted.get(group_col, pd.Series(dtype=str))
        .dropna()
        .astype(str)
        .map(lambda s: s.strip())
    )
    key_set = {k for k in keys.tolist() if k}
    board_view = (
        board_df[board_df[group_col].astype(str).str.strip().isin(key_set)].copy()
        if key_set
        else board_df.head(0).copy()
    )

    candidates_by_topic = _build_latest_post_uid_candidates(
        board_view, group_col=group_col
    )

    rows: list[dict[str, str]] = []
    for _, row in agg_sorted.iterrows():
        topic = str(row.get(group_col) or "").strip()
        post_uid = (
            ""
            if pd.isna(row.get("post_uid"))
            else normalize_tree_lookup_post_uid(row.get("post_uid"))
        )
        tree_post_uid = post_uid if post_uid else ""
        if not tree_post_uid:
            for cand in candidates_by_topic.get(topic, []):
                cand_uid = normalize_tree_lookup_post_uid(cand)
                if cand_uid:
                    tree_post_uid = cand_uid
                    break

        def to_int_text(v: object) -> str:
            val = pd.to_numeric(v, errors="coerce")
            if pd.isna(val):
                return "0"
            if math.isfinite(float(val)):
                return str(int(val))
            return "0"

        net_strength = to_int_text(row.get("net_strength"))
        buy_strength = to_int_text(row.get("buy_strength_sum"))
        sell_strength = to_int_text(row.get("sell_strength_sum"))
        mentions = to_int_text(row.get("mentions"))
        author_count = to_int_text(row.get("author_count"))

        rows.append(
            {
                "topic": topic,
                "summary": str(row.get("summary") or "").strip(),
                "url": str(row.get("url") or "").strip(),
                "recent_action": str(row.get("recent_action") or "").strip(),
                "recent_age": str(row.get("recent_age") or "").strip(),
                "recent_author": str(row.get("recent_author") or "").strip(),
                "net_strength": net_strength,
                "buy_strength": buy_strength,
                "sell_strength": sell_strength,
                "mentions": mentions,
                "author_count": author_count,
                "tree_post_uid": tree_post_uid,
            }
        )

    return BoardResult(
        caption=caption,
        window_max_days=window_max_days,
        used_window_days=used_window_days,
        rows=rows,
    )


def build_tree(*, post_uid: str, posts: pd.DataFrame) -> tuple[str, str]:
    return build_post_tree(post_uid=post_uid, posts=posts)
