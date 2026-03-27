from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import math

import pandas as pd

from alphavault_reflex.services.homework_constants import (
    TRADE_BOARD_DEFAULT_WINDOW_DAYS,
    TRADE_BOARD_MAX_WINDOW_DAYS,
)
from alphavault_reflex.services.thread_tree import build_post_tree

TRADE_BUY_ACTIONS = frozenset({"trade.buy", "trade.add"})
TRADE_SELL_ACTIONS = frozenset({"trade.sell", "trade.reduce"})
TRADE_HOLD_ACTIONS = frozenset({"trade.hold"})

CONSENSUS_BUY = "↑偏买"
CONSENSUS_SELL = "↓偏卖"
CONSENSUS_HOLD = "→只看"
CONSENSUS_UNKNOWN = "·不清楚"

CONSENSUS_FILTER_ALL = "全部"
CONSENSUS_FILTER_HAS = "只看有共识（买/卖）"
CONSENSUS_FILTER_BUY = f"只看偏买（{CONSENSUS_BUY}）"
CONSENSUS_FILTER_SELL = f"只看偏卖（{CONSENSUS_SELL}）"
CONSENSUS_FILTER_HOLD = f"只看只看（{CONSENSUS_HOLD}）"
CONSENSUS_FILTER_UNKNOWN = f"只看不清楚（{CONSENSUS_UNKNOWN}）"

CONSENSUS_FILTER_OPTIONS = [
    CONSENSUS_FILTER_ALL,
    CONSENSUS_FILTER_HAS,
    CONSENSUS_FILTER_BUY,
    CONSENSUS_FILTER_SELL,
    CONSENSUS_FILTER_HOLD,
    CONSENSUS_FILTER_UNKNOWN,
]

CONSENSUS_FILTER_VALUES = {
    CONSENSUS_FILTER_HAS: frozenset({CONSENSUS_BUY, CONSENSUS_SELL}),
    CONSENSUS_FILTER_BUY: frozenset({CONSENSUS_BUY}),
    CONSENSUS_FILTER_SELL: frozenset({CONSENSUS_SELL}),
    CONSENSUS_FILTER_HOLD: frozenset({CONSENSUS_HOLD}),
    CONSENSUS_FILTER_UNKNOWN: frozenset({CONSENSUS_UNKNOWN}),
}

SORT_MODE_NEWEST = "最新"
SORT_MODE_AUTHOR = "大佬最多"
SORT_MODE_CONSENSUS = "共识最强"

SORT_MODE_OPTIONS = [
    SORT_MODE_NEWEST,
    SORT_MODE_AUTHOR,
    SORT_MODE_CONSENSUS,
]


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

    action_cn = "交易"
    if action_str in TRADE_BUY_ACTIONS:
        action_cn = "买"
    elif action_str in TRADE_SELL_ACTIONS:
        action_cn = "卖"
    elif action_str in TRADE_HOLD_ACTIONS:
        action_cn = "只看"
    elif action_str.startswith("trade."):
        action_cn = "交易"
    elif action_str:
        action_cn = action_str

    parts = [
        str(action_cn),
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
    view["post_uid"] = view["post_uid"].apply(lambda v: "" if pd.isna(v) else str(v))
    view["post_uid"] = view["post_uid"].astype(str).str.strip()
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
    sort_mode: str,
    consensus_filter: str,
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
        group_col = "topic_key" if "topic_key" in board_df.columns else group_col
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

    consensus = pd.Series([CONSENSUS_UNKNOWN] * len(agg), index=agg.index)
    consensus = consensus.mask(agg["net_strength"] > 0, CONSENSUS_BUY)
    consensus = consensus.mask(agg["net_strength"] < 0, CONSENSUS_SELL)
    consensus = consensus.mask(
        (agg["net_strength"] == 0)
        & (agg["buy_strength_sum"] == 0)
        & (agg["sell_strength_sum"] == 0)
        & (agg["hold_mentions_sum"] > 0),
        CONSENSUS_HOLD,
    )
    agg["consensus"] = consensus

    if sort_mode == SORT_MODE_NEWEST:
        agg_sorted = agg.sort_values(by="recent_time", ascending=False)
    elif sort_mode == SORT_MODE_AUTHOR:
        agg_sorted = agg.sort_values(
            by=["author_count", "mentions", "recent_time"], ascending=False
        )
    else:
        abs_net = agg["net_strength"].abs()
        agg_sorted = (
            agg.assign(_abs_net=abs_net)
            .sort_values(
                by=["_abs_net", "author_count", "recent_time"], ascending=False
            )
            .drop(columns=["_abs_net"])
        )

    allowed = CONSENSUS_FILTER_VALUES.get(str(consensus_filter or "").strip())
    if allowed:
        agg_sorted = agg_sorted[
            agg_sorted["consensus"].astype(str).isin(allowed)
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
            else str(row.get("post_uid") or "").strip()
        )
        tree_post_uid = post_uid if post_uid else ""
        if not tree_post_uid:
            for cand in candidates_by_topic.get(topic, []):
                cand_uid = str(cand or "").strip()
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
                "consensus": str(row.get("consensus") or "").strip(),
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
