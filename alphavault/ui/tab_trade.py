"""
Streamlit tab: trade flow.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import html as _html

import pandas as pd
import streamlit as st

from alphavault.ui.thread_tree import build_weibo_thread_forest

TRADE_BUY_ACTIONS = frozenset({"trade.buy", "trade.add"})
TRADE_SELL_ACTIONS = frozenset({"trade.sell", "trade.reduce"})
TRADE_HOLD_ACTIONS = frozenset({"trade.hold"})
TRADE_BOARD_MAX_WINDOW_DAYS = 60

TRADE_BOARD_CONSENSUS_BUY = "↑偏买"
TRADE_BOARD_CONSENSUS_SELL = "↓偏卖"
TRADE_BOARD_CONSENSUS_HOLD = "→只看"
TRADE_BOARD_CONSENSUS_UNKNOWN = "·不清楚"

TRADE_BOARD_CONSENSUS_FILTER_ALL = "全部"
TRADE_BOARD_CONSENSUS_FILTER_HAS = "只看有共识（买/卖）"
TRADE_BOARD_CONSENSUS_FILTER_BUY = f"只看偏买（{TRADE_BOARD_CONSENSUS_BUY}）"
TRADE_BOARD_CONSENSUS_FILTER_SELL = f"只看偏卖（{TRADE_BOARD_CONSENSUS_SELL}）"
TRADE_BOARD_CONSENSUS_FILTER_HOLD = f"只看只看（{TRADE_BOARD_CONSENSUS_HOLD}）"
TRADE_BOARD_CONSENSUS_FILTER_UNKNOWN = f"只看不清楚（{TRADE_BOARD_CONSENSUS_UNKNOWN}）"

TRADE_BOARD_CONSENSUS_FILTER_OPTIONS = [
    TRADE_BOARD_CONSENSUS_FILTER_ALL,
    TRADE_BOARD_CONSENSUS_FILTER_HAS,
    TRADE_BOARD_CONSENSUS_FILTER_BUY,
    TRADE_BOARD_CONSENSUS_FILTER_SELL,
    TRADE_BOARD_CONSENSUS_FILTER_HOLD,
    TRADE_BOARD_CONSENSUS_FILTER_UNKNOWN,
]
TRADE_BOARD_CONSENSUS_FILTER_VALUES = {
    TRADE_BOARD_CONSENSUS_FILTER_HAS: frozenset(
        {TRADE_BOARD_CONSENSUS_BUY, TRADE_BOARD_CONSENSUS_SELL}
    ),
    TRADE_BOARD_CONSENSUS_FILTER_BUY: frozenset({TRADE_BOARD_CONSENSUS_BUY}),
    TRADE_BOARD_CONSENSUS_FILTER_SELL: frozenset({TRADE_BOARD_CONSENSUS_SELL}),
    TRADE_BOARD_CONSENSUS_FILTER_HOLD: frozenset({TRADE_BOARD_CONSENSUS_HOLD}),
    TRADE_BOARD_CONSENSUS_FILTER_UNKNOWN: frozenset({TRADE_BOARD_CONSENSUS_UNKNOWN}),
}

TRADE_BOARD_DOT_SEPARATOR = " · "

TRADE_BOARD_MATRIX_TOOLTIP_STYLE = """
<style>
.av-trade-matrix {
  width: max-content;
  min-width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}
.av-trade-matrix-wrap {
  width: 100%;
  overflow-x: auto;
  overflow-y: visible;
}
.av-trade-matrix th,
.av-trade-matrix td {
  border: 1px solid rgba(49, 51, 63, 0.18);
  padding: 6px 8px;
  vertical-align: top;
}
.av-trade-matrix th {
  background: rgba(240, 242, 246, 0.7);
  font-weight: 600;
}
.av-trade-matrix td {
  position: relative;
  overflow: visible;
}
.av-trade-cell {
  position: relative;
  display: block;
  width: 100%;
  min-height: 1.2em;
}
.av-trade-summary {
  max-width: 520px;
  white-space: normal;
  word-break: break-word;
  overflow-wrap: anywhere;
}
.av-trade-tip {
  visibility: hidden;
  opacity: 0;
  transition: opacity 120ms ease-in-out;
  position: absolute;
  left: 0;
  top: 1.6em;
  z-index: 1000;
  background: rgba(17, 17, 19, 0.98);
  color: #f7f7f7;
  border: 1px solid rgba(255, 255, 255, 0.12);
  border-radius: 12px;
  padding: 10px 12px;
  max-width: min(900px, 90vw);
  max-height: 520px;
  overflow: auto;
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.35);
}
.av-trade-cell:hover .av-trade-tip {
  visibility: visible;
  opacity: 1;
}
.av-trade-tip-title {
  font-size: 12px;
  opacity: 0.85;
  margin: 0 0 8px 0;
}
.av-trade-tip pre {
  margin: 0;
  white-space: pre !important;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas,
    "Liberation Mono", "Courier New", monospace !important;
  font-size: 12px;
  line-height: 1.35;
  font-variant-ligatures: none;
}
.av-trade-empty {
  color: rgba(49, 51, 63, 0.45);
}
</style>
"""

TRADE_BOARD_LIST_STYLE_TEMPLATE = """
<style>
.av-trade-board-header {{
  font-weight: 600;
  opacity: 0.85;
  margin-bottom: 4px;
}}
.av-trade-board-summary {{
  max-width: {summary_width_em}em;
  white-space: normal;
  word-break: break-word;
  overflow-wrap: anywhere;
}}
.av-trade-board-sep {{
  border-bottom: 1px solid rgba(49, 51, 63, 0.12);
  margin: 4px 0 6px 0;
}}
</style>
"""


def format_age_label(max_ts: datetime, ts: datetime) -> str:
    """Return a short age label like '3分钟', '2小时', '5天'."""
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
    """Return a readable action label like '卖 · 中等偏强 · 强度 2'."""
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

    return TRADE_BOARD_DOT_SEPARATOR.join(
        [
            str(action_cn),
            str(strength_text),
            f"强度 {strength_num}",
        ]
    )


def _trade_group_col(assertions_filtered: pd.DataFrame, group_col: str) -> str:
    return group_col if group_col in assertions_filtered.columns else "topic_key"


def _filter_trade_df(assertions_filtered: pd.DataFrame) -> pd.DataFrame:
    if assertions_filtered.empty or "action" not in assertions_filtered.columns:
        return pd.DataFrame()
    trade_mask = assertions_filtered["action"].str.startswith("trade.", na=False)
    return assertions_filtered[trade_mask].copy()


def _unique_post_uids_in_order(df: pd.DataFrame) -> list[str]:
    if df.empty or "post_uid" not in df.columns:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for item in df["post_uid"].tolist():
        uid = "" if pd.isna(item) else str(item or "").strip()
        if not uid or uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return out


def _build_tree_maps(
    view_df: pd.DataFrame,
    *,
    posts_all: pd.DataFrame | None,
) -> tuple[dict[str, str], dict[str, str]]:
    if view_df.empty or "post_uid" not in view_df.columns:
        return {}, {}
    if posts_all is None or posts_all.empty:
        return {}, {}

    selected_uids = _unique_post_uids_in_order(view_df)
    selected_set = set(selected_uids)
    if not selected_set:
        return {}, {}

    threads_all = build_weibo_thread_forest(view_df, posts_all=posts_all)
    if not threads_all:
        return {}, {}

    tree_by_uid: dict[str, str] = {}
    label_by_uid: dict[str, str] = {}
    for thread in threads_all:
        tree_text = str(thread.get("tree_text") or "").rstrip()
        label = str(thread.get("label") or "").strip()
        nodes = thread.get("nodes") or {}
        if not isinstance(nodes, dict):
            continue
        for node in nodes.values():
            uid = str((node or {}).get("post_uid") or "").strip()
            if uid and uid in selected_set:
                tree_by_uid.setdefault(uid, tree_text)
                label_by_uid.setdefault(uid, label)
    return tree_by_uid, label_by_uid


@st.dialog("原文树", width="large")
def _show_tree_dialog(*, tree_label: str, tree_text: str, post_uid: str) -> None:
    label = str(tree_label or "").strip()
    if label:
        st.caption(f"对话：{label}")
    else:
        st.caption("原文树")
    st.code(str(tree_text or "").rstrip(), language="text")
    st.caption(f"post_uid：{str(post_uid or '').strip()}")


def _post_uid_set(posts_all: pd.DataFrame | None) -> set[str]:
    if posts_all is None or posts_all.empty or "post_uid" not in posts_all.columns:
        return set()
    values = posts_all["post_uid"].dropna().astype(str).map(lambda v: v.strip())
    return {v for v in values.tolist() if v}


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


def _trade_data_coverage_days(
    df: pd.DataFrame,
) -> tuple[int, datetime | None, datetime | None]:
    if df.empty or "created_at" not in df.columns:
        return 1, None, None
    created = pd.to_datetime(df["created_at"], errors="coerce")
    created = created.dropna()
    if created.empty:
        return 1, None, None
    min_ts = created.min()
    max_ts = created.max()
    try:
        days = int((max_ts - min_ts).days) + 1
    except Exception:
        days = 1
    return max(1, days), min_ts.to_pydatetime(), max_ts.to_pydatetime()


def _trade_board_settings(*, window_max_days: int) -> tuple[int, str, str, int]:
    row1_a, row1_b, row1_c = st.columns([1, 1, 1])
    window_max_days = max(1, int(window_max_days))
    window_max_days = min(window_max_days, TRADE_BOARD_MAX_WINDOW_DAYS)
    window_key = "trade_board_window_days"
    default_window = min(7, window_max_days)
    if window_key in st.session_state:
        try:
            current = int(st.session_state.get(window_key) or default_window)
        except Exception:
            current = default_window
        st.session_state[window_key] = max(1, min(current, window_max_days))
    with row1_a:
        window_days = st.slider(
            "时间窗（天）",
            1,
            int(window_max_days),
            int(default_window),
            step=1,
            key=window_key,
        )
    with row1_b:
        sort_mode = st.selectbox(
            "排序",
            ["最新", "大佬最多", "共识最强"],
            index=0,
            key="trade_board_sort_mode",
        )

    with row1_c:
        consensus_filter = st.selectbox(
            "共识筛选",
            TRADE_BOARD_CONSENSUS_FILTER_OPTIONS,
            index=0,
            key="trade_board_consensus_filter",
        )

    summary_width_chars = st.slider(
        "总结一行大概多少字",
        20,
        120,
        60,
        step=5,
        key="trade_board_summary_width_chars",
    )

    return (
        int(window_days),
        str(sort_mode),
        str(consensus_filter),
        int(summary_width_chars),
    )


def _prepare_trade_board_df(
    trade_df: pd.DataFrame,
    *,
    group_col: str,
    group_label: str,
    window_days: int,
) -> tuple[pd.DataFrame, datetime] | None:
    board_df = trade_df.dropna(subset=["created_at"]).copy()
    board_df = board_df[board_df[group_col].astype(str).str.strip().ne("")].copy()
    if board_df.empty:
        st.info(f"没有可用的数据（{group_label}/created_at 为空）。")
        return None

    max_ts = board_df["created_at"].max()
    cutoff = max_ts - timedelta(days=int(window_days))
    board_df = board_df[board_df["created_at"] >= cutoff].copy()
    if board_df.empty:
        st.info("时间窗内没有交易观点。")
        return None

    board_df["strength"] = (
        pd.to_numeric(board_df["action_strength"], errors="coerce")
        .fillna(0)
        .astype(int)
        .clip(lower=0, upper=3)
    )
    board_df["buy_strength"] = board_df["strength"].where(
        board_df["action"].isin(TRADE_BUY_ACTIONS), 0
    )
    board_df["sell_strength"] = board_df["strength"].where(
        board_df["action"].isin(TRADE_SELL_ACTIONS), 0
    )
    board_df["hold_mentions"] = board_df["action"].isin(TRADE_HOLD_ACTIONS).astype(int)
    return board_df, max_ts


def _build_trade_board_agg(
    board_df: pd.DataFrame, *, group_col: str, max_ts: datetime
) -> pd.DataFrame:
    agg = board_df.groupby(group_col, as_index=False).agg(
        买强度=("buy_strength", "sum"),
        卖强度=("sell_strength", "sum"),
        只看次数=("hold_mentions", "sum"),
        提及次数=(group_col, "count"),
        大佬数=("author", "nunique"),
        最近时间=("created_at", "max"),
    )
    agg["净强度"] = agg["买强度"] - agg["卖强度"]

    last_rows = (
        board_df.sort_values(by="created_at")
        .groupby(group_col)
        .tail(1)
        .set_index(group_col)
    )
    last_badge = last_rows.apply(
        lambda row: trade_action_badge(row["action"], row["strength"]), axis=1
    )
    agg["post_uid"] = agg[group_col].map(last_rows["post_uid"])
    agg["最近大佬"] = agg[group_col].map(last_rows["author"])
    agg["最近动作"] = agg[group_col].map(last_badge)
    agg["最近摘要"] = (
        agg[group_col]
        .map(last_rows["summary"])
        .fillna("")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
    )
    agg["url"] = agg[group_col].map(last_rows["url"])
    agg["最近"] = agg["最近时间"].apply(lambda ts: format_age_label(max_ts, ts))

    consensus = pd.Series([TRADE_BOARD_CONSENSUS_UNKNOWN] * len(agg), index=agg.index)
    consensus = consensus.mask(agg["净强度"] > 0, TRADE_BOARD_CONSENSUS_BUY)
    consensus = consensus.mask(agg["净强度"] < 0, TRADE_BOARD_CONSENSUS_SELL)
    consensus = consensus.mask(
        (agg["净强度"] == 0)
        & (agg["买强度"] == 0)
        & (agg["卖强度"] == 0)
        & (agg["只看次数"] > 0),
        TRADE_BOARD_CONSENSUS_HOLD,
    )
    agg["共识"] = consensus
    return agg


def _sort_trade_board_agg(agg: pd.DataFrame, *, sort_mode: str) -> pd.DataFrame:
    if sort_mode == "最新":
        return agg.sort_values(by="最近时间", ascending=False)
    if sort_mode == "大佬最多":
        return agg.sort_values(by=["大佬数", "提及次数", "最近时间"], ascending=False)
    return (
        agg.assign(_abs_net=agg["净强度"].abs())
        .sort_values(by=["_abs_net", "大佬数", "最近时间"], ascending=False)
        .drop(columns=["_abs_net"])
    )


def _filter_trade_board_agg_by_consensus(
    agg_sorted: pd.DataFrame, *, consensus_filter: str
) -> pd.DataFrame:
    values = TRADE_BOARD_CONSENSUS_FILTER_VALUES.get(
        str(consensus_filter or "").strip()
    )
    if not values or agg_sorted.empty or "共识" not in agg_sorted.columns:
        return agg_sorted
    return agg_sorted[agg_sorted["共识"].astype(str).isin(values)].copy()


def _render_trade_kpis(
    agg_sorted: pd.DataFrame, *, board_df: pd.DataFrame, group_label: str
) -> None:
    kpi_left, kpi_mid, kpi_right = st.columns(3)
    kpi_left.metric(f"{group_label}数", f"{len(agg_sorted)}")
    kpi_mid.metric("大佬数", f"{int(board_df['author'].nunique())}")
    kpi_right.metric(
        "有共识",
        f"{int((agg_sorted['共识'].isin([TRADE_BOARD_CONSENSUS_BUY, TRADE_BOARD_CONSENSUS_SELL])).sum())}",
    )


def _render_top_assets(
    agg_sorted: pd.DataFrame,
    *,
    board_view: pd.DataFrame,
    group_col: str,
    group_label: str,
    posts_all: pd.DataFrame | None = None,
    summary_width_chars: int,
) -> pd.DataFrame:
    top_assets = agg_sorted.copy()
    if top_assets.empty:
        st.info(f"没有{group_label}数据。")
        return top_assets

    available_post_uids = _post_uid_set(posts_all)
    candidates_by_topic = _build_latest_post_uid_candidates(
        board_view, group_col=group_col
    )

    tree_post_uids = top_assets.get("post_uid", pd.Series(dtype=object)).copy()
    tree_post_uids = tree_post_uids.apply(lambda v: "" if pd.isna(v) else str(v))
    tree_post_uids = tree_post_uids.astype(str).str.strip()

    if available_post_uids and candidates_by_topic:
        for idx, row in top_assets.iterrows():
            current_uid = str(tree_post_uids.get(idx, "") or "").strip()
            if current_uid and current_uid in available_post_uids:
                continue
            topic_key = str(row.get(group_col) or "").strip()
            for cand_uid in candidates_by_topic.get(topic_key, []):
                if cand_uid in available_post_uids:
                    tree_post_uids.at[idx] = cand_uid
                    break

    tree_view = top_assets.copy()
    tree_view["post_uid"] = tree_post_uids
    tree_by_uid, label_by_uid = _build_tree_maps(tree_view, posts_all=posts_all)

    # Note: We use CSS `em` as a rough "character width" for Chinese text.
    summary_width_em = max(10, int(summary_width_chars))
    st.markdown(
        TRADE_BOARD_LIST_STYLE_TEMPLATE.format(summary_width_em=summary_width_em),
        unsafe_allow_html=True,
    )

    col_sizes = [2, 1, 6, 1, 1, 2, 1, 2, 4]
    (
        header_topic,
        header_consensus,
        header_summary,
        header_tree,
        header_link,
        header_recent_action,
        header_recent_time,
        header_recent_author,
        header_stats,
    ) = st.columns(col_sizes)
    with header_topic:
        st.markdown(
            f"<div class='av-trade-board-header'>{_escape_html(group_label)}</div>",
            unsafe_allow_html=True,
        )
    with header_consensus:
        st.markdown(
            "<div class='av-trade-board-header'>共识</div>", unsafe_allow_html=True
        )
    with header_summary:
        st.markdown(
            "<div class='av-trade-board-header'>总结</div>", unsafe_allow_html=True
        )
    with header_tree:
        st.markdown(
            "<div class='av-trade-board-header'>树</div>", unsafe_allow_html=True
        )
    with header_link:
        st.markdown(
            "<div class='av-trade-board-header'>链接</div>", unsafe_allow_html=True
        )
    with header_recent_action:
        st.markdown(
            "<div class='av-trade-board-header'>最近动作</div>", unsafe_allow_html=True
        )
    with header_recent_time:
        st.markdown(
            "<div class='av-trade-board-header'>最近时间</div>", unsafe_allow_html=True
        )
    with header_recent_author:
        st.markdown(
            "<div class='av-trade-board-header'>最近大佬</div>", unsafe_allow_html=True
        )
    with header_stats:
        st.markdown(
            "<div class='av-trade-board-header'>统计</div>", unsafe_allow_html=True
        )

    for idx, row in top_assets.iterrows():
        topic = str(row.get(group_col) or "").strip()
        consensus = str(row.get("共识") or "").strip()
        summary = str(row.get("最近摘要") or "").strip()
        url = str(row.get("url") or "").strip()
        tree_post_uid = str(tree_post_uids.get(idx, "") or "").strip()

        (
            col_topic,
            col_consensus,
            col_summary,
            col_tree,
            col_link,
            col_recent_action,
            col_recent_time,
            col_recent_author,
            col_stats,
        ) = st.columns(col_sizes)
        with col_topic:
            st.markdown(f"<b>{_escape_html(topic)}</b>", unsafe_allow_html=True)
        with col_consensus:
            st.write(consensus)
        with col_summary:
            st.markdown(
                f"<div class='av-trade-board-summary'>{_escape_html(summary)}</div>",
                unsafe_allow_html=True,
            )
        with col_tree:
            tree_text = str(tree_by_uid.get(tree_post_uid, "") or "").rstrip()
            tree_label = str(label_by_uid.get(tree_post_uid, "") or "").strip()
            if tree_text.strip():
                tree_key = f"trade_board:tree:{idx}:{tree_post_uid}"
                if st.button("树", key=tree_key):
                    _show_tree_dialog(
                        tree_label=tree_label,
                        tree_text=tree_text,
                        post_uid=tree_post_uid,
                    )
            else:
                with st.popover("无", width="stretch"):
                    st.caption("没有树：找不到原帖数据。")
                    if not tree_post_uid:
                        st.write("原因：这条的 post_uid 是空的。")
                    elif (
                        available_post_uids and tree_post_uid not in available_post_uids
                    ):
                        st.write("原因：posts 里没有这条 post_uid。")
                        st.code(tree_post_uid, language="text")
                    else:
                        st.write("原因：posts 有，但 tree 算不出来。")
        with col_link:
            if url:
                st.link_button("打开", url, width="content", type="tertiary")
            else:
                st.write("—")

        recent_action = str(row.get("最近动作") or "").strip()
        recent_author = str(row.get("最近大佬") or "").strip()
        recent_age = str(row.get("最近") or "").strip()
        net_strength = row.get("净强度")
        buy_strength = row.get("买强度")
        sell_strength = row.get("卖强度")
        mentions = row.get("提及次数")
        author_count = row.get("大佬数")

        with col_recent_action:
            st.write(recent_action)
        with col_recent_time:
            st.write(recent_age)
        with col_recent_author:
            st.write(recent_author)
        with col_stats:
            st.write(
                TRADE_BOARD_DOT_SEPARATOR.join(
                    [
                        item
                        for item in [
                            f"净强度 {net_strength}",
                            f"买强度 {buy_strength}",
                            f"卖强度 {sell_strength}",
                            f"提及次数 {mentions}",
                            f"大佬数 {author_count}",
                        ]
                        if str(item or "").strip()
                    ]
                )
            )
        st.markdown("<div class='av-trade-board-sep'></div>", unsafe_allow_html=True)

    return top_assets


def _top_authors(board_df: pd.DataFrame) -> list[str]:
    if "invest_score" in board_df.columns:
        score_series = pd.to_numeric(board_df["invest_score"], errors="coerce").fillna(
            0.0
        )
    else:
        score_series = pd.Series(0.0, index=board_df.index)

    author_stats = (
        board_df.assign(_score=score_series)
        .groupby("author", as_index=False)
        .agg(分数=("_score", "max"), 提及=("author", "count"))
    )
    author_stats = author_stats[author_stats["author"].astype(str).str.strip().ne("")]
    author_stats = author_stats.sort_values(by=["分数", "提及"], ascending=False)
    return author_stats["author"].tolist()


def _escape_html(value: object) -> str:
    return _html.escape(str(value or ""), quote=True)


def _trade_matrix_cell_html(
    cell_text: object,
    *,
    tree_text: str,
    tree_label: str,
) -> str:
    cell = str(cell_text or "").strip()
    if not cell:
        return "<span class='av-trade-empty'> </span>"

    cell_escaped = _escape_html(cell)
    tree = str(tree_text or "").rstrip()
    if not tree.strip():
        return f"<div class='av-trade-cell'>{cell_escaped}</div>"

    label = str(tree_label or "").strip()
    title_html = (
        f"<div class='av-trade-tip-title'>对话：{_escape_html(label)}</div>"
        if label
        else "<div class='av-trade-tip-title'>原文树</div>"
    )
    return (
        "<div class='av-trade-cell'>"
        f"{cell_escaped}"
        "<div class='av-trade-tip'>"
        f"{title_html}"
        f"<pre>{_escape_html(tree)}</pre>"
        "</div>"
        "</div>"
    )


def _render_trade_matrix(
    board_df: pd.DataFrame,
    *,
    top_asset_keys: list,
    top_author_list: list[str],
    group_col: str,
    group_label: str,
    max_ts: datetime,
) -> None:
    if not top_asset_keys:
        st.info(f"没有{group_label}数据。")
        return

    if not top_author_list:
        st.info("没有大佬数据（author 为空）。")
        return

    pair_df = board_df[
        board_df[group_col].isin(top_asset_keys)
        & board_df["author"].isin(top_author_list)
    ].copy()
    if pair_df.empty:
        st.info("当前条件下，作业格没有数据。")
        return

    norm_assets = [str(x or "").strip() for x in top_asset_keys if str(x or "").strip()]
    norm_authors = [
        str(x or "").strip() for x in top_author_list if str(x or "").strip()
    ]
    if not norm_assets or not norm_authors:
        st.info("当前条件下，作业格没有可用的行/列。")
        return

    pair_last = (
        pair_df.sort_values(by="created_at")
        .groupby([group_col, "author"])
        .tail(1)
        .copy()
    )
    pair_last["badge"] = pair_last.apply(
        lambda row: trade_action_badge(row["action"], row["strength"]), axis=1
    )
    pair_last["age"] = pair_last["created_at"].apply(
        lambda ts: format_age_label(max_ts, ts)
    )
    pair_last["cell"] = (
        pair_last["badge"] + TRADE_BOARD_DOT_SEPARATOR + pair_last["age"]
    )

    cell_by_pair: dict[tuple[str, str], str] = {}
    for _, row in pair_last.iterrows():
        asset = str(row.get(group_col) or "").strip()
        author = str(row.get("author") or "").strip()
        if not asset or not author:
            continue
        key = (asset, author)
        cell_by_pair[key] = str(row.get("cell") or "").strip()

    parts: list[str] = [
        TRADE_BOARD_MATRIX_TOOLTIP_STYLE,
        "<div class='av-trade-matrix-wrap'>",
        "<table class='av-trade-matrix'>",
        "<thead><tr>",
        f"<th>{_escape_html(group_label)}</th>",
    ]
    for author in norm_authors:
        parts.append(f"<th>{_escape_html(author)}</th>")
    parts += ["</tr></thead>", "<tbody>"]

    for asset in norm_assets:
        parts.append("<tr>")
        parts.append(f"<td>{_escape_html(asset)}</td>")
        for author in norm_authors:
            key = (asset, author)
            cell_html = _trade_matrix_cell_html(
                cell_by_pair.get(key, ""),
                tree_text="",
                tree_label="",
            )
            parts.append(f"<td>{cell_html}</td>")
        parts.append("</tr>")

    parts += ["</tbody></table>", "</div>"]
    st.markdown("\n".join(parts), unsafe_allow_html=True)


def _render_trade_detail(
    board_df: pd.DataFrame, *, top_asset_keys: list, group_col: str, group_label: str
) -> None:
    st.markdown(f"**{group_label}细节（点开看最近几条）**")
    if not top_asset_keys:
        st.info(f"没有{group_label}数据。")
        return

    selected_key = st.selectbox(
        f"选择{group_label}",
        options=top_asset_keys,
        index=0,
        key="trade_board_selected_topic",
    )
    detail_n = st.slider(
        "显示条数",
        5,
        50,
        10,
        step=5,
        key="trade_board_detail_n",
    )
    detail_df = board_df[board_df[group_col] == selected_key].copy()
    detail_df = detail_df.sort_values(by="created_at", ascending=False).head(
        int(detail_n)
    )
    detail_df["动作"] = detail_df.apply(
        lambda row: trade_action_badge(row["action"], row["strength"]), axis=1
    )
    detail_df["原文"] = (
        detail_df["raw_text"]
        .fillna("")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.slice(0, 120)
    )
    detail_cols = [
        "created_at",
        "author",
        "source",
        "动作",
        "summary",
        "confidence",
        "原文",
        "url",
    ]
    if group_col != "topic_key" and "topic_key" in detail_df.columns:
        detail_cols.insert(3, "topic_key")
    st.dataframe(
        detail_df[detail_cols],
        width="stretch",
        hide_index=True,
        column_config={
            "url": st.column_config.LinkColumn("链接", display_text="打开"),
        },
    )


def show_trade_flow(
    assertions_filtered: pd.DataFrame,
    *,
    group_col: str,
    group_label: str,
    posts_all: pd.DataFrame | None = None,
) -> None:
    group_col = _trade_group_col(assertions_filtered, group_col)
    trade_df = _filter_trade_df(assertions_filtered)
    if trade_df.empty:
        st.info("没有交易类观点。")
        return
    st.markdown("**作业板（抄作业用，一眼看懂）**")

    coverage_days, min_ts, max_ts = _trade_data_coverage_days(trade_df)
    if min_ts and max_ts:
        st.caption(
            f"当前交易观点时间：{min_ts.date()} ~ {max_ts.date()}（{coverage_days} 天）。"
        )
    window_days, sort_mode, consensus_filter, summary_width_chars = (
        _trade_board_settings(window_max_days=coverage_days)
    )
    prepared = _prepare_trade_board_df(
        trade_df,
        group_col=group_col,
        group_label=group_label,
        window_days=window_days,
    )
    if prepared is None:
        return
    board_df, max_ts = prepared

    agg = _build_trade_board_agg(board_df, group_col=group_col, max_ts=max_ts)
    agg_sorted = _sort_trade_board_agg(agg, sort_mode=sort_mode)
    agg_view = _filter_trade_board_agg_by_consensus(
        agg_sorted, consensus_filter=consensus_filter
    )
    view_keys = set(
        str(x).strip()
        for x in agg_view.get(group_col, pd.Series(dtype=str)).dropna().tolist()
        if str(x).strip()
    )
    board_view = (
        board_df[board_df[group_col].astype(str).str.strip().isin(view_keys)].copy()
        if view_keys
        else board_df.head(0).copy()
    )

    _render_trade_kpis(agg_view, board_df=board_view, group_label=group_label)
    top_assets = _render_top_assets(
        agg_view,
        board_view=board_view,
        group_col=group_col,
        group_label=group_label,
        posts_all=posts_all,
        summary_width_chars=summary_width_chars,
    )

    st.markdown(f"**作业格（大佬 × {group_label}）**")
    top_asset_keys = top_assets[group_col].tolist()
    top_author_list = _top_authors(board_view)
    _render_trade_matrix(
        board_view,
        top_asset_keys=top_asset_keys,
        top_author_list=top_author_list,
        group_col=group_col,
        group_label=group_label,
        max_ts=max_ts,
    )
    _render_trade_detail(
        board_view,
        top_asset_keys=top_asset_keys,
        group_col=group_col,
        group_label=group_label,
    )
