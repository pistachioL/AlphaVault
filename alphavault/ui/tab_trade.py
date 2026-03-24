"""
Streamlit tab: trade flow.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import html as _html
import math

import pandas as pd
import streamlit as st

from alphavault.ui.thread_tree import build_weibo_thread_forest

TRADE_BUY_ACTIONS = frozenset({"trade.buy", "trade.add"})
TRADE_SELL_ACTIONS = frozenset({"trade.sell", "trade.reduce"})
TRADE_HOLD_ACTIONS = frozenset({"trade.hold"})

DEFAULT_TRADE_FLOW_PAGE_SIZE = 5
MAX_TRADE_FLOW_PAGE_SIZE = 20
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
  white-space: pre;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas,
    "Liberation Mono", "Courier New", monospace;
  font-size: 12px;
  line-height: 1.35;
}
.av-trade-empty {
  color: rgba(49, 51, 63, 0.45);
}
</style>
"""


def format_age_label(max_ts: datetime, ts: datetime) -> str:
    """Return a short age label like '3m', '2h', '5d'."""
    if not isinstance(ts, datetime) or not isinstance(max_ts, datetime):
        return ""
    delta = max_ts - ts
    if delta.total_seconds() < 0:
        delta = timedelta(seconds=0)
    minutes = int(delta.total_seconds() // 60)
    if minutes < 60:
        return f"{minutes}m"
    hours = int(minutes // 60)
    if hours < 48:
        return f"{hours}h"
    days = int(hours // 24)
    return f"{days}d"


def trade_action_badge(action: str, strength: object) -> str:
    """Return a readable action label like 'trade.sell-卖-中等偏强-强度2'."""
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

    action_cn = ""
    if action_str in TRADE_BUY_ACTIONS:
        action_cn = "买"
    elif action_str in TRADE_SELL_ACTIONS:
        action_cn = "卖"
    elif action_str in TRADE_HOLD_ACTIONS:
        action_cn = "看"
    elif action_str.startswith("trade."):
        action_cn = "交易"

    if action_cn:
        return f"{action_str}-{action_cn}-{strength_text}-强度{strength_num}"
    if action_str:
        return f"{action_str}-{strength_text}-强度{strength_num}"
    return f"交易-{strength_text}-强度{strength_num}"


def _trade_group_col(assertions_filtered: pd.DataFrame, group_col: str) -> str:
    return group_col if group_col in assertions_filtered.columns else "topic_key"


def _filter_trade_df(assertions_filtered: pd.DataFrame) -> pd.DataFrame:
    if assertions_filtered.empty or "action" not in assertions_filtered.columns:
        return pd.DataFrame()
    trade_mask = assertions_filtered["action"].str.startswith("trade.", na=False)
    return assertions_filtered[trade_mask].copy()


def _format_ts(value: object) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    try:
        return ts.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts)


def _unique_post_uids_in_order(df: pd.DataFrame) -> list[str]:
    if df.empty or "post_uid" not in df.columns:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for item in df["post_uid"].tolist():
        uid = str(item or "").strip()
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


def _paginate_rows(total: int, *, key_prefix: str) -> tuple[int, int, int, int]:
    if total <= 0:
        return 1, 1, 0, 0

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        page_size = int(
            st.number_input(
                "一页多少条",
                min_value=1,
                max_value=MAX_TRADE_FLOW_PAGE_SIZE,
                value=int(DEFAULT_TRADE_FLOW_PAGE_SIZE),
                step=1,
                key=f"{key_prefix}:page_size",
            )
        )

    total_pages = max(1, int(math.ceil(total / max(1, page_size))))
    page_key = f"{key_prefix}:page"
    if page_key in st.session_state:
        try:
            current = int(st.session_state.get(page_key) or 1)
        except Exception:
            current = 1
        st.session_state[page_key] = max(1, min(current, total_pages))
    with col2:
        page = st.selectbox(
            "第几页",
            list(range(1, total_pages + 1)),
            index=0,
            key=page_key,
        )
    with col3:
        st.caption(f"共有 {total} 条")

    start_idx = (int(page) - 1) * page_size
    end_idx = min(start_idx + page_size, total)
    st.caption(f"本页：{start_idx + 1} - {end_idx}")
    return int(page_size), int(page), int(start_idx), int(end_idx)


def _render_recent_trade_flow(
    trade_df: pd.DataFrame,
    *,
    group_col: str,
    group_label: str,
    posts_all: pd.DataFrame | None,
) -> None:
    st.markdown("**最近交易流（按时间倒序）**")
    trade_view = trade_df.sort_values(by="created_at", ascending=False)
    if trade_view.empty:
        st.info("没有交易观点。")
        return

    total = len(trade_view)
    _, _, start_idx, end_idx = _paginate_rows(total, key_prefix="trade_flow")
    page_df = trade_view.iloc[start_idx:end_idx].copy()

    tree_by_uid, label_by_uid = _build_tree_maps(page_df, posts_all=posts_all)
    if posts_all is None or posts_all.empty:
        st.info("没有帖子数据，tree 画不出来。")

    for i, (_, row) in enumerate(page_df.iterrows(), start=1):
        post_uid = str(row.get("post_uid") or "").strip()
        created = _format_ts(row.get("created_at"))
        author = str(row.get("author") or "").strip()
        action_badge = trade_action_badge(row.get("action"), row.get("action_strength"))
        summary = str(row.get("summary") or "").strip()
        group_val = str(row.get(group_col) or "").strip()
        topic_key = str(row.get("topic_key") or "").strip()
        confidence = row.get("confidence")
        url = str(row.get("url") or "").strip()

        title = summary if summary else action_badge
        st.markdown(f"**{action_badge} {title}**")

        meta_parts = [p for p in [created, author] if p]
        if group_val:
            meta_parts.append(f"{group_label}：{group_val}")
        if topic_key and topic_key != group_val:
            meta_parts.append(f"主题：{topic_key}")
        if confidence is not None and str(confidence).strip():
            meta_parts.append(f"置信度：{confidence}")
        if meta_parts:
            st.caption(" · ".join(meta_parts))

        if url:
            st.link_button(
                f"打开链接（{start_idx + i}）",
                url,
                type="secondary",
                width="content",
            )

        label = label_by_uid.get(post_uid, "")
        if label:
            st.caption(f"对话：{label}")

        tree_text = tree_by_uid.get(post_uid, "").rstrip()
        if tree_text.strip():
            st.code(tree_text, language="text")
        else:
            st.info("tree 为空。")

        if i < len(page_df):
            st.divider()


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


def _trade_board_settings(*, window_max_days: int) -> tuple[int, str, str]:
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

    return (
        int(window_days),
        str(sort_mode),
        str(consensus_filter),
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
    agg["最近大佬"] = agg[group_col].map(last_rows["author"])
    agg["最近动作"] = agg[group_col].map(last_badge)
    agg["最近摘要"] = (
        agg[group_col]
        .map(last_rows["summary"])
        .fillna("")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.slice(0, 60)
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
    group_col: str,
    group_label: str,
) -> pd.DataFrame:
    top_assets = agg_sorted.copy()
    st.dataframe(
        top_assets[
            [
                group_col,
                "共识",
                "净强度",
                "买强度",
                "卖强度",
                "提及次数",
                "大佬数",
                "最近",
                "最近动作",
                "最近大佬",
                "最近摘要",
                "url",
            ]
        ].rename(columns={group_col: group_label}),
        width="stretch",
        hide_index=True,
        column_config={
            "url": st.column_config.LinkColumn("链接", display_text="打开"),
        },
    )
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
        return f"<span class='av-trade-cell'>{cell_escaped}</span>"

    label = str(tree_label or "").strip()
    title_html = (
        f"<div class='av-trade-tip-title'>对话：{_escape_html(label)}</div>"
        if label
        else "<div class='av-trade-tip-title'>原文树</div>"
    )
    return (
        "<span class='av-trade-cell'>"
        f"{cell_escaped}"
        "<span class='av-trade-tip'>"
        f"{title_html}"
        f"<pre>{_escape_html(tree)}</pre>"
        "</span>"
        "</span>"
    )


def _render_trade_matrix(
    board_df: pd.DataFrame,
    *,
    top_asset_keys: list,
    top_author_list: list[str],
    group_col: str,
    group_label: str,
    max_ts: datetime,
    posts_all: pd.DataFrame | None = None,
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
    pair_last["cell"] = pair_last["badge"] + " " + pair_last["age"]

    tree_by_uid, label_by_uid = _build_tree_maps(pair_last, posts_all=posts_all)

    cell_by_pair: dict[tuple[str, str], str] = {}
    tree_by_pair: dict[tuple[str, str], str] = {}
    label_by_pair: dict[tuple[str, str], str] = {}
    for _, row in pair_last.iterrows():
        asset = str(row.get(group_col) or "").strip()
        author = str(row.get("author") or "").strip()
        if not asset or not author:
            continue
        key = (asset, author)
        cell_by_pair[key] = str(row.get("cell") or "").strip()
        post_uid = str(row.get("post_uid") or "").strip()
        if post_uid:
            tree = str(tree_by_uid.get(post_uid, "") or "").rstrip()
            if tree.strip():
                tree_by_pair[key] = tree
                label_by_pair[key] = str(label_by_uid.get(post_uid, "") or "").strip()

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
                tree_text=tree_by_pair.get(key, ""),
                tree_label=label_by_pair.get(key, ""),
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
    _render_recent_trade_flow(
        trade_df,
        group_col=group_col,
        group_label=group_label,
        posts_all=posts_all,
    )

    st.divider()
    st.markdown("**作业板（抄作业用，一眼看懂）**")

    coverage_days, min_ts, max_ts = _trade_data_coverage_days(trade_df)
    if min_ts and max_ts:
        st.caption(
            f"当前交易观点时间：{min_ts.date()} ~ {max_ts.date()}（{coverage_days} 天）。"
        )
    window_days, sort_mode, consensus_filter = _trade_board_settings(
        window_max_days=coverage_days
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
        group_col=group_col,
        group_label=group_label,
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
        posts_all=posts_all,
    )
    _render_trade_detail(
        board_view,
        top_asset_keys=top_asset_keys,
        group_col=group_col,
        group_label=group_label,
    )
