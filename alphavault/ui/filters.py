from __future__ import annotations

"""
Streamlit sidebar filters.

Keep: build_filters(posts, assertions) -> (posts_filtered, assertions_filtered, meta)
"""

from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from alphavault.topic_cluster import UNCATEGORIZED_LABEL


def build_filters(
    posts: pd.DataFrame,
    assertions: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    st.sidebar.header("筛选条件")

    group_by_cluster = False
    if "cluster_display" in assertions.columns:
        group_by_cluster = st.sidebar.checkbox(
            "按板块看（聚合）", value=False, key="filter_group_by_cluster"
        )
    group_col = "cluster_display" if group_by_cluster else "topic_key"
    group_label = "板块" if group_by_cluster else "主题"

    posts_filtered = posts.copy()
    required_post_cols = {
        "post_uid": "",
        "created_at": pd.NaT,
        "author": "",
        "status": "",
        "invest_score": 0.0,
        "url": "",
        "raw_text": "",
        "source": "unknown",
        "platform": "",
    }
    for col, default in required_post_cols.items():
        if col not in posts_filtered.columns:
            posts_filtered[col] = default

    date_col = "created_at"
    if not posts_filtered["created_at"].notna().any():
        date_col = "ingested_at"

    if posts_filtered[date_col].notna().any():
        min_date = posts_filtered[date_col].min().date()
        max_date = posts_filtered[date_col].max().date()
    else:
        max_date = date.today()
        min_date = max_date - timedelta(days=30)

    default_start = max(min_date, max_date - timedelta(days=30))
    selected_range = st.sidebar.date_input(
        "日期范围",
        value=(default_start, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(selected_range, tuple):
        start_date, end_date = selected_range
    else:
        start_date = selected_range
        end_date = selected_range

    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    posts_filtered = posts_filtered[
        (posts_filtered[date_col] >= start_dt) & (posts_filtered[date_col] <= end_dt)
    ]

    source_options = (
        sorted(posts["source"].dropna().unique().tolist())
        if "source" in posts.columns
        else []
    )
    if source_options:
        selected_sources = st.sidebar.multiselect("来源", source_options, default=source_options)
        if selected_sources:
            posts_filtered = posts_filtered[posts_filtered["source"].isin(selected_sources)]

    platform_options = (
        sorted(posts["platform"].dropna().unique().tolist())
        if "platform" in posts.columns
        else []
    )
    if platform_options:
        selected_platforms = st.sidebar.multiselect("平台", platform_options, default=None)
        if selected_platforms:
            posts_filtered = posts_filtered[posts_filtered["platform"].isin(selected_platforms)]

    status_options = sorted(posts["status"].dropna().unique().tolist())
    default_status = ["relevant"] if "relevant" in status_options else status_options
    selected_status = st.sidebar.multiselect("状态", status_options, default=default_status)
    if selected_status:
        posts_filtered = posts_filtered[posts_filtered["status"].isin(selected_status)]

    author_options = sorted(posts["author"].dropna().unique().tolist())
    selected_authors = st.sidebar.multiselect("作者", author_options, default=None)
    if selected_authors:
        posts_filtered = posts_filtered[posts_filtered["author"].isin(selected_authors)]

    keyword = st.sidebar.text_input("关键词 / 片段", value="").strip()
    if keyword:
        posts_filtered = posts_filtered[
            posts_filtered["raw_text"].str.contains(keyword, case=False, na=False)
        ]

    exclude_reposts = st.sidebar.checkbox("排除转发链", value=False)
    if exclude_reposts:
        posts_filtered = posts_filtered[~posts_filtered["repost_flag"]]

    required_assert_cols = {
        "post_uid": "",
        "topic_key": "",
        "action": "",
        "action_strength": 0,
        "summary": "",
        "evidence": "",
        "confidence": 0.0,
        "stock_codes_json": "[]",
        "stock_names_json": "[]",
        "industries_json": "[]",
        "commodities_json": "[]",
        "indices_json": "[]",
        "author": "",
        "created_at": pd.NaT,
    }
    for col, default in required_assert_cols.items():
        if col not in assertions.columns:
            assertions[col] = default

    assertions_joined = assertions.merge(
        posts_filtered[
            [
                "post_uid",
                "created_at",
                "author",
                "status",
                "invest_score",
                "url",
                "raw_text",
                "source",
                "platform",
            ]
        ],
        on="post_uid",
        how="inner",
    )
    for col in ["created_at", "author", "status", "invest_score", "url", "raw_text", "source"]:
        col_x = f"{col}_x"
        col_y = f"{col}_y"
        if col not in assertions_joined.columns:
            if col_x in assertions_joined.columns and col_y in assertions_joined.columns:
                assertions_joined[col] = assertions_joined[col_x].fillna(assertions_joined[col_y])
            elif col_x in assertions_joined.columns:
                assertions_joined[col] = assertions_joined[col_x]
            elif col_y in assertions_joined.columns:
                assertions_joined[col] = assertions_joined[col_y]
            else:
                assertions_joined[col] = ""

    show_uncategorized = True
    if group_by_cluster:
        show_uncategorized = st.sidebar.checkbox(
            "显示未归类", value=True, key="filter_show_uncategorized"
        )
        if not show_uncategorized and "cluster_display" in assertions_joined.columns:
            assertions_joined = assertions_joined[
                assertions_joined["cluster_display"] != UNCATEGORIZED_LABEL
            ]

    group_options = (
        sorted(assertions[group_col].dropna().unique().tolist())
        if group_col in assertions.columns
        else []
    )
    if group_by_cluster and not show_uncategorized:
        group_options = [
            item for item in group_options if str(item).strip() != UNCATEGORIZED_LABEL
        ]
    selected_groups = st.sidebar.multiselect(group_label, group_options, default=None)
    if selected_groups and group_col in assertions_joined.columns:
        assertions_joined = assertions_joined[assertions_joined[group_col].isin(selected_groups)]

    action_options = sorted(assertions["action"].dropna().unique().tolist())
    selected_actions = st.sidebar.multiselect("动作", action_options, default=None)
    if selected_actions:
        assertions_joined = assertions_joined[assertions_joined["action"].isin(selected_actions)]

    strength_range = st.sidebar.slider("动作强度", 0, 3, (0, 3), step=1)
    assertions_joined = assertions_joined[
        (assertions_joined["action_strength"] >= strength_range[0])
        & (assertions_joined["action_strength"] <= strength_range[1])
    ]

    conf_range = st.sidebar.slider("置信度区间", 0.0, 1.0, (0.0, 1.0), step=0.05)
    assertions_joined = assertions_joined[
        (assertions_joined["confidence"] >= conf_range[0])
        & (assertions_joined["confidence"] <= conf_range[1])
    ]

    only_with_assertions = st.sidebar.checkbox("仅显示有观点的帖子", value=True)
    if only_with_assertions:
        posts_filtered = posts_filtered[
            posts_filtered["post_uid"].isin(assertions_joined["post_uid"].unique())
        ]

    meta = {
        "date_range": (start_date, end_date),
        "keyword": keyword,
        "only_with_assertions": only_with_assertions,
        "date_col": date_col,
        "group_by_cluster": group_by_cluster,
        "group_col": group_col,
        "group_label": group_label,
    }
    return posts_filtered, assertions_joined, meta
