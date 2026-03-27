from __future__ import annotations

import streamlit as st

from alphavault.env import load_dotenv_if_present
from alphavault.ui.data import (
    enrich_assertions,
    enrich_posts,
    load_sources,
)
from alphavault.ui.tab_misc import (
    show_conflicts_and_changes,
    show_learning_library,
    show_tables,
    show_topic_timeline,
)
from alphavault.ui.tab_logs import show_logs
from alphavault.ui.tab_overview import show_kpis, show_overview_charts
from alphavault.ui.tab_risk import show_risk_radar

load_dotenv_if_present()

DEFAULT_GROUP_COL = "topic_key"
DEFAULT_GROUP_LABEL = "主题"


def main() -> None:
    st.set_page_config(
        page_title="AlphaVault 观点可视化",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700&family=Zilla+Slab:wght@600&display=swap');
        :root {
          --ink: #0f172a;
          --muted: #475569;
          --accent: #ff6a3d;
          --bg: #f5f1e9;
          --panel: #ffffff;
        }
        html, body, [class*="css"] {
          font-family: 'Manrope', 'Trebuchet MS', sans-serif;
          color: var(--ink);
        }
        .block-container {
          padding-top: 1.5rem;
          padding-bottom: 3rem;
        }
        .app-hero {
          background: linear-gradient(120deg, #fff5e8 0%, #f0f7ff 100%);
          border: 1px solid #f1e4d4;
          border-radius: 18px;
          padding: 18px 24px;
          margin-bottom: 18px;
        }
        .app-hero h1 {
          font-family: 'Zilla Slab', serif;
          margin: 0;
          font-size: 32px;
        }
        .app-hero p {
          margin: 6px 0 0 0;
          color: var(--muted);
        }
        .stSidebar {
          background: radial-gradient(circle at top, #fff4eb 0%, #f7f3ec 60%, #f2efe9 100%);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="app-hero">
          <h1>AlphaVault · 观点可视化</h1>
          <p>基于 posts / assertions 的交易流、风险雷达、主题时间线与学习库。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    pages = [
        "总览",
        "交易流",
        "风险雷达",
        "主题时间线",
        "关注页",
        "主题聚合",
        "学习库",
        "冲突/变化",
        "日志",
        "数据表",
    ]

    posts, assertions, missing = load_sources()
    if missing:
        st.error("Turso 没配好，或者连不上。")
        st.info(f"缺少/错误：{', '.join(missing)}")
        st.stop()
    if posts.empty:
        st.warning("Turso 里还没有“已处理”的数据（processed_at 为空会被隐藏）。")
        st.stop()
    posts = enrich_posts(posts)
    assertions = enrich_assertions(assertions)

    # Note: Left sidebar filters removed on purpose.
    posts_filtered = posts
    assertions_filtered = assertions

    selected_page = st.segmented_control(
        "页面",
        pages,
        default=pages[0],
        key="main_page",
        label_visibility="collapsed",
        width="stretch",
    )
    selected_page = str(selected_page or pages[0])

    if selected_page == "总览":
        show_kpis(posts_filtered, assertions_filtered)
        st.divider()
        show_overview_charts(
            posts_filtered,
            assertions_filtered,
            group_col=DEFAULT_GROUP_COL,
            group_label=DEFAULT_GROUP_LABEL,
        )
        return

    if selected_page == "交易流":
        st.info("这个页换成 Reflex 了（列表 + 弹窗树 + 可拖拽列宽）。")
        st.link_button(
            "打开 Reflex 作业板",
            "http://localhost:3000/homework",
            width="content",
            type="primary",
        )
        return

    if selected_page == "风险雷达":
        show_risk_radar(
            assertions_filtered,
            group_col=DEFAULT_GROUP_COL,
            group_label=DEFAULT_GROUP_LABEL,
        )
        return

    if selected_page == "主题时间线":
        show_topic_timeline(
            posts,
            assertions_filtered,
            group_col=DEFAULT_GROUP_COL,
            group_label=DEFAULT_GROUP_LABEL,
        )
        return

    if selected_page == "关注页":
        st.info("关注页已经迁到 Reflex 研究台。")
        st.link_button(
            "打开 Reflex 整理中心",
            "http://localhost:3000/organizer",
            width="content",
            type="primary",
        )
        return

    if selected_page == "主题聚合":
        st.info("主题聚合已经迁到 Reflex 研究台。")
        st.link_button(
            "打开 Reflex 整理中心",
            "http://localhost:3000/organizer",
            width="content",
            type="primary",
        )
        return

    if selected_page == "学习库":
        show_learning_library(assertions_filtered)
        return

    if selected_page == "冲突/变化":
        show_conflicts_and_changes(
            assertions_filtered,
            group_col=DEFAULT_GROUP_COL,
            group_label=DEFAULT_GROUP_LABEL,
        )
        return

    if selected_page == "日志":
        show_logs()
        return

    assertion_counts = (
        assertions_filtered.groupby("post_uid")["idx"].count()
        if not assertions_filtered.empty
        else None
    )
    posts_view = posts_filtered.copy()
    if assertion_counts is not None:
        posts_view["assertion_count"] = (
            posts_view["post_uid"].map(assertion_counts).fillna(0).astype(int)
        )
    else:
        posts_view["assertion_count"] = 0
    show_tables(
        posts_view,
        assertions_filtered,
        group_col=DEFAULT_GROUP_COL,
        group_label=DEFAULT_GROUP_LABEL,
    )


if __name__ == "__main__":
    main()
