from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import select

from db import init_db, get_session, Influencer, Post


st.set_page_config(page_title="大佬观点中枢 Demo", layout="wide")


def load_data(session, start_date, end_date, influencer_ids=None, keyword=None):
    stmt = select(Post, Influencer).join(Influencer, Post.influencer_id == Influencer.id)

    if start_date:
        stmt = stmt.filter(Post.published_at >= start_date)
    if end_date:
        stmt = stmt.filter(Post.published_at <= end_date)
    if influencer_ids:
        stmt = stmt.filter(Post.influencer_id.in_(influencer_ids))
    if keyword:
        like = "%%%s%%" % keyword
        stmt = stmt.filter(
            (Post.title.ilike(like))
            | (Post.summary.ilike(like))
            | (Post.content.ilike(like))
            | (Post.stock_tags.ilike(like))
            | (Post.topic_tags.ilike(like))
        )

    rows = session.execute(stmt).all()
    records = []
    for post, inf in rows:
        records.append(
            {
                "id": post.id,
                "大佬": inf.name,
                "平台": inf.platform,
                "标题": post.title,
                "摘要": (post.summary or "")[:200],
                "链接": post.link,
                "发文时间": post.published_at,
                "标的标签": post.stock_tags,
                "主题标签": post.topic_tags,
            }
        )
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


def main():
    init_db()
    st.title("大佬观点中枢 Demo")

    with get_session() as session:
        influencers = session.query(Influencer).all()

        # 侧边栏过滤
        st.sidebar.header("筛选条件")

        col1, col2 = st.sidebar.columns(2)
        with col1:
            days = st.number_input(
                "最近 N 天", min_value=1, max_value=365, value=30, step=1
            )
        with col2:
            date_end = datetime.today()
            date_start = date_end - timedelta(days=int(days))

        inf_options = {
            "%s（%s）" % (inf.name, inf.platform or "未知平台"): inf.id
            for inf in influencers
        }
        selected_infs = st.sidebar.multiselect(
            "选择大佬（不选则为全部）", list(inf_options.keys())
        )
        selected_ids = [inf_options[name] for name in selected_infs] if selected_infs else None

        keyword = st.sidebar.text_input("关键字 / 标的 / 行业", "")

        df = load_data(session, date_start, date_end, selected_ids, keyword or None)

        st.subheader("当前时间段观点概览")

        if df.empty:
            st.info("当前条件下没有抓到任何内容，可以检查 RSS/时间范围。")
        else:
            # 1）按主题标签统计，用于「当前窗口提醒」
            topic_series = (
                df["主题标签"]
                .dropna()
                .str.split(",")
                .explode()
                .str.strip()
            )
            topic_counts = topic_series.value_counts().head(10)

            col_left, col_right = st.columns([1, 2])

            with col_left:
                st.markdown("**热门主题（最近 N 天）**")
                if topic_counts.empty:
                    st.write("暂无主题标签信息。")
                else:
                    st.bar_chart(topic_counts)

            with col_right:
                # 2）按日期统计发文数量
                df["日期"] = df["发文时间"].dt.date
                daily_counts = df.groupby("日期")["id"].count()
                st.markdown("**发文数量时间轴**")
                st.line_chart(daily_counts)

            st.subheader("详细列表（按时间倒序）")
            df = df.sort_values(by="发文时间", ascending=False)
            st.dataframe(
                df[
                    [
                        "发文时间",
                        "大佬",
                        "标题",
                        "摘要",
                        "标的标签",
                        "主题标签",
                        "链接",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )

        # 苏宁时间接口数据展示（暂时注释掉）
        # st.subheader("苏宁时间接口数据（最近 200 条）")
        # suning_rows = (
        #     session.query(SuningTime)
        #     .order_by(SuningTime.created_at.desc())
        #     .limit(200)
        #     .all()
        # )
        # if not suning_rows:
        #     st.info("还没有从苏宁接口抓到任何数据。")
        # else:
        #     suning_df = pd.DataFrame(
        #         [
        #             {
        #                 "抓取时间": row.created_at,
        #                 "接口标识": row.api,
        #                 "返回码": row.code,
        #                 "currentTime": row.current_time,
        #                 "消息": row.msg,
        #             }
        #             for row in suning_rows
        #         ]
        #     ).sort_values(by="抓取时间")

        #     col_a, col_b = st.columns([1, 1])
        #     with col_a:
        #         st.markdown("**currentTime 时间序列**")
        #         st.line_chart(suning_df.set_index("抓取时间")["currentTime"])
        #     with col_b:
        #         st.markdown("**原始返回数据表**")
        #         st.dataframe(
        #             suning_df.sort_values(by="抓取时间", ascending=False),
        #             use_container_width=True,
        #             hide_index=True,
        #         )


if __name__ == "__main__":
    main()

