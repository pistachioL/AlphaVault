"""
Streamlit tab: logs.

This tab is for a quick view of AI queue state and recent errors.
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st

from alphavault.constants import ENV_TURSO_MAX_CONNECTIONS
from alphavault.db.introspect import table_columns
from alphavault.db.sql.common import make_in_params, make_in_placeholders
from alphavault.db.sql.logs import SELECT_AI_STATUS_COUNTS, select_ai_queue_rows
from alphavault.db.turso_db import ensure_turso_engine, turso_connect_autocommit


def _rows_to_df(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def show_logs(*, turso_url: str, turso_token: str) -> None:
    st.markdown("**日志（AI 队列 / 错误）**")

    url = str(turso_url or "").strip()
    if not url:
        st.error("TURSO_DATABASE_URL 没填。")
        return

    engine = ensure_turso_engine(url, str(turso_token or "").strip())

    st.caption(
        f"连接数上限：{os.getenv(ENV_TURSO_MAX_CONNECTIONS, '').strip() or '4'}（ENV: {ENV_TURSO_MAX_CONNECTIONS}）"
    )

    limit = int(
        st.number_input(
            "最多显示多少行",
            min_value=50,
            max_value=2000,
            value=200,
            step=50,
        )
    )
    only_with_error = bool(st.toggle("只看有错误文本的行（ai_last_error）", value=True))

    statuses = st.multiselect(
        "状态（ai_status）",
        options=["error", "pending", "running", "done"],
        default=["error", "pending", "running"],
    )
    statuses = [str(s).strip() for s in statuses if str(s).strip()]
    if not statuses:
        st.info("你还没选状态。")
        return

    if st.button("刷新", type="primary"):
        st.rerun()

    with turso_connect_autocommit(engine) as conn:
        post_cols = table_columns(conn, "posts")
        required = {
            "ai_status",
            "ai_retry_count",
            "ai_next_retry_at",
            "ai_running_at",
            "ai_last_error",
            "ingested_at",
        }
        missing = sorted(required - set(post_cols))
        if missing:
            st.warning(
                "posts 表缺少队列字段，可能还没初始化过队列/worker："
                + ", ".join(missing)
            )
            return

        summary_rows = conn.execute(SELECT_AI_STATUS_COUNTS).mappings().fetchall()
        counts = {
            str(r.get("ai_status") or ""): int(r.get("n") or 0) for r in summary_rows
        }
        cols = st.columns(4)
        cols[0].metric("error", counts.get("error", 0))
        cols[1].metric("pending", counts.get("pending", 0))
        cols[2].metric("running", counts.get("running", 0))
        cols[3].metric("done", counts.get("done", 0))

        placeholders = make_in_placeholders(prefix="st", count=len(statuses))
        params = make_in_params(prefix="st", values=statuses)
        params["limit"] = max(1, int(limit))
        query = select_ai_queue_rows(
            placeholders, only_with_error=bool(only_with_error)
        )
        rows = conn.execute(query, params).mappings().fetchall()

    df = _rows_to_df([dict(r) for r in rows if r])
    if df.empty:
        st.info("没有数据。")
        return

    st.dataframe(df, width="stretch", hide_index=True)


__all__ = ["show_logs"]
