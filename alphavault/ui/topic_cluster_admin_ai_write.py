from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine

from alphavault.topic_cluster import (
    ensure_cluster_schema,
    upsert_cluster_topics_detailed,
)
from alphavault.ui.topic_cluster_admin_helpers import (
    _parse_confidence,
    _sort_by_count,
)


def _render_ai_write_section(
    *,
    engine: Engine,
    topic_map: pd.DataFrame,
    selected_cluster: str,
    include_items: list[dict],
    unsure_items: list[dict],
    count_by_topic: dict[str, int],
) -> None:
    st.markdown("**写入板块成员**")
    st.caption("include 默认全选；unsure 默认不选。只会加入，不会移走。")

    selected_cluster = str(selected_cluster or "").strip()
    if not selected_cluster:
        st.info("请先选一个板块。")
        return

    existing_in_cluster: set[str] = set()
    if (
        not topic_map.empty
        and "topic_key" in topic_map.columns
        and "cluster_key" in topic_map.columns
    ):
        df = topic_map[["topic_key", "cluster_key"]].dropna().copy()
        df["topic_key"] = df["topic_key"].astype(str).str.strip()
        df["cluster_key"] = df["cluster_key"].astype(str).str.strip()
        existing_in_cluster = set(
            df.loc[df["cluster_key"] == selected_cluster, "topic_key"].tolist()
        )

    include_conf_by_topic: dict[str, float] = {}
    for item in include_items:
        key = str(item.get("key") or item.get("topic_key") or "").strip()
        if key:
            include_conf_by_topic[key] = _parse_confidence(
                item.get("confidence", None), 0.8
            )

    unsure_conf_by_topic: dict[str, float] = {}
    for item in unsure_items:
        key = str(item.get("key") or item.get("topic_key") or "").strip()
        if key and key not in include_conf_by_topic:
            unsure_conf_by_topic[key] = _parse_confidence(
                item.get("confidence", None), 0.55
            )

    include_keys_raw = [
        str(item.get("key") or item.get("topic_key") or "").strip()
        for item in include_items
        if str(item.get("key") or item.get("topic_key") or "").strip()
    ]
    include_keys = sorted(
        set([k for k in include_keys_raw if k and k not in existing_in_cluster])
    )
    include_keys = _sort_by_count(include_keys, count_by_topic=count_by_topic)

    unsure_keys_raw = [
        str(item.get("key") or item.get("topic_key") or "").strip()
        for item in unsure_items
        if str(item.get("key") or item.get("topic_key") or "").strip()
    ]
    unsure_keys = sorted(
        set(
            [
                k
                for k in unsure_keys_raw
                if k and k not in existing_in_cluster and k not in set(include_keys)
            ]
        )
    )
    unsure_keys = _sort_by_count(unsure_keys, count_by_topic=count_by_topic)

    COL_SELECTED = "selected"
    COL_KEY = "key"
    COL_COUNT = "count"
    COL_CONF = "confidence"
    COL_REASON = "reason"
    COL_HINT = "hint"

    include_reason_by_topic: dict[str, str] = {}
    include_hint_by_topic: dict[str, str] = {}
    for item in include_items:
        key = str(item.get("key") or item.get("topic_key") or "").strip()
        if not key:
            continue
        if key not in include_reason_by_topic:
            include_reason_by_topic[key] = str(item.get("reason") or "").strip()
        if key not in include_hint_by_topic:
            include_hint_by_topic[key] = str(item.get("hint") or "").strip()

    unsure_reason_by_topic: dict[str, str] = {}
    unsure_hint_by_topic: dict[str, str] = {}
    for item in unsure_items:
        key = str(item.get("key") or item.get("topic_key") or "").strip()
        if not key:
            continue
        if key not in unsure_reason_by_topic:
            unsure_reason_by_topic[key] = str(item.get("reason") or "").strip()
        if key not in unsure_hint_by_topic:
            unsure_hint_by_topic[key] = str(item.get("hint") or "").strip()

    st.markdown("**include（默认全选）**")
    st.caption("在左边直接勾选/取消勾选。")

    include_editor_key = f"cluster_ai_write_include_editor:{selected_cluster}"
    include_editor_sig_key = f"cluster_ai_write_include_editor_sig:{selected_cluster}"
    include_sig_prev = st.session_state.get(include_editor_sig_key, None)
    if not isinstance(include_sig_prev, list) or include_sig_prev != include_keys:
        st.session_state[include_editor_sig_key] = list(include_keys)
        st.session_state.pop(include_editor_key, None)

    include_selected: list[str] = []
    if include_keys:
        include_rows = [
            {
                COL_SELECTED: True,
                COL_KEY: str(k),
                COL_COUNT: int(count_by_topic.get(str(k), 0)),
                COL_CONF: float(include_conf_by_topic.get(str(k), 0.8)),
                COL_REASON: str(include_reason_by_topic.get(str(k), "") or "").strip(),
                COL_HINT: str(include_hint_by_topic.get(str(k), "") or "").strip(),
            }
            for k in include_keys
        ]
        include_df = pd.DataFrame(include_rows)
        include_edited = st.data_editor(
            include_df,
            width="stretch",
            hide_index=True,
            num_rows="fixed",
            column_order=[
                COL_SELECTED,
                COL_KEY,
                COL_COUNT,
                COL_CONF,
                COL_REASON,
                COL_HINT,
            ],
            column_config={
                COL_SELECTED: st.column_config.CheckboxColumn("选", help="勾上=要写入"),
                COL_KEY: st.column_config.TextColumn("key"),
                COL_COUNT: st.column_config.NumberColumn("次数"),
                COL_CONF: st.column_config.NumberColumn("confidence", format="%.2f"),
                COL_REASON: st.column_config.TextColumn("reason"),
                COL_HINT: st.column_config.TextColumn("hint"),
            },
            disabled=[COL_KEY, COL_COUNT, COL_CONF, COL_REASON, COL_HINT],
            key=include_editor_key,
        )
        if isinstance(include_edited, pd.DataFrame):
            include_selected = [
                str(x).strip()
                for x in include_edited.loc[
                    include_edited[COL_SELECTED].astype(bool), COL_KEY
                ].tolist()
                if str(x).strip()
            ]
        st.caption(f"include：选了 {len(include_selected)}/{len(include_keys)}")
    else:
        st.info("include 为空。")

    st.markdown("**unsure（默认不选）**")
    st.caption("在左边直接勾选。")

    unsure_editor_key = f"cluster_ai_write_unsure_editor:{selected_cluster}"
    unsure_editor_sig_key = f"cluster_ai_write_unsure_editor_sig:{selected_cluster}"
    unsure_sig_prev = st.session_state.get(unsure_editor_sig_key, None)
    if not isinstance(unsure_sig_prev, list) or unsure_sig_prev != unsure_keys:
        st.session_state[unsure_editor_sig_key] = list(unsure_keys)
        st.session_state.pop(unsure_editor_key, None)

    unsure_selected: list[str] = []
    if unsure_keys:
        unsure_rows = [
            {
                COL_SELECTED: False,
                COL_KEY: str(k),
                COL_COUNT: int(count_by_topic.get(str(k), 0)),
                COL_CONF: float(unsure_conf_by_topic.get(str(k), 0.55)),
                COL_REASON: str(unsure_reason_by_topic.get(str(k), "") or "").strip(),
                COL_HINT: str(unsure_hint_by_topic.get(str(k), "") or "").strip(),
            }
            for k in unsure_keys
        ]
        unsure_df = pd.DataFrame(unsure_rows)
        unsure_edited = st.data_editor(
            unsure_df,
            width="stretch",
            hide_index=True,
            num_rows="fixed",
            column_order=[
                COL_SELECTED,
                COL_KEY,
                COL_COUNT,
                COL_CONF,
                COL_REASON,
                COL_HINT,
            ],
            column_config={
                COL_SELECTED: st.column_config.CheckboxColumn("选", help="勾上=要写入"),
                COL_KEY: st.column_config.TextColumn("key"),
                COL_COUNT: st.column_config.NumberColumn("次数"),
                COL_CONF: st.column_config.NumberColumn("confidence", format="%.2f"),
                COL_REASON: st.column_config.TextColumn("reason"),
                COL_HINT: st.column_config.TextColumn("hint"),
            },
            disabled=[COL_KEY, COL_COUNT, COL_CONF, COL_REASON, COL_HINT],
            key=unsure_editor_key,
        )
        if isinstance(unsure_edited, pd.DataFrame):
            unsure_selected = [
                str(x).strip()
                for x in unsure_edited.loc[
                    unsure_edited[COL_SELECTED].astype(bool), COL_KEY
                ].tolist()
                if str(x).strip()
            ]
        st.caption(f"unsure：选了 {len(unsure_selected)}/{len(unsure_keys)}")

    to_write = [
        *[str(x).strip() for x in include_selected],
        *[str(x).strip() for x in unsure_selected],
    ]
    to_write = [x for x in to_write if x]
    to_write = sorted(set(to_write))
    st.caption(f"本次将写入：{len(to_write)} 个 key")

    if not st.button("确认加入这个板块", type="primary", disabled=not bool(to_write)):
        return

    try:
        ensure_cluster_schema(engine)
        payloads: list[dict] = []
        for key in to_write:
            conf = include_conf_by_topic.get(key)
            if conf is None:
                conf = unsure_conf_by_topic.get(key, 0.75)
            # Note: DB column name stays "topic_key", but we store the generic member key.
            payloads.append(
                {"topic_key": key, "source": "ai", "confidence": float(conf)}
            )
        n = upsert_cluster_topics_detailed(
            engine,
            cluster_key=selected_cluster,
            topic_items=payloads,
            default_source="ai",
            default_confidence=0.75,
        )
    except Exception as exc:
        st.error(f"写入失败：{type(exc).__name__}: {exc}")
        st.stop()

    st.success(f"已写入 {n} 个 key。")
    st.cache_data.clear()
    st.rerun()
