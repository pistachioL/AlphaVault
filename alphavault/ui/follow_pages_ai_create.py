from __future__ import annotations

import pandas as pd
import streamlit as st

from alphavault.ai.analyze import _call_ai_with_litellm, clean_text
from alphavault.ai.topic_cluster_suggest import (
    _get_ai_config_from_env,
    ai_is_configured,
)
from alphavault.follow_pages import (
    FOLLOW_TYPE_TOPIC,
    ensure_follow_pages_schema,
    upsert_follow_page,
)
from alphavault.ui.follow_pages_key_match import (
    build_stock_name_by_code,
    is_stock_code_value,
    key_candidates,
)


MAX_PREVIEW_ROWS = 200


def _split_lines(value: str) -> list[str]:
    lines = [str(x).strip() for x in str(value or "").splitlines()]
    return [x for x in lines if x]


def _suggest_search_keywords(*, user_input: str, max_keywords: int = 8) -> dict:
    config, err = _get_ai_config_from_env()
    if config is None:
        raise RuntimeError(err or "ai_not_configured")

    q = clean_text(user_input)
    prompt = f"""
你是股票搜索词助手。

用户输入的是“股票名/简称/代码”（不是行业、不是主题）。
你的任务是：把这只股票的 code 写出来，然后给一组搜索关键字，方便在本地 match_keys 里搜到它。

用户输入：
{q or "（空）"}

请输出严格 JSON（不要 Markdown），结构：
{{
  "stock_codes": ["601899.SH"],
  "keywords": ["..."],
  "note": "一句话说明"
}}

规则：
- **stock_codes 不能为空**；如果你不确定，也必须给出你最可能的猜测，并在 note 里说明“不确定”。
- stock_codes 必须是这种格式之一：
  - A 股：000001.SZ / 600900.SH / 430047.BJ
  - 港股：00700.HK
  - 美股：AAPL.US
- keywords 用普通字符串，不要正则。
- keywords 里必须包含：stock_codes 里的 code（以及去掉后缀的纯数字/纯 ticker）。
- 还要包含：用户输入本身 + 常见简称/全称（比如去掉“股份/集团”）。
- 词不要太短（一般 >=2 个字，或包含数字）。
- 去重；一般 3~{int(max_keywords)} 个就够。
""".strip()

    parsed = _call_ai_with_litellm(
        prompt=prompt,
        api_mode=config.api_mode,
        ai_stream=False,
        model_name=config.model,
        base_url=config.base_url,
        api_key=config.api_key,
        timeout_seconds=float(config.timeout_seconds),
        retry_count=int(config.retries),
        temperature=float(config.temperature),
        reasoning_effort=str(config.reasoning_effort),
        trace_out=None,
        trace_label=f"follow_pages_search:{q}",
    )
    if not isinstance(parsed, dict):
        raise RuntimeError("ai_invalid_json")

    raw_codes = parsed.get("stock_codes") or []
    if isinstance(raw_codes, str):
        raw_codes = [raw_codes]
    if not isinstance(raw_codes, list):
        raw_codes = []
    stock_codes = [clean_text(x).upper() for x in raw_codes if clean_text(x)]
    # keep only valid shaped codes for display
    stock_codes_valid: list[str] = []
    seen_codes: set[str] = set()
    for code in stock_codes:
        c = str(code or "").strip().upper()
        if not c or c in seen_codes:
            continue
        if not is_stock_code_value(c):
            continue
        seen_codes.add(c)
        stock_codes_valid.append(c)

    if not stock_codes_valid:
        raise RuntimeError("ai_missing_stock_code")

    kws = parsed.get("keywords") or []
    if not isinstance(kws, list):
        kws = []
    out: list[str] = []
    seen: set[str] = set()
    # Always include codes first (so match_keys search hits stock:CODE).
    for code in stock_codes_valid:
        if code and code not in seen:
            seen.add(code)
            out.append(code)
        short = code.split(".", 1)[0].strip() if "." in code else ""
        if short and short not in seen:
            seen.add(short)
            out.append(short)
    for item in kws:
        s = clean_text(item)
        if not s or s in seen:
            continue
        if len(s) < 2 and not any(ch.isdigit() for ch in s):
            continue
        seen.add(s)
        out.append(s)
        if len(out) >= int(max(1, max_keywords)):
            break

    note = clean_text(parsed.get("note", ""))
    return {"keywords": out, "stock_codes": stock_codes_valid, "note": note}


def _format_alias_for_user(raw_key: str, *, stock_name_by_code: dict[str, str]) -> str:
    k = str(raw_key or "").strip()
    if not k:
        return ""
    if k.startswith("stock:"):
        value = k[len("stock:") :].strip()
        if not value:
            return "（空）"
        if is_stock_code_value(value):
            name = str(stock_name_by_code.get(value, "") or "").strip()
            return f"{name} ({value})" if name else value
        return value
    if ":" in k:
        return k.split(":", 1)[1].strip() or k
    return k


def _pick_primary_follow_key(rows: list[dict]) -> str:
    # Prefer stock:CODE keys as the main follow_key when possible.
    stock_code_rows: list[dict] = []
    for r in rows:
        k = str(r.get("key") or "").strip()
        if not k.startswith("stock:"):
            continue
        v = k[len("stock:") :].strip()
        if is_stock_code_value(v):
            stock_code_rows.append(r)
    if stock_code_rows:
        stock_code_rows = sorted(
            stock_code_rows,
            key=lambda x: (-int(x.get("count") or 0), str(x.get("key") or "")),
        )
        return str(stock_code_rows[0].get("key") or "").strip()

    rows = sorted(
        rows, key=lambda x: (-int(x.get("count") or 0), str(x.get("key") or ""))
    )
    return str(rows[0].get("key") or "").strip() if rows else ""


def _build_post_text_table(assertions_filtered: pd.DataFrame) -> pd.DataFrame:
    if (
        assertions_filtered.empty
        or "post_uid" not in assertions_filtered.columns
        or "raw_text" not in assertions_filtered.columns
    ):
        return pd.DataFrame(columns=["post_uid", "raw_text"])
    post_text = assertions_filtered[["post_uid", "raw_text"]].copy()
    post_text["post_uid"] = post_text["post_uid"].astype(str).str.strip()
    post_text = post_text[post_text["post_uid"].ne("")]
    post_text = post_text.drop_duplicates(subset=["post_uid"], keep="first")
    return post_text


def _count_posts_hit(post_text: pd.DataFrame, *, keyword: str) -> int:
    if post_text.empty:
        return 0
    kw = str(keyword or "").strip()
    if not kw:
        return 0
    try:
        mask = (
            post_text["raw_text"]
            .astype(str)
            .str.contains(kw, case=False, na=False, regex=False)
        )
    except Exception:
        return 0
    return int(mask.sum())


def _count_posts_hit_any(post_text: pd.DataFrame, *, keywords: list[str]) -> int:
    if post_text.empty:
        return 0
    words = [str(k).strip() for k in (keywords or []) if str(k).strip()]
    if not words:
        return 0
    try:
        texts = post_text["raw_text"].astype(str)
        mask = pd.Series(False, index=texts.index)
        for w in words:
            mask |= texts.str.contains(w, case=False, na=False, regex=False)
    except Exception:
        return 0
    return int(mask.sum())


def render_follow_pages_ai_create(
    engine,
    *,
    assertions_filtered: pd.DataFrame,
) -> None:
    st.markdown("**AI 新建关注页（推荐）**")

    state_prefix = "follow_pages_ai_create"
    input_key = f"{state_prefix}:input"
    keywords_key = f"{state_prefix}:keywords"
    note_key = f"{state_prefix}:note"
    codes_key = f"{state_prefix}:stock_codes"

    query = st.text_input(
        "输入股票名/简称（比如：紫金 / 长电）", value="", key=input_key
    ).strip()

    col_a, col_b = st.columns([1, 1])
    with col_a:
        if st.button(
            "AI 生成搜索关键字", type="secondary", key=f"{state_prefix}:ai_btn"
        ):
            ok, err = ai_is_configured()
            if not ok:
                st.info(f"AI 没配好：{err}")
            else:
                try:
                    result = _suggest_search_keywords(user_input=query)
                except Exception as exc:
                    msg = str(exc).strip()
                    if msg == "ai_missing_stock_code":
                        st.error(
                            "AI 没给出股票 code（比如 601899.SH）。你可以换个写法再点一次。"
                        )
                    else:
                        st.error(f"AI 失败：{type(exc).__name__}: {exc}")
                else:
                    st.session_state[keywords_key] = "\n".join(
                        result.get("keywords") or []
                    )
                    st.session_state[note_key] = str(result.get("note") or "").strip()
                    st.session_state[codes_key] = result.get("stock_codes") or []
                    st.rerun()
    with col_b:
        if st.button("清空", type="secondary", key=f"{state_prefix}:clear_btn"):
            st.session_state.pop(keywords_key, None)
            st.session_state.pop(note_key, None)
            st.session_state.pop(codes_key, None)
            st.session_state.pop(f"{state_prefix}:editor", None)
            st.rerun()

    note = str(st.session_state.get(note_key) or "").strip()
    if note:
        st.caption(f"AI 提示：{note}")
    codes = st.session_state.get(codes_key) or []
    if isinstance(codes, str):
        codes = [codes]
    if isinstance(codes, list):
        codes = [str(x).strip() for x in codes if str(x).strip()]
    else:
        codes = []
    if codes:
        st.caption(f"股票 code：{', '.join(codes)}")

    keywords_text = st.text_area(
        "搜索关键字（每行一个；你可以手动改）",
        value=str(st.session_state.get(keywords_key) or ""),
        height=90,
        key=keywords_key,
    )
    keywords = _split_lines(keywords_text)

    if assertions_filtered.empty or "match_keys" not in assertions_filtered.columns:
        st.info("当前没有 AI标签（match_keys）数据，无法预览。")
        return

    post_text = _build_post_text_table(assertions_filtered)

    stock_name_by_code = build_stock_name_by_code(assertions_filtered)
    raw_counts = key_candidates(assertions_filtered)
    if raw_counts.empty:
        st.info("没有 key 数据。")
        return

    # Build candidates for keyword effect preview.
    candidates: list[dict] = []
    for raw_key, count in raw_counts.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        alias = (
            _format_alias_for_user(key, stock_name_by_code=stock_name_by_code) or key
        )
        hay = f"{key} {alias}".lower()
        candidates.append(
            {
                "key": key,
                "alias": alias,
                "count": int(count or 0),
                "hay": hay,
            }
        )

    # Keyword effect preview (per keyword).
    enabled_map_key = f"{state_prefix}:kw_enabled_map"
    enabled_map = st.session_state.get(enabled_map_key) or {}
    if not isinstance(enabled_map, dict):
        enabled_map = {}

    kw_rows: list[dict] = []
    for kw in keywords:
        needle = str(kw or "").strip().lower()
        key_hits = 0
        if needle:
            for c in candidates:
                if needle in str(c.get("hay") or ""):
                    key_hits += 1
        kw_rows.append(
            {
                "使用": bool(enabled_map.get(kw, True)),
                "关键字": kw,
                "AI标签命中(结果数)": int(key_hits),
                "正文关键字搜索命中(帖数)": int(
                    _count_posts_hit(post_text, keyword=kw)
                ),
            }
        )

    active_keywords = keywords[:]
    if kw_rows:
        st.markdown("**关键字效果（每个关键字能匹配到多少 AI标签）**")
        kw_edited = st.data_editor(
            pd.DataFrame(kw_rows),
            width="stretch",
            height="auto",
            hide_index=True,
            disabled=["关键字", "AI标签命中(结果数)", "正文关键字搜索命中(帖数)"],
            column_order=[
                "使用",
                "关键字",
                "AI标签命中(结果数)",
                "正文关键字搜索命中(帖数)",
            ],
            key=f"{state_prefix}:kw_editor",
        )
        if isinstance(kw_edited, pd.DataFrame) and not kw_edited.empty:
            new_map: dict[str, bool] = {}
            active_keywords = []
            for _, r in kw_edited.iterrows():
                w = str(r.get("关键字") or "").strip()
                if not w:
                    continue
                use = bool(r.get("使用"))
                new_map[w] = use
                if use:
                    active_keywords.append(w)
            st.session_state[enabled_map_key] = new_map

        needles = [str(k).strip().lower() for k in active_keywords if str(k).strip()]
        key_total = 0
        if needles:
            for c in candidates:
                hay = str(c.get("hay") or "")
                if any(n in hay for n in needles):
                    key_total += 1
        text_total = _count_posts_hit_any(post_text, keywords=active_keywords)
        st.caption(
            f"你选了 {len(active_keywords)} 个关键字：AI标签命中 {key_total} 个结果，正文关键字搜索命中 {text_total} 帖"
        )

    rows: list[dict] = []
    needles = [str(k).strip().lower() for k in active_keywords if str(k).strip()]
    for c in candidates:
        key = str(c.get("key") or "").strip()
        if not key:
            continue
        alias = str(c.get("alias") or "").strip() or key
        count = int(c.get("count") or 0)
        if needles:
            hay = str(c.get("hay") or "")
            if not any(n in hay for n in needles):
                continue
        rows.append(
            {
                "选择": True,
                "AI标签": alias,
                "出现次数": int(count or 0),
                "key": key,
            }
        )
        if len(rows) >= int(MAX_PREVIEW_ROWS):
            break

    if not rows:
        st.warning("没有命中。你可以换几个搜索关键字再试。")
        return

    st.caption("提示：你只要勾你想要的 AI标签 就行。")
    st.caption(f"预览命中 {len(rows)} 个结果（最多展示 {MAX_PREVIEW_ROWS} 个）")
    editor_df = pd.DataFrame(rows).set_index("key", drop=True)
    edited = st.data_editor(
        editor_df,
        width="stretch",
        height="auto",
        hide_index=True,
        disabled=[
            "AI标签",
            "出现次数",
        ],
        column_order=[
            "选择",
            "AI标签",
            "出现次数",
        ],
        key=f"{state_prefix}:editor",
    )

    selected_rows = []
    if isinstance(edited, pd.DataFrame) and not edited.empty:
        for idx, r in edited.iterrows():
            if bool(r.get("选择")):
                selected_rows.append(
                    {
                        "key": str(idx or "").strip(),
                        "count": int(r.get("出现次数") or 0),
                    }
                )

    st.divider()
    page_name = st.text_input(
        "页面名字（可空）",
        value="",
        key=f"{state_prefix}:page_name",
    )

    if st.button(
        "创建关注页（1 个）", type="primary", key=f"{state_prefix}:create_btn"
    ):
        if not selected_rows:
            st.error("你要先勾选至少 1 个 AI标签。")
            st.stop()

        primary_key = _pick_primary_follow_key(selected_rows)
        if not primary_key:
            st.error("主 key 为空。")
            st.stop()

        follow_keys = sorted(
            str(item.get("key") or "").strip()
            for item in selected_rows
            if str(item.get("key") or "").strip()
        )
        keywords_to_save = "\n".join(active_keywords)

        try:
            ensure_follow_pages_schema(engine)
            page_key = upsert_follow_page(
                engine,
                follow_type=FOLLOW_TYPE_TOPIC,
                follow_key=primary_key,
                follow_keys=follow_keys,
                page_name=page_name,
                keywords_text=keywords_to_save,
            )
        except Exception as exc:
            st.error(f"保存失败：{type(exc).__name__}: {exc}")
            st.stop()

        st.success("已创建。")
        st.session_state["follow_pages_selected_page_key"] = page_key
        st.cache_data.clear()
        st.rerun()


__all__ = ["render_follow_pages_ai_create"]
