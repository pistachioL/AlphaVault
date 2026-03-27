from __future__ import annotations

import re

import pandas as pd

from alphavault.ui.thread_tree import build_weibo_thread_forest
from alphavault.ui.thread_tree import extract_platform_post_id


_TRAILING_ESCAPED_TAB_RE = re.compile(r"(?:\\+t)+$")


def _clean_post_id(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("\t", "")
    text = _TRAILING_ESCAPED_TAB_RE.sub("", text)
    return text.strip()


def _find_thread_text(threads: list[dict], *, post_id: str) -> tuple[str, str]:
    target = str(post_id or "").strip()
    if not target:
        return "", ""
    for thread in threads:
        nodes = thread.get("nodes")
        if isinstance(nodes, dict) and target in nodes:
            label = str(thread.get("label") or "").strip()
            tree_text = str(thread.get("tree_text") or "").rstrip()
            return label, tree_text
    return "", ""


def build_post_tree(*, post_uid: str, posts: pd.DataFrame) -> tuple[str, str]:
    uid = str(post_uid or "").strip()
    if not uid or posts.empty:
        return "", ""
    view_df = pd.DataFrame({"post_uid": [uid]})
    threads = build_weibo_thread_forest(view_df, posts_all=posts)
    if not threads:
        return "", ""
    first = threads[0] or {}
    label = str(first.get("label") or "").strip()
    tree_text = str(first.get("tree_text") or "").rstrip()
    return label, tree_text


def build_post_tree_map(
    *, post_uids: list[str], posts: pd.DataFrame
) -> dict[str, tuple[str, str]]:
    """
    Batch version of build_post_tree().

    Important for performance: building the thread forest walks the entire posts table,
    so we should only do it once per page.
    """
    if posts.empty or not post_uids:
        return {}

    cleaned_uids: list[str] = []
    seen: set[str] = set()
    for raw in post_uids:
        uid = str(raw or "").strip()
        if not uid or uid in seen:
            continue
        seen.add(uid)
        cleaned_uids.append(uid)
    if not cleaned_uids:
        return {}

    threads = build_weibo_thread_forest(
        pd.DataFrame({"post_uid": cleaned_uids}),
        posts_all=posts,
    )
    if not threads:
        return {uid: ("", "") for uid in cleaned_uids}

    out: dict[str, tuple[str, str]] = {}
    for uid in cleaned_uids:
        post_id = _clean_post_id(extract_platform_post_id(uid))
        out[uid] = _find_thread_text(threads, post_id=post_id)
    return out
