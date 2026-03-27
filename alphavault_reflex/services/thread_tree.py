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


def _resolve_platform_post_id(*, post_uid: str, posts: pd.DataFrame) -> str:
    uid = _clean_post_id(post_uid)
    if not uid or posts.empty or "platform_post_id" not in posts.columns:
        return ""

    cleaned = posts["platform_post_id"].astype(str)
    cleaned = cleaned.str.replace("\t", "", regex=False)
    cleaned = cleaned.str.replace(_TRAILING_ESCAPED_TAB_RE, "", regex=True)
    cleaned = cleaned.str.strip()
    if bool(cleaned.eq(uid).any()):
        return uid
    return ""


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


def slice_posts_for_single_post_tree(
    *, post_uid: str, posts: pd.DataFrame
) -> pd.DataFrame:
    """
    Keep tree building fast by slicing posts_all down to the one target post.

    This intentionally trades completeness (full thread) for speed: we only need
    the root-to-leaf path for the selected post.
    """
    uid = str(post_uid or "").strip()
    if not uid or posts.empty:
        return posts.head(0).copy()

    if "post_uid" in posts.columns:
        uid_series = posts["post_uid"].astype(str).str.strip()
        uid_mask = uid_series.eq(uid)
        if bool(uid_mask.any()):
            return posts.loc[uid_mask].copy()

    resolved_platform_post_id = _resolve_platform_post_id(post_uid=uid, posts=posts)
    if resolved_platform_post_id and "platform_post_id" in posts.columns:
        cleaned = posts["platform_post_id"].astype(str)
        cleaned = cleaned.str.replace("\t", "", regex=False)
        cleaned = cleaned.str.replace(_TRAILING_ESCAPED_TAB_RE, "", regex=True)
        cleaned = cleaned.str.strip()
        resolved_mask = cleaned.eq(resolved_platform_post_id)
        if bool(resolved_mask.any()):
            return posts.loc[resolved_mask].copy()

    platform_post_id = _clean_post_id(extract_platform_post_id(uid))
    if not platform_post_id or "platform_post_id" not in posts.columns:
        return posts.head(0).copy()

    cleaned = posts["platform_post_id"].astype(str)
    cleaned = cleaned.str.replace("\t", "", regex=False)
    cleaned = cleaned.str.replace(_TRAILING_ESCAPED_TAB_RE, "", regex=True)
    cleaned = cleaned.str.strip()
    id_mask = cleaned.eq(platform_post_id)
    if bool(id_mask.any()):
        return posts.loc[id_mask].copy()

    return posts.head(0).copy()


def build_post_tree(*, post_uid: str, posts: pd.DataFrame) -> tuple[str, str]:
    uid = str(post_uid or "").strip()
    if not uid or posts.empty:
        return "", ""
    view_df = pd.DataFrame(
        {
            "post_uid": [uid],
            "platform_post_id": [_resolve_platform_post_id(post_uid=uid, posts=posts)],
        }
    )
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

    view_df = pd.DataFrame(
        {
            "post_uid": cleaned_uids,
            "platform_post_id": [
                _resolve_platform_post_id(post_uid=uid, posts=posts)
                for uid in cleaned_uids
            ],
        }
    )
    threads = build_weibo_thread_forest(
        view_df,
        posts_all=posts,
    )
    if not threads:
        return {uid: ("", "") for uid in cleaned_uids}

    out: dict[str, tuple[str, str]] = {}
    for uid in cleaned_uids:
        post_id = _clean_post_id(extract_platform_post_id(uid))
        out[uid] = _find_thread_text(threads, post_id=post_id)
    return out
