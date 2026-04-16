from __future__ import annotations

import re

from alphavault.constants import PLATFORM_XUEQIU
from alphavault.domains.thread_tree.api import build_weibo_thread_forest
from alphavault.domains.thread_tree.api import extract_platform_post_id


_TRAILING_ESCAPED_TAB_RE = re.compile(r"(?:\\+t)+$")


def _clean_post_id(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("\t", "")
    text = _TRAILING_ESCAPED_TAB_RE.sub("", text)
    return text.strip()


def normalize_tree_lookup_post_uid(value: object) -> str:
    if value is None:
        return ""
    raw = "" if value is None else str(value)
    stripped = raw.strip()
    if not stripped:
        return ""
    if stripped.lower().startswith(f"{PLATFORM_XUEQIU}:"):
        return raw
    return stripped


def _clean_platform_post_id(value: object) -> str:
    return _clean_post_id(value)


def _resolve_platform_post_id(*, post_uid: str, posts: list[dict[str, object]]) -> str:
    uid = _clean_post_id(post_uid)
    if not uid or not posts:
        return ""

    cleaned_ids = [
        _clean_platform_post_id(row.get("platform_post_id")) for row in posts
    ]
    if uid in cleaned_ids:
        return uid

    extracted = _clean_post_id(extract_platform_post_id(uid))
    if extracted and extracted in cleaned_ids:
        return extracted

    if uid.lower().startswith(f"{PLATFORM_XUEQIU}:"):
        last_segment = _clean_post_id(extracted.rsplit(":", 1)[-1] if extracted else "")
        if last_segment and last_segment in cleaned_ids:
            return last_segment

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
    *, post_uid: str, posts: list[dict[str, object]]
) -> list[dict[str, object]]:
    """
    Keep tree building fast by slicing posts_all down to the one target post.

    This intentionally trades completeness (full thread) for speed: we only need
    the root-to-leaf path for the selected post.
    """
    uid = normalize_tree_lookup_post_uid(post_uid)
    if not uid or not posts:
        return []

    uid_matches = [
        dict(row)
        for row in posts
        if normalize_tree_lookup_post_uid(row.get("post_uid")) == uid
    ]
    if uid_matches:
        return uid_matches

    resolved_platform_post_id = _resolve_platform_post_id(post_uid=uid, posts=posts)
    if resolved_platform_post_id:
        resolved_matches = [
            dict(row)
            for row in posts
            if _clean_platform_post_id(row.get("platform_post_id"))
            == resolved_platform_post_id
        ]
        if resolved_matches:
            return resolved_matches

    platform_post_id = _clean_post_id(extract_platform_post_id(uid))
    if not platform_post_id:
        return []

    id_matches = [
        dict(row)
        for row in posts
        if _clean_platform_post_id(row.get("platform_post_id")) == platform_post_id
    ]
    if id_matches:
        return id_matches

    return []


def build_post_tree(
    *, post_uid: str, posts: list[dict[str, object]]
) -> tuple[str, str]:
    uid = normalize_tree_lookup_post_uid(post_uid)
    if not uid or not posts:
        return "", ""
    view_rows: list[dict[str, object]] = [
        {
            "post_uid": uid,
            "platform_post_id": _resolve_platform_post_id(post_uid=uid, posts=posts),
        }
    ]
    threads = build_weibo_thread_forest(view_rows, posts_all=posts)
    if not threads and uid.lower().startswith(f"{PLATFORM_XUEQIU}:"):
        fallback_uids: list[str] = []
        seen: set[str] = set()
        for row in posts:
            platform_id = _clean_post_id(row.get("platform_post_id"))
            if not platform_id:
                continue
            fallback_uid = f"{PLATFORM_XUEQIU}:{platform_id}"
            if fallback_uid in seen:
                continue
            seen.add(fallback_uid)
            fallback_uids.append(fallback_uid)
        if fallback_uids:
            threads = build_weibo_thread_forest(
                [{"post_uid": fallback_uid} for fallback_uid in fallback_uids],
                posts_all=posts,
            )
    if not threads:
        return "", ""
    first = threads[0] or {}
    label = str(first.get("label") or "").strip()
    tree_text = str(first.get("tree_text") or "").rstrip()
    return label, tree_text


def build_post_tree_map(
    *, post_uids: list[str], posts: list[dict[str, object]]
) -> dict[str, tuple[str, str]]:
    """
    Batch version of build_post_tree().

    Important for performance: building the thread forest walks the entire posts table,
    so we should only do it once per page.
    """
    if not posts or not post_uids:
        return {}

    cleaned_uids: list[str] = []
    seen: set[str] = set()
    for raw in post_uids:
        uid = normalize_tree_lookup_post_uid(raw)
        if not uid or uid in seen:
            continue
        seen.add(uid)
        cleaned_uids.append(uid)
    if not cleaned_uids:
        return {}

    view_rows: list[dict[str, object]] = [
        {
            "post_uid": uid,
            "platform_post_id": _resolve_platform_post_id(post_uid=uid, posts=posts),
        }
        for uid in cleaned_uids
    ]
    threads = build_weibo_thread_forest(
        view_rows,
        posts_all=posts,
    )
    if not threads:
        return {uid: ("", "") for uid in cleaned_uids}

    out: dict[str, tuple[str, str]] = {}
    for uid in cleaned_uids:
        post_id = _resolve_platform_post_id(
            post_uid=uid, posts=posts
        ) or _clean_post_id(extract_platform_post_id(uid))
        out[uid] = _find_thread_text(threads, post_id=post_id)
    return out
