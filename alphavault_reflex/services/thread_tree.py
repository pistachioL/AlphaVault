from __future__ import annotations

import pandas as pd

from alphavault.ui.thread_tree import build_weibo_thread_forest


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
