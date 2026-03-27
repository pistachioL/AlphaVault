from __future__ import annotations

import pandas as pd

from alphavault_reflex.services.thread_tree import build_post_tree


def test_build_post_tree_uses_raw_text_for_xueqiu() -> None:
    post_uid = "xueqiu:400776255"
    posts = pd.DataFrame(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": "400776255",
                "author": "雪球作者",
                "raw_text": "A：根 --- B：中 --- A：叶",
                "display_md": "MD_BAD",
                "created_at": "2026-03-25 10:23:48",
            }
        ]
    )

    _label, tree_text = build_post_tree(post_uid=post_uid, posts=posts)

    assert "A：根" in tree_text
    assert "B：中" in tree_text
    assert "A：叶" in tree_text
    assert "└──" in tree_text
    assert "MD_BAD" not in tree_text


def test_build_post_tree_uses_richer_display_md_for_xueqiu() -> None:
    post_uid = "xueqiu:400776256"
    posts = pd.DataFrame(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": "400776256",
                "author": "雪球作者",
                "raw_text": "A：叶",
                "display_md": "A：根\n\n---\n\nB：中\n\n---\n\nA：叶",
                "created_at": "2026-03-25 10:23:49",
            }
        ]
    )

    _label, tree_text = build_post_tree(post_uid=post_uid, posts=posts)

    assert "A：根" in tree_text
    assert "B：中" in tree_text
    assert "A：叶" in tree_text


def test_build_post_tree_keeps_display_md_for_weibo() -> None:
    post_uid = "weibo:5281025354637435"
    display_md = "老：根\n\n---\n\n新：叶"
    posts = pd.DataFrame(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": "5281025354637435",
                "author": "微博作者",
                "raw_text": "RAW_WEIBO",
                "display_md": display_md,
                "created_at": "2026-03-27 11:36:44",
            }
        ]
    )

    _label, tree_text = build_post_tree(post_uid=post_uid, posts=posts)

    assert "老：根" in tree_text
    assert "新：叶" in tree_text
