from __future__ import annotations

import pandas as pd

from alphavault.domains.thread_tree.service import slice_posts_for_single_post_tree


def test_slice_posts_for_single_post_tree_matches_post_uid() -> None:
    posts = pd.DataFrame(
        [
            {"post_uid": "weibo:1", "platform_post_id": "1", "raw_text": "a"},
            {"post_uid": "weibo:2", "platform_post_id": "2", "raw_text": "b"},
        ]
    )

    view = slice_posts_for_single_post_tree(post_uid="weibo:2", posts=posts)

    assert len(view.index) == 1
    assert view.iloc[0]["post_uid"] == "weibo:2"


def test_slice_posts_for_single_post_tree_falls_back_to_platform_post_id() -> None:
    posts = pd.DataFrame(
        [
            {"platform_post_id": "400912898", "raw_text": "a"},
            {"platform_post_id": "400776255", "raw_text": "b"},
        ]
    )

    view = slice_posts_for_single_post_tree(post_uid="xueqiu:400776255", posts=posts)

    assert len(view.index) == 1
    assert view.iloc[0]["platform_post_id"] == "400776255"
