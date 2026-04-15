from __future__ import annotations

from alphavault.domains.thread_tree.service import build_post_tree


def _rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return rows


def test_build_post_tree_uses_raw_text_for_xueqiu() -> None:
    post_uid = "xueqiu:400776255"
    raw_text = "A：根\n\n---\n\nB：中\n\n---\n\nA：叶"
    posts = _rows(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": "400776255",
                "author": "雪球作者",
                "raw_text": raw_text,
                "created_at": "2026-03-25 10:23:48",
            }
        ]
    )

    _label, tree_text = build_post_tree(post_uid=post_uid, posts=posts)

    assert "A：根" in tree_text
    assert "B：中" in tree_text
    assert "A：叶" in tree_text
    assert "└──" in tree_text


def test_build_post_tree_uses_xueqiu_raw_text_only() -> None:
    post_uid = "xueqiu:400776256"
    posts = _rows(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": "400776256",
                "author": "雪球作者",
                "raw_text": "A：叶",
                "created_at": "2026-03-25 10:23:49",
            }
        ]
    )

    _label, tree_text = build_post_tree(post_uid=post_uid, posts=posts)

    assert "A：叶" in tree_text
    assert "A：根" not in tree_text


def test_build_post_tree_uses_weibo_raw_text_only() -> None:
    post_uid = "weibo:5281025354637435"
    raw_text = "老：根\n\n---\n\n新：叶"
    posts = _rows(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": "5281025354637435",
                "author": "微博作者",
                "raw_text": raw_text,
                "created_at": "2026-03-27 11:36:44",
            }
        ]
    )

    _label, tree_text = build_post_tree(post_uid=post_uid, posts=posts)

    assert "老：根" in tree_text
    assert "新：叶" in tree_text


def test_build_post_tree_supports_xueqiu_guid_as_post_uid() -> None:
    post_uid = "xueqiu:381213336"
    posts = _rows(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": post_uid,
                "author": "雪球作者",
                "raw_text": "A：根\n\n---\n\nB：叶",
                "created_at": "2026-03-25 10:23:50",
            }
        ]
    )

    _label, tree_text = build_post_tree(post_uid=post_uid, posts=posts)

    assert "A：根" in tree_text
    assert "B：叶" in tree_text


def test_build_post_tree_supports_xueqiu_comment_uid_with_numeric_platform_id() -> None:
    post_uid = "xueqiu:comment:400768409"
    posts = _rows(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": "400768409",
                "author": "雪球作者",
                "raw_text": "雪球作者：评论正文",
                "created_at": "2026-03-27 11:36:44",
            }
        ]
    )

    _label, tree_text = build_post_tree(post_uid=post_uid, posts=posts)

    assert "雪球作者：评论正文" in tree_text
    assert "[原帖 ID: 400768409]" in tree_text
