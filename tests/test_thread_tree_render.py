from __future__ import annotations

from datetime import datetime, timezone

from alphavault.domains.thread_tree.service import build_post_tree
from alphavault.domains.thread_tree.service import build_post_tree_map


def _rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return rows


def test_build_post_tree_keeps_compact_weibo_reply_chain_as_raw_text() -> None:
    post_uid = "xueqiu:https://weibo.com/3962719063/QxH0rF27I"
    raw_text = (
        "回复@落晚平沙:看走势，可以看出大家想法//@落晚平沙:请教公公碰到好几次这个问题"
        "但是没有想明白：1.根据年报，此时是该保持现有持仓吃5.3%股息还是分红前卖出"
        "逃权？2.已知2026一季度财报会较差，是不是应该卖出当前仓位，待一季报出来"
        "再低位买入呢[抱抱][抱抱]//@挖地瓜的超级鹿鼎公:回复@勇敢的心E11:并没有，"
        "投资资金和负债也没啥眼睛一亮的数据变化，还是老样子//@勇敢的心E11:超预期"
        "[赞] - 转发 @挖地瓜的超级鹿鼎公: 华能国际2025年年报显示，当年度公司主营"
        "收入2292.88亿元，同比下降6.62%；归母净利润144.1亿元，同比上升42.17%；"
        "扣非净利润134.82亿元，同比上升28.13% 年报每股分红0.40元"
    )
    posts = _rows(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": "https://weibo.com/3962719063/QxH0rF27I",
                "author": "挖地瓜的超级鹿鼎公",
                "raw_text": raw_text,
                "created_at": "2026-03-25 10:23:48",
            }
        ]
    )

    _label, tree_text = build_post_tree(post_uid=post_uid, posts=posts)

    assert "回复@落晚平沙:看走势，可以看出大家想法" in tree_text
    assert "勇敢的心E11：超预期[赞]" not in tree_text
    assert "落晚平沙：请教公公碰到好几次这个问题但是没有想明白" not in tree_text
    assert "[回复]" not in tree_text


def test_build_post_tree_keeps_plain_repost_raw_text() -> None:
    post_uid = "xueqiu:https://weibo.com/1000/abc"
    raw_text = "转发 @原作者: 原文内容"
    posts = _rows(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": "https://weibo.com/1000/abc",
                "author": "转发的人",
                "raw_text": raw_text,
                "created_at": "2026-03-25 10:23:48",
            }
        ]
    )

    _label, tree_text = build_post_tree(post_uid=post_uid, posts=posts)

    assert "转发 @原作者: 原文内容 [转发 ID: https://weibo.com/1000/abc]" in tree_text
    assert "原作者：原文内容" not in tree_text


def test_build_post_tree_keeps_compact_repost_chain_in_current_node() -> None:
    post_uid = "weibo:222"
    raw_text = (
        "当前评论//@A:中间回复//@B:最早回复\n\n"
        "[转发原文]\n原微博正文\n\n"
        '[CSV原始字段]\n{"源用户昵称":"原作者","源微博id":"111","源微博正文":"原微博正文"}'
    )
    posts = _rows(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": "222",
                "author": "当前作者",
                "raw_text": raw_text,
                "created_at": "2026-03-25 10:23:48",
            }
        ]
    )

    _label, tree_text = build_post_tree(post_uid=post_uid, posts=posts)

    assert "原微博正文 [源帖 ID: 111]" in tree_text
    assert "当前评论//@A:中间回复//@B:最早回复 [转发 ID: 222]" in tree_text
    assert "B：最早回复" not in tree_text
    assert "A：中间回复" not in tree_text


def test_build_post_tree_map_handles_mixed_tz_created_at() -> None:
    posts = _rows(
        [
            {
                "post_uid": "weibo:1",
                "platform_post_id": "1",
                "author": "a",
                "raw_text": "第一条",
                "created_at": datetime(2026, 3, 25, 10, 23, 48),
            },
            {
                "post_uid": "weibo:2",
                "platform_post_id": "2",
                "author": "b",
                "raw_text": "第二条",
                "created_at": datetime(2026, 3, 25, 11, 23, 48, tzinfo=timezone.utc),
            },
        ]
    )

    tree_map = build_post_tree_map(post_uids=["weibo:1", "weibo:2"], posts=posts)

    assert set(tree_map.keys()) == {"weibo:1", "weibo:2"}
