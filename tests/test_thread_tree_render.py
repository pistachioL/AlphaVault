from __future__ import annotations

import pandas as pd

from alphavault_reflex.services.thread_tree import build_post_tree


def test_build_post_tree_expands_compact_weibo_reply_chain() -> None:
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
    posts = pd.DataFrame(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": "https://weibo.com/3962719063/QxH0rF27I",
                "author": "挖地瓜的超级鹿鼎公",
                "raw_text": raw_text,
                "display_md": raw_text,
                "created_at": "2026-03-25 10:23:48",
            }
        ]
    )

    _label, tree_text = build_post_tree(post_uid=post_uid, posts=posts)

    assert "勇敢的心E11：超预期[赞]" in tree_text
    assert "落晚平沙：请教公公碰到好几次这个问题但是没有想明白" in tree_text
    assert (
        "挖地瓜的超级鹿鼎公：看走势，可以看出大家想法 "
        "[转发 ID: https://weibo.com/3962719063/QxH0rF27I]"
    ) in tree_text
    assert "[回复]" not in tree_text
    assert (
        "[转发 ID: https://weibo.com/3962719063/QxH0rF27I] "
        "挖地瓜的超级鹿鼎公：看走势，可以看出大家想法"
    ) not in tree_text


def test_build_post_tree_shows_plain_repost_as_author_forward() -> None:
    post_uid = "xueqiu:https://weibo.com/1000/abc"
    raw_text = "转发 @原作者: 原文内容"
    posts = pd.DataFrame(
        [
            {
                "post_uid": post_uid,
                "platform_post_id": "https://weibo.com/1000/abc",
                "author": "转发的人",
                "raw_text": raw_text,
                "display_md": raw_text,
                "created_at": "2026-03-25 10:23:48",
            }
        ]
    )

    _label, tree_text = build_post_tree(post_uid=post_uid, posts=posts)

    assert "原作者：原文内容" in tree_text
    assert "转发的人：转发 [转发 ID: https://weibo.com/1000/abc]" in tree_text
