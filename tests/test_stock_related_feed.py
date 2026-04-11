from __future__ import annotations

from alphavault_reflex.services.stock_related_feed import (
    DEFAULT_RELATED_LIMIT,
    MAX_RELATED_LIMIT,
    RELATED_FILTER_ALL,
    RELATED_FILTER_SIGNAL,
    build_related_feed,
    normalize_related_filter,
    normalize_related_limit,
)


def test_normalize_related_filter_defaults_to_all() -> None:
    assert normalize_related_filter("") == RELATED_FILTER_ALL
    assert normalize_related_filter("wat") == RELATED_FILTER_ALL
    assert normalize_related_filter("ALL") == RELATED_FILTER_ALL
    assert normalize_related_filter("signal") == RELATED_FILTER_SIGNAL


def test_normalize_related_limit_clamps_and_defaults() -> None:
    assert normalize_related_limit("") == DEFAULT_RELATED_LIMIT
    assert normalize_related_limit("0") == DEFAULT_RELATED_LIMIT
    assert normalize_related_limit("-1") == DEFAULT_RELATED_LIMIT
    assert normalize_related_limit("2") == 2
    assert normalize_related_limit(str(MAX_RELATED_LIMIT + 1000)) == MAX_RELATED_LIMIT


def test_build_related_feed_sorts_and_slices_signals_only() -> None:
    signals = [
        {
            "post_uid": "weibo:1",
            "summary": "s1",
            "action": "trade.buy",
            "author": "a",
            "created_at": "2026-04-01 10:00",
            "created_at_line": "2026-04-01 10:00",
            "url": "u1",
            "raw_text": "t",
            "tree_text": "tree1",
        },
        {
            "post_uid": "weibo:2",
            "summary": "s2",
            "action": "trade.sell",
            "author": "b",
            "created_at": "2026-04-01 09:00",
            "created_at_line": "2026-04-01 09:00",
            "url": "u2",
            "raw_text": "t2",
            "tree_text": "tree2",
        },
    ]

    feed = build_related_feed(
        signals=signals,
        related_filter="all",
        limit=2,
        now="2026-04-01 10:30",
    )
    assert feed.total == 2
    assert [row["post_uid"] for row in feed.rows] == ["weibo:1", "weibo:2"]
    assert feed.rows[0]["is_signal"] == "1"
    assert feed.rows[0]["signal_badge"] == "买"
    assert feed.rows[0]["title"] == "s1"
    assert feed.rows[0]["created_at_line"] == "2026-04-01 10:00 · 30分钟前"
    assert feed.rows[0]["tree_lines"][0]["content"] == "tree1"
    assert feed.rows[0]["tree_preview_lines"][0]["content"] == "tree1"
    assert feed.rows[0]["tree_can_expand"] == ""
    assert feed.rows[0]["tree_expand_label"] == ""
    assert feed.rows[1]["is_signal"] == "1"
    assert feed.rows[1]["signal_badge"] == "卖"
    assert feed.rows[1]["action"] == "trade.sell"
    assert feed.rows[1]["raw_text"] == "t2"
    assert feed.rows[1]["title"] == "s2"
    assert feed.rows[1]["created_at_line"] == "2026-04-01 09:00 · 1小时前"
    assert feed.rows[1]["tree_lines"][0]["content"] == "tree2"
    assert "display_md" not in feed.rows[0]


def test_build_related_feed_signal_filter_keeps_signals() -> None:
    feed = build_related_feed(
        signals=[{"post_uid": "p1", "summary": "s", "action": "trade.buy"}],
        related_filter="signal",
        limit=20,
    )
    assert feed.total == 1
    assert [row["post_uid"] for row in feed.rows] == ["p1"]


def test_build_related_feed_rebuilds_weibo_reply_chain_tree_when_missing() -> None:
    raw_chain = (
        "回复@挖地瓜的超级鹿鼎公:卖了10万股"
        "//@挖地瓜的超级鹿鼎公:回复@一名造价师De:卖了10万股"
        "//@一名造价师De:公公你是怎么做到上个月新城成本0.753港币，这个月就到了0.3"
    )
    feed = build_related_feed(
        signals=[
            {
                "post_uid": "weibo:5260841549040598",
                "summary": "s",
                "action": "trade.watch",
                "author": "一名造价师De",
                "created_at": "2026-04-01 10:00",
                "created_at_line": "2026-04-01 10:00",
                "url": "u1",
                "raw_text": raw_chain,
                "tree_text": "",
            }
        ],
        related_filter="all",
        limit=20,
        now="2026-04-01 10:30",
    )
    assert len(feed.rows) == 1
    tree_text = feed.rows[0]["tree_text"]
    assert tree_text.startswith(
        "📌一名造价师De：公公你是怎么做到上个月新城成本0.753港币，这个月就到了0.3"
    )
    assert "└── @挖地瓜的超级鹿鼎公：卖了10万股" in tree_text
    assert "└── 📌一名造价师De：卖了10万股" in tree_text
    assert any(line["prefix"] != "" for line in feed.rows[0]["tree_lines"])


def test_build_related_feed_rebuilds_tree_even_when_cached_tree_text_exists() -> None:
    feed = build_related_feed(
        signals=[
            {
                "post_uid": "weibo:1",
                "summary": "s",
                "action": "trade.watch",
                "author": "A",
                "created_at": "2026-04-01 10:00",
                "url": "u",
                "raw_text": "回复@B:最后一条//@B:上一条",
                "tree_text": "旧缓存",
            }
        ],
        related_filter="all",
        limit=20,
    )
    assert len(feed.rows) == 1
    assert feed.rows[0]["tree_text"] != "旧缓存"
    assert "└── " in feed.rows[0]["tree_text"]


def test_build_related_feed_hides_weibo_meta_and_forward_markers() -> None:
    feed = build_related_feed(
        signals=[
            {
                "post_uid": "weibo:9",
                "summary": "s",
                "action": "trade.watch",
                "author": "A",
                "created_at": "2026-04-01 10:00",
                "raw_text": "",
                "tree_text": "A：第一句\n[微博元信息]\n@用户: x,y\n[转发原文]\nB：第二句",
            }
        ],
        related_filter="all",
        limit=20,
    )
    assert len(feed.rows) == 1
    tree_text = feed.rows[0]["tree_text"]
    assert "[微博元信息]" not in tree_text
    assert "[转发原文]" not in tree_text
    assert "@用户:" not in tree_text


def test_build_related_feed_long_forward_original_keeps_single_root_and_inline_expand() -> (
    None
):
    long_original = (
        "游戏仓2026年2月PS图 本游戏仓2月收盘1888.5W 相比本月初1862.2W 本月盈利26.3W "
        "本月盈利1.41% 本月上证指数上涨1.09% 本月跑赢0.32% "
        "相比本年初1762.5W 本年盈利金额126W 盈利7.15%"
    )
    raw_text = (
        "回复@透明水纹:分红//@透明水纹:舅舅，长电T了两毛钱。。\n\n"
        "[微博元信息]\n@用户: 透明水纹,透明水纹\n\n"
        f"[转发原文]\n{long_original}\n\n"
        '[CSV原始字段]\n{"源微博id":"5270950817566951"}'
    )
    feed = build_related_feed(
        signals=[
            {
                "post_uid": "weibo:5270975543513975",
                "summary": "s",
                "action": "trade.watch",
                "author": "挖地瓜的超级鹿鼎公",
                "created_at": "2026-04-01 10:00",
                "raw_text": raw_text,
                "tree_text": "旧缓存树",
            }
        ],
        related_filter="all",
        limit=20,
    )
    assert len(feed.rows) == 1
    row = feed.rows[0]
    tree_text = row["tree_text"]
    assert tree_text.startswith("📌挖地瓜的超级鹿鼎公：游戏仓2026年2月PS图")
    assert "└── @透明水纹：舅舅，长电T了两毛钱。。" in tree_text
    assert "[微博元信息]" not in tree_text
    assert "[转发原文]" not in tree_text
    assert tree_text.count("游戏仓2026年2月PS图") == 1
    assert "[原帖 ID: 5270950817566951]" in tree_text
    assert row["tree_root_can_expand"] == "1"
    assert row["tree_root_preview_line"]["content"].endswith("…")
    assert row["tree_root_preview_line"]["id_suffix"] == "[原帖 ID: 5270950817566951]"
    assert row["tree_root_expand_line"]["content"] != ""
    assert row["tree_root_expand_line"]["id_suffix"] == ""


def test_build_related_feed_original_post_keeps_tree_structure() -> None:
    expected_tree = (
        "游戏仓2026年2月PS图 ... [原帖 ID: 5270950817566951]\n"
        "└── @透明水纹：舅舅，长电T了两毛钱。。\n"
        "    └── 📌挖地瓜的超级鹿鼎公：分红 [转发 ID: 5270975543513975]"
    )
    feed = build_related_feed(
        signals=[
            {
                "post_uid": "weibo:5270950817566951",
                "summary": "s",
                "action": "trade.watch",
                "author": "挖地瓜的超级鹿鼎公",
                "created_at": "2026-04-01 10:00",
                "raw_text": "游戏仓2026年2月PS图\n本游戏仓2月收盘1888.5W",
                "tree_text": expected_tree,
            }
        ],
        related_filter="all",
        limit=20,
    )
    assert len(feed.rows) == 1
    assert feed.rows[0]["tree_text"] == expected_tree
    assert len(feed.rows[0]["tree_lines"]) == 3
    assert feed.rows[0]["tree_lines"][2]["content"] == "📌挖地瓜的超级鹿鼎公：分红"


def test_build_related_feed_keeps_multi_line_tree_for_normal_original_post() -> None:
    expected_tree = (
        "作者A：根节点 [原帖 ID: 100]\n"
        "└── 作者B：子节点 [转发 ID: 101]\n"
        "    └── 作者C：叶子 [转发 ID: 102]"
    )
    feed = build_related_feed(
        signals=[
            {
                "post_uid": "weibo:100",
                "summary": "s",
                "action": "trade.watch",
                "author": "作者A",
                "created_at": "2026-04-01 10:00",
                "raw_text": "普通原帖正文",
                "tree_text": expected_tree,
            }
        ],
        related_filter="all",
        limit=20,
    )

    assert len(feed.rows) == 1
    assert feed.rows[0]["tree_text"] == expected_tree
    assert len(feed.rows[0]["tree_lines"]) == 3
    assert feed.rows[0]["tree_lines"][1]["prefix"] != ""


def test_build_related_feed_original_colon_line_not_prefixed_with_at() -> None:
    feed = build_related_feed(
        signals=[
            {
                "post_uid": "weibo:100",
                "summary": "s",
                "action": "trade.watch",
                "author": "作者A",
                "created_at": "2026-04-01 10:00",
                "raw_text": "本游戏仓：2015年8月18日3999点入市",
                "tree_text": "本游戏仓：2015年8月18日3999点入市 [原帖 ID: 100]",
            }
        ],
        related_filter="all",
        limit=20,
    )
    assert len(feed.rows) == 1
    assert feed.rows[0]["tree_text"].startswith("本游戏仓：2015年8月18日3999点入市")
    assert not feed.rows[0]["tree_text"].startswith("@本游戏仓")
