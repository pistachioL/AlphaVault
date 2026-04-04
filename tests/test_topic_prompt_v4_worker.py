from __future__ import annotations

import json

from alphavault.worker.post_processor_topic_prompt_v4 import (
    map_topic_prompt_assertions_to_rows,
)


def test_map_topic_prompt_assertions_to_rows_keeps_mentions_and_derives_topic_key() -> (
    None
):
    ai_result: dict[str, object] = {
        "topic_status_id": "status-1",
        "topic_summary": "他说开始买茅台。",
        "assertions": [
            {
                "speaker": "老王",
                "relation_to_topic": "new",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "他说已经开始买了。",
                "evidence_refs": [
                    {
                        "source_kind": "status",
                        "source_id": "1001",
                        "quote": "我今天开始买600519了，茅台先上车",
                    }
                ],
                "mentions": ["600519", "茅台", "白酒"],
            }
        ],
        "mentions": [
            {
                "mention_text": "600519",
                "mention_type": "stock_code",
                "evidence": "我今天开始买600519了",
                "confidence": 0.95,
            },
            {
                "mention_text": "茅台",
                "mention_type": "stock_alias",
                "evidence": "茅台先上车",
                "confidence": 0.9,
            },
            {
                "mention_text": "白酒",
                "mention_type": "industry_name",
                "evidence": "白酒这块也在看",
                "confidence": 0.85,
            },
        ],
    }

    rows_by_post_uid = map_topic_prompt_assertions_to_rows(
        ai_result=ai_result,
        focus_username="老王",
        message_lookup={
            ("status", "1001"): {
                "text": "我今天开始买600519了，茅台先上车，白酒这块也在看"
            }
        },
        post_uid_by_platform_post_id={"1001": "weibo:1001"},
    )

    rows = rows_by_post_uid["weibo:1001"]
    assert len(rows) == 1
    row = rows[0]
    assert row["topic_key"] == "stock:600519"
    assert json.loads(str(row["stock_codes_json"])) == ["600519"]
    assert json.loads(str(row["stock_names_json"])) == ["茅台"]
    assert json.loads(str(row["industries_json"])) == ["白酒"]
    assert json.loads(str(row["keywords_json"])) == []
    assert row["speaker"] == "老王"
    assert row["relation_to_topic"] == "new"
    assert json.loads(str(row["evidence_refs_json"])) == [
        {
            "source_kind": "status",
            "source_id": "1001",
            "quote": "我今天开始买600519了，茅台先上车",
        }
    ]
    assert row["assertion_mentions"] == [
        {
            "mention_text": "600519",
            "mention_type": "stock_code",
            "evidence": "我今天开始买600519了",
            "confidence": 0.95,
        },
        {
            "mention_text": "茅台",
            "mention_type": "stock_alias",
            "evidence": "茅台先上车",
            "confidence": 0.9,
        },
        {
            "mention_text": "白酒",
            "mention_type": "industry_name",
            "evidence": "白酒这块也在看",
            "confidence": 0.85,
        },
    ]
    assert row["assertion_entities"] == [
        {
            "entity_key": "stock:600519.SH",
            "entity_type": "stock",
            "source_mention_text": "600519",
            "source_mention_type": "stock_code",
            "confidence": 0.95,
        },
        {
            "entity_key": "stock:600519.SH",
            "entity_type": "stock",
            "source_mention_text": "茅台",
            "source_mention_type": "stock_alias",
            "confidence": 0.9,
        },
        {
            "entity_key": "industry:白酒",
            "entity_type": "industry",
            "source_mention_text": "白酒",
            "source_mention_type": "industry_name",
            "confidence": 0.85,
        },
    ]


def test_map_topic_prompt_assertions_to_rows_supports_commodity_layer() -> None:
    ai_result: dict[str, object] = {
        "topic_status_id": "status-commodity-2",
        "topic_summary": "他说黄金可以继续看。",
        "assertions": [
            {
                "speaker": "老王",
                "relation_to_topic": "new",
                "action": "view.bullish",
                "action_strength": 1,
                "summary": "他说黄金还不错。",
                "evidence_refs": [
                    {
                        "source_kind": "status",
                        "source_id": "2002",
                        "quote": "黄金我继续看",
                    }
                ],
                "mentions": ["黄金", "汇率"],
            }
        ],
        "mentions": [
            {
                "mention_text": "黄金",
                "mention_type": "commodity_name",
                "evidence": "黄金我继续看",
                "confidence": 0.88,
            },
            {
                "mention_text": "汇率",
                "mention_type": "keyword",
                "evidence": "汇率也得一起盯",
                "confidence": 0.7,
            },
        ],
    }

    rows_by_post_uid = map_topic_prompt_assertions_to_rows(
        ai_result=ai_result,
        focus_username="老王",
        message_lookup={("status", "2002"): {"text": "黄金我继续看，汇率也得一起盯"}},
        post_uid_by_platform_post_id={"2002": "weibo:2002"},
    )

    rows = rows_by_post_uid["weibo:2002"]
    assert len(rows) == 1
    row = rows[0]
    assert row["topic_key"] == "commodity:黄金"
    assert json.loads(str(row["commodities_json"])) == ["黄金"]
    assert json.loads(str(row["keywords_json"])) == ["汇率"]
    assert row["assertion_entities"] == [
        {
            "entity_key": "commodity:黄金",
            "entity_type": "commodity",
            "source_mention_text": "黄金",
            "source_mention_type": "commodity_name",
            "confidence": 0.88,
        },
        {
            "entity_key": "keyword:汇率",
            "entity_type": "keyword",
            "source_mention_text": "汇率",
            "source_mention_type": "keyword",
            "confidence": 0.7,
        },
    ]
