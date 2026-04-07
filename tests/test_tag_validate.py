from __future__ import annotations

import pytest

from alphavault.ai.tag_validate import (
    AiTagValidationError,
    validate_assertion_row,
    validate_topic_prompt_v4_ai_result,
)
from alphavault.ai.topic_prompt_v4 import TOPIC_PROMPT_VERSION


def test_topic_prompt_v4_ok_mentions_and_assertions() -> None:
    parsed = {
        "topic_status_id": "status-1",
        "topic_summary": "他主要在说茅台现在先看，不急着追。",
        "assertions": [
            {
                "speaker": "老王",
                "relation_to_topic": "new",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "他说已经开始买了茅台。",
                "evidence_refs": [
                    {
                        "source_kind": "status",
                        "source_id": "1001",
                        "quote": "我今天开始买600519了",
                    }
                ],
                "mentions": ["600519", "茅台"],
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
                "evidence": "我今天开始买600519了，先看茅台",
                "confidence": 0.9,
            },
        ],
    }
    validate_topic_prompt_v4_ai_result(parsed)


def test_topic_prompt_v4_ok_keyword_topic_and_follow_relation() -> None:
    parsed = {
        "topic_status_id": "status-2",
        "topic_summary": "他是在接着前面的话继续看汇率。",
        "assertions": [
            {
                "speaker": "老王",
                "relation_to_topic": "follow",
                "action": "trade.watch",
                "action_strength": 1,
                "summary": "他觉得汇率这事还得再看。",
                "evidence_refs": [
                    {
                        "source_kind": "comment",
                        "source_id": "2002",
                        "quote": "汇率这事先看看",
                    }
                ],
                "mentions": ["汇率"],
            }
        ],
        "mentions": [
            {
                "mention_text": "汇率",
                "mention_type": "keyword",
                "evidence": "汇率这事先看看",
                "confidence": 0.8,
            }
        ],
    }
    validate_topic_prompt_v4_ai_result(parsed)


def test_topic_prompt_v4_ok_commodity_mention() -> None:
    parsed = {
        "topic_status_id": "status-commodity-1",
        "topic_summary": "他主要在说黄金还可以继续看。",
        "assertions": [
            {
                "speaker": "老王",
                "relation_to_topic": "new",
                "action": "view.bullish",
                "action_strength": 1,
                "summary": "他说黄金还行。",
                "evidence_refs": [
                    {
                        "source_kind": "status",
                        "source_id": "5005",
                        "quote": "黄金我继续看",
                    }
                ],
                "mentions": ["黄金"],
            }
        ],
        "mentions": [
            {
                "mention_text": "黄金",
                "mention_type": "commodity_name",
                "evidence": "黄金我继续看",
                "confidence": 0.88,
            }
        ],
    }
    validate_topic_prompt_v4_ai_result(parsed)


def test_topic_prompt_v4_reject_assertion_mention_not_defined() -> None:
    parsed = {
        "topic_status_id": "status-3",
        "topic_summary": "他提了一只票。",
        "assertions": [
            {
                "speaker": "老王",
                "relation_to_topic": "new",
                "action": "trade.watch",
                "action_strength": 1,
                "summary": "他说先看着。",
                "evidence_refs": [
                    {
                        "source_kind": "status",
                        "source_id": "3003",
                        "quote": "长电先看着",
                    }
                ],
                "mentions": ["长电"],
            }
        ],
        "mentions": [],
    }
    with pytest.raises(AiTagValidationError):
        validate_topic_prompt_v4_ai_result(parsed)


def test_topic_prompt_v4_reject_bad_mention_type() -> None:
    parsed = {
        "topic_status_id": "status-4",
        "topic_summary": "他说了一句。",
        "assertions": [
            {
                "speaker": "老王",
                "relation_to_topic": "new",
                "action": "trade.watch",
                "action_strength": 1,
                "summary": "他说先别动。",
                "evidence_refs": [
                    {
                        "source_kind": "status",
                        "source_id": "4004",
                        "quote": "先别动",
                    }
                ],
                "mentions": ["长电"],
            }
        ],
        "mentions": [
            {
                "mention_text": "长电",
                "mention_type": "company_name",
                "evidence": "先别动",
                "confidence": 0.9,
            }
        ],
    }
    with pytest.raises(AiTagValidationError):
        validate_topic_prompt_v4_ai_result(parsed)


def test_db_row_ok_v4_assertion_row_minimal_fields() -> None:
    row = {
        "assertion_id": "weibo:1#1",
        "post_uid": "weibo:1",
        "idx": 1,
        "action": "trade.watch",
        "action_strength": 1,
        "summary": "他说先看着。",
        "evidence": "长电先看着",
        "created_at": "2026-03-28 12:00:00",
    }
    validate_assertion_row(row, prompt_version=TOPIC_PROMPT_VERSION)


def test_db_row_rejects_missing_assertion_id() -> None:
    row = {
        "post_uid": "weibo:1",
        "idx": 1,
        "action": "view.bullish",
        "action_strength": 1,
        "summary": "他说继续看。",
        "evidence": "黄金继续看",
        "created_at": "2026-03-28 12:00:00",
    }
    with pytest.raises(AiTagValidationError):
        validate_assertion_row(row, prompt_version=TOPIC_PROMPT_VERSION)


def test_db_row_rejects_empty_evidence() -> None:
    row = {
        "assertion_id": "weibo:1#1",
        "post_uid": "weibo:1",
        "idx": 1,
        "action": "view.bullish",
        "action_strength": 1,
        "summary": "他说继续看。",
        "evidence": "",
        "created_at": "2026-03-28 12:00:00",
    }
    with pytest.raises(AiTagValidationError):
        validate_assertion_row(row, prompt_version=TOPIC_PROMPT_VERSION)
