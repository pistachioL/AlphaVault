from __future__ import annotations

import json
from typing import Any, cast

import libsql

from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.turso_db import TursoConnection
from alphavault.domains.entity_match.resolve import EntityMatchResult
from alphavault.research_workbench import RESEARCH_RELATION_CANDIDATES_TABLE
from alphavault.worker.post_processor_topic_prompt_v4 import (
    map_topic_prompt_assertions_to_rows,
    resolve_rows_entity_matches,
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
            "entity_key": "industry:白酒",
            "entity_type": "industry",
            "source_mention_text": "白酒",
            "source_mention_type": "industry_name",
            "confidence": 0.85,
        },
    ]


def test_map_topic_prompt_assertions_to_rows_falls_back_to_current_post_uid_for_talk_reply() -> (
    None
):
    ai_result: dict[str, object] = {
        "topic_status_id": "status-2",
        "topic_summary": "他说可以买。",
        "assertions": [
            {
                "speaker": "老王",
                "relation_to_topic": "new",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "他说可以买。",
                "evidence_refs": [
                    {
                        "source_kind": "talk_reply",
                        "source_id": "src:virtual",
                        "quote": "我觉得可以买",
                    }
                ],
                "mentions": ["茅台"],
            }
        ],
        "mentions": [
            {
                "mention_text": "茅台",
                "mention_type": "stock_alias",
                "evidence": "茅台我觉得可以买",
                "confidence": 0.9,
            }
        ],
    }

    rows_by_post_uid = map_topic_prompt_assertions_to_rows(
        ai_result=ai_result,
        focus_username="老王",
        message_lookup={
            ("talk_reply", "src:virtual"): {"text": "茅台我觉得可以买"},
        },
        post_uid_by_platform_post_id={},
        fallback_post_uid="weibo:300",
    )

    assert "weibo:300" in rows_by_post_uid
    assert len(rows_by_post_uid["weibo:300"]) == 1


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


def test_resolve_rows_entity_matches_overwrites_entities_and_persists_candidates() -> (
    None
):
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        apply_cloud_schema(conn)
        rows_by_post_uid: dict[str, list[dict[str, object]]] = {
            "weibo:1": [
                {
                    "assertion_mentions": cast(
                        list[dict[str, Any]],
                        [
                            {
                                "mention_text": "600519",
                                "mention_type": "stock_code",
                                "confidence": 0.95,
                            },
                            {
                                "mention_text": "茅台",
                                "mention_type": "stock_alias",
                                "confidence": 0.9,
                            },
                        ],
                    ),
                    "assertion_entities": cast(
                        list[dict[str, Any]],
                        [
                            {
                                "entity_key": "stock:600519.SH",
                                "entity_type": "stock",
                                "source_mention_text": "茅台",
                                "source_mention_type": "stock_alias",
                                "confidence": 0.9,
                            }
                        ],
                    ),
                }
            ]
        }

        followups_by_post_uid = resolve_rows_entity_matches(conn, rows_by_post_uid)

        assert rows_by_post_uid["weibo:1"][0]["assertion_entities"] == [
            {
                "entity_key": "stock:600519.SH",
                "entity_type": "stock",
                "source_mention_text": "600519",
                "source_mention_type": "stock_code",
                "confidence": 0.95,
            }
        ]
        followups = followups_by_post_uid["weibo:1"]
        assert len(followups) == 1
        assert followups[0].relation_candidates == [
            {
                "candidate_id": "stock_alias|alias_of|stock:600519.SH|stock:茅台",
                "relation_type": "stock_alias",
                "left_key": "stock:600519.SH",
                "right_key": "stock:茅台",
                "relation_label": "alias_of",
                "suggestion_reason": "同条观点里代码和简称一起出现",
                "evidence_summary": "同条观点里代码和简称一起出现",
                "score": 0.9,
                "ai_status": "skipped",
            }
        ]
        assert followups[0].alias_task_keys == []
        candidate_rows = (
            conn.execute(
                f"""
SELECT left_key, right_key, relation_type
FROM {RESEARCH_RELATION_CANDIDATES_TABLE}
ORDER BY candidate_id
"""
            )
            .mappings()
            .all()
        )
        assert candidate_rows == []
    finally:
        conn.close()


def test_resolve_rows_entity_matches_prefetches_thread_lookups_once(
    monkeypatch,
) -> None:
    from alphavault.worker import post_processor_topic_prompt_v4 as worker_module

    load_calls: list[tuple[list[str], list[str]]] = []
    resolve_calls: list[tuple[dict[str, str] | None, dict[str, str] | None]] = []

    def _fake_load_entity_match_lookup_maps(
        _engine_or_conn,
        *,
        stock_name_texts: list[str],
        stock_alias_texts: list[str],
    ) -> tuple[dict[str, str], dict[str, str]]:
        load_calls.append((list(stock_name_texts), list(stock_alias_texts)))
        return (
            {"贵州茅台": "stock:600519.SH"},
            {"茅台": "stock:600519.SH"},
        )

    monkeypatch.setattr(
        worker_module,
        "load_entity_match_lookup_maps",
        _fake_load_entity_match_lookup_maps,
        raising=False,
    )

    def _fake_resolve(
        _engine_or_conn,
        *,
        assertion_mentions,
        stock_name_targets=None,
        stock_alias_targets=None,
    ) -> EntityMatchResult:
        del assertion_mentions
        resolve_calls.append((stock_name_targets, stock_alias_targets))
        return EntityMatchResult([], [], [])

    monkeypatch.setattr(worker_module, "resolve_assertion_mentions", _fake_resolve)

    rows_by_post_uid: dict[str, list[dict[str, object]]] = {
        "weibo:1": [
            {
                "assertion_mentions": [
                    {"mention_text": "贵州茅台", "mention_type": "stock_name"},
                    {"mention_text": "茅台", "mention_type": "stock_alias"},
                ]
            },
            {
                "assertion_mentions": [
                    {"mention_text": "茅台", "mention_type": "stock_alias"}
                ]
            },
        ]
    }

    followups_by_post_uid = resolve_rows_entity_matches(object(), rows_by_post_uid)

    assert load_calls == [(["贵州茅台"], ["茅台"])]
    assert resolve_calls == [
        ({"贵州茅台": "stock:600519.SH"}, {"茅台": "stock:600519.SH"}),
        ({"贵州茅台": "stock:600519.SH"}, {"茅台": "stock:600519.SH"}),
    ]
    assert followups_by_post_uid == {"weibo:1": []}
