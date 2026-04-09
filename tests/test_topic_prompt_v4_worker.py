from __future__ import annotations
from typing import Any, cast

from alphavault.constants import SCHEMA_STANDARD
from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.postgres_db import PostgresConnection
from alphavault.db.turso_queue import CloudPost
from alphavault.domains.entity_match.resolve import EntityMatchResult
from alphavault.research_workbench import (
    RESEARCH_ALIAS_RESOLVE_TASKS_TABLE,
    RESEARCH_RELATION_CANDIDATES_TABLE,
    RESEARCH_RELATIONS_TABLE,
    RESEARCH_SECURITY_MASTER_TABLE,
)
from alphavault.rss.utils import RateLimiter
from alphavault.worker import post_processor_topic_prompt_v4 as topic_prompt_module
from alphavault.worker.post_processor_topic_prompt_v4 import (
    map_topic_prompt_assertions_to_rows,
    resolve_rows_entity_matches,
)
from alphavault.worker.runtime_models import LLMConfig


def _build_config() -> LLMConfig:
    return LLMConfig(
        api_key="test-key",
        model="test-model",
        prompt_version="topic_prompt_v4",
        relevant_threshold=0.5,
        base_url="",
        api_mode="responses",
        ai_stream=False,
        ai_retries=0,
        ai_temperature=0.1,
        ai_reasoning_effort="low",
        ai_rpm=0.0,
        ai_timeout_seconds=30.0,
        trace_out=None,
        verbose=False,
    )


def _workbench_conn(pg_conn) -> PostgresConnection:
    apply_cloud_schema(pg_conn, target="standard", schema_name=SCHEMA_STANDARD)
    pg_conn.execute(
        f"""
TRUNCATE TABLE
  {RESEARCH_ALIAS_RESOLVE_TASKS_TABLE},
  {RESEARCH_RELATION_CANDIDATES_TABLE},
  {RESEARCH_RELATIONS_TABLE},
  {RESEARCH_SECURITY_MASTER_TABLE}
RESTART IDENTITY CASCADE
"""
    )
    return PostgresConnection(pg_conn, schema_name=SCHEMA_STANDARD)


def test_map_topic_prompt_assertions_to_rows_keeps_mentions_and_derives_entities() -> (
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
    assert row["action"] == "trade.buy"
    assert row["action_strength"] == 2
    assert row["summary"] == "他说已经开始买了。"
    assert row["evidence"] == "我今天开始买600519了，茅台先上车"
    assert row["assertion_mentions"] == [
        {
            "mention_text": "600519",
            "mention_norm": "600519",
            "mention_type": "stock_code",
            "evidence": "我今天开始买600519了",
            "confidence": 0.95,
        },
        {
            "mention_text": "茅台",
            "mention_norm": "茅台",
            "mention_type": "stock_alias",
            "evidence": "茅台先上车",
            "confidence": 0.9,
        },
        {
            "mention_text": "白酒",
            "mention_norm": "白酒",
            "mention_type": "industry_name",
            "evidence": "白酒这块也在看",
            "confidence": 0.85,
        },
    ]
    assert row["assertion_entities"] == [
        {
            "entity_key": "stock:600519.SH",
            "entity_type": "stock",
            "match_source": "stock_code",
            "is_primary": 1,
        },
        {
            "entity_key": "industry:白酒",
            "entity_type": "industry",
            "match_source": "industry_name",
            "is_primary": 0,
        },
    ]


def test_process_one_post_uid_topic_prompt_v4_passes_limiter_wait_as_request_gate(
    monkeypatch,
) -> None:
    wait_calls: list[str] = []
    seen_request_gate: dict[str, object] = {}
    writes: list[str] = []
    dirty_marks: list[str] = []
    limiter = RateLimiter(0)
    monkeypatch.setattr(limiter, "wait", lambda: wait_calls.append("wait"))

    monkeypatch.setattr(
        topic_prompt_module,
        "load_cloud_post",
        lambda _engine, _post_uid: CloudPost(
            post_uid="weibo:1",
            platform="weibo",
            platform_post_id="1001",
            author="老王",
            created_at="2026-04-07 10:00:00",
            url="https://example.com/post/1001",
            raw_text="茅台我觉得可以买",
            ai_retry_count=0,
        ),
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "thread_root_info_for_post",
        lambda **_kwargs: ("root:1001", "", ""),
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "build_topic_prompt_v4_with_prompt_chars_limit",
        lambda **_kwargs: (
            {"message_lookup": {("status", "1001"): {"text": "茅台我觉得可以买"}}},
            0,
            "prompt",
            10,
            10,
            False,
            True,
        ),
    )

    def _fake_call_ai(**kwargs):  # type: ignore[no-untyped-def]
        seen_request_gate["value"] = kwargs.get("request_gate")
        request_gate = kwargs.get("request_gate")
        if callable(request_gate):
            request_gate()
        return {
            "assertions": [
                {
                    "speaker": "老王",
                    "action": "trade.buy",
                    "action_strength": 1,
                    "summary": "可以买",
                    "evidence_refs": [
                        {
                            "source_kind": "status",
                            "source_id": "1001",
                            "quote": "茅台我觉得可以买",
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

    monkeypatch.setattr(topic_prompt_module, "_call_ai_with_litellm", _fake_call_ai)
    monkeypatch.setattr(
        topic_prompt_module,
        "resolve_rows_entity_matches",
        lambda _engine, _rows_by_post_uid: {"weibo:1": []},
    )
    monkeypatch.setattr(topic_prompt_module, "score_from_assertions", lambda _rows: 0.7)
    monkeypatch.setattr(
        topic_prompt_module,
        "write_assertions_and_mark_done",
        lambda *_args, **_kwargs: writes.append("write"),
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "mark_entity_page_dirty_from_assertions",
        lambda *_args, **_kwargs: dirty_marks.append("dirty"),
    )

    ok = topic_prompt_module.process_one_post_uid_topic_prompt_v4(
        engine=cast(Any, object()),
        post_uid="weibo:1",
        config=_build_config(),
        limiter=limiter,
    )

    assert ok is True
    assert callable(seen_request_gate["value"])
    assert wait_calls == ["wait"]
    assert writes == ["write"]
    assert dirty_marks == ["dirty"]


def test_process_one_post_uid_topic_prompt_v4_passes_prefetched_post_to_final_write(
    monkeypatch,
) -> None:
    limiter = RateLimiter(rpm=0.0)
    writes: list[dict[str, object]] = []
    prefetched_post = CloudPost(
        post_uid="xueqiu:1",
        platform="xueqiu",
        platform_post_id="xueqiu:1",
        author="泽元投资",
        created_at="2026-03-31 17:39:52+08:00",
        url="https://xueqiu.com/5992135535/381907747",
        raw_text="泽元投资：[献花花][献花花]",
        ai_retry_count=1,
    )

    def _fake_call_ai(**_kwargs):  # type: ignore[no-untyped-def]
        return {"assertions": [], "mentions": []}

    def _fake_write(*_args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        writes.append(dict(kwargs))

    monkeypatch.setattr(topic_prompt_module, "_call_ai_with_litellm", _fake_call_ai)
    monkeypatch.setattr(
        topic_prompt_module,
        "resolve_rows_entity_matches",
        lambda _engine, _rows_by_post_uid: {"xueqiu:1": []},
    )
    monkeypatch.setattr(topic_prompt_module, "score_from_assertions", lambda _rows: 0.0)
    monkeypatch.setattr(
        topic_prompt_module,
        "write_assertions_and_mark_done",
        _fake_write,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "mark_entity_page_dirty_from_assertions",
        lambda *_args, **_kwargs: None,
    )

    topic_prompt_module.process_one_post_uid_topic_prompt_v4(
        engine=cast(Any, object()),
        post_uid="xueqiu:1",
        config=_build_config(),
        limiter=limiter,
        prefetched_post=prefetched_post,
    )

    assert len(writes) == 1
    assert writes[0]["post_uid"] == "xueqiu:1"
    assert writes[0]["prefetched_post"] == prefetched_post


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
    assert row["assertion_entities"] == [
        {
            "entity_key": "commodity:黄金",
            "entity_type": "commodity",
            "match_source": "commodity_name",
            "is_primary": 1,
        },
        {
            "entity_key": "keyword:汇率",
            "entity_type": "keyword",
            "match_source": "keyword",
            "is_primary": 0,
        },
    ]


def test_resolve_rows_entity_matches_overwrites_entities_and_persists_candidates(
    monkeypatch,
    pg_conn,
) -> None:
    from alphavault.worker import post_processor_topic_prompt_v4 as worker_module

    conn = _workbench_conn(pg_conn)
    try:
        monkeypatch.setattr(
            worker_module,
            "get_research_workbench_engine_from_env",
            lambda: conn,
            raising=False,
        )
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
                                "match_source": "stock_alias",
                                "is_primary": 1,
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
                "match_source": "stock_code",
                "is_primary": 1,
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

    standard_engine = cast(Any, object())
    load_calls: list[tuple[list[str], list[str]]] = []
    resolve_calls: list[tuple[dict[str, str] | None, dict[str, str] | None]] = []

    monkeypatch.setattr(
        worker_module,
        "get_research_workbench_engine_from_env",
        lambda: standard_engine,
        raising=False,
    )

    def _fake_load_entity_match_lookup_maps(
        engine_or_conn,
        *,
        stock_name_texts: list[str],
        stock_alias_texts: list[str],
    ) -> tuple[dict[str, str], dict[str, str]]:
        assert engine_or_conn is standard_engine
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
        alias_task_sample=None,
    ) -> EntityMatchResult:
        del assertion_mentions
        del alias_task_sample
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


def test_resolve_rows_entity_matches_uses_standard_engine_for_lookup_prefetch(
    monkeypatch,
) -> None:
    from alphavault.worker import post_processor_topic_prompt_v4 as worker_module

    source_engine = cast(Any, object())
    standard_engine = cast(Any, object())
    load_calls: list[tuple[list[str], list[str]]] = []
    resolve_calls: list[str] = []

    monkeypatch.setattr(
        worker_module,
        "get_research_workbench_engine_from_env",
        lambda: standard_engine,
        raising=False,
    )

    def _fake_load_entity_match_lookup_maps(
        engine_or_conn,
        *,
        stock_name_texts: list[str],
        stock_alias_texts: list[str],
    ) -> tuple[dict[str, str], dict[str, str]]:
        assert engine_or_conn is standard_engine
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
        engine_or_conn,
        *,
        assertion_mentions,
        stock_name_targets=None,
        stock_alias_targets=None,
        alias_task_sample=None,
    ) -> EntityMatchResult:
        del assertion_mentions
        del alias_task_sample
        assert engine_or_conn is source_engine
        resolve_calls.append("resolve")
        assert stock_name_targets == {"贵州茅台": "stock:600519.SH"}
        assert stock_alias_targets == {"茅台": "stock:600519.SH"}
        return EntityMatchResult([], [], [])

    monkeypatch.setattr(worker_module, "resolve_assertion_mentions", _fake_resolve)

    rows_by_post_uid: dict[str, list[dict[str, object]]] = {
        "weibo:1": [
            {
                "assertion_mentions": [
                    {"mention_text": "贵州茅台", "mention_type": "stock_name"},
                    {"mention_text": "茅台", "mention_type": "stock_alias"},
                ]
            }
        ]
    }

    followups_by_post_uid = resolve_rows_entity_matches(source_engine, rows_by_post_uid)

    assert load_calls == [(["贵州茅台"], ["茅台"])]
    assert resolve_calls == ["resolve"]
    assert followups_by_post_uid == {"weibo:1": []}


def test_resolve_rows_entity_matches_skips_standard_lookup_without_stock_mentions(
    monkeypatch,
) -> None:
    from alphavault.worker import post_processor_topic_prompt_v4 as worker_module

    source_engine = cast(Any, object())
    resolve_calls: list[str] = []

    def _fail_get_standard_engine():
        raise AssertionError("should_not_load_standard_engine")

    def _fail_load_entity_match_lookup_maps(
        _engine_or_conn,
        *,
        stock_name_texts: list[str],
        stock_alias_texts: list[str],
    ) -> tuple[dict[str, str], dict[str, str]]:
        del stock_name_texts
        del stock_alias_texts
        raise AssertionError("should_not_prefetch_lookup_maps")

    monkeypatch.setattr(
        worker_module,
        "get_research_workbench_engine_from_env",
        _fail_get_standard_engine,
        raising=False,
    )
    monkeypatch.setattr(
        worker_module,
        "load_entity_match_lookup_maps",
        _fail_load_entity_match_lookup_maps,
        raising=False,
    )

    def _fake_resolve(
        engine_or_conn,
        *,
        assertion_mentions,
        stock_name_targets=None,
        stock_alias_targets=None,
        alias_task_sample=None,
    ) -> EntityMatchResult:
        del assertion_mentions
        del alias_task_sample
        assert engine_or_conn is source_engine
        assert stock_name_targets == {}
        assert stock_alias_targets == {}
        resolve_calls.append("resolve")
        return EntityMatchResult([], [], [])

    monkeypatch.setattr(worker_module, "resolve_assertion_mentions", _fake_resolve)

    rows_by_post_uid: dict[str, list[dict[str, object]]] = {
        "weibo:1": [
            {
                "assertion_mentions": [
                    {"mention_text": "消费电子", "mention_type": "topic"},
                ]
            }
        ]
    }

    followups_by_post_uid = resolve_rows_entity_matches(source_engine, rows_by_post_uid)

    assert resolve_calls == ["resolve"]
    assert followups_by_post_uid == {"weibo:1": []}


def test_resolve_rows_entity_matches_attaches_alias_task_sample_context(
    monkeypatch,
    pg_conn,
) -> None:
    from alphavault.worker import post_processor_topic_prompt_v4 as worker_module

    conn = _workbench_conn(pg_conn)
    try:
        monkeypatch.setattr(
            worker_module,
            "get_research_workbench_engine_from_env",
            lambda: conn,
            raising=False,
        )
        rows_by_post_uid: dict[str, list[dict[str, object]]] = {
            "weibo:7": [
                {
                    "evidence": "长电今天继续走强",
                    "source_text_excerpt": "原文里说长电科技和封测景气度继续上行。",
                    "assertion_mentions": cast(
                        list[dict[str, Any]],
                        [
                            {
                                "mention_text": "长电",
                                "mention_type": "stock_alias",
                                "confidence": 0.8,
                            }
                        ],
                    ),
                    "assertion_entities": [],
                }
            ]
        }

        followups_by_post_uid = resolve_rows_entity_matches(conn, rows_by_post_uid)

        followups = followups_by_post_uid["weibo:7"]
        assert len(followups) == 1
        assert followups[0].alias_task_keys == ["stock:长电"]
        assert followups[0].alias_task_samples == [
            {
                "alias_key": "stock:长电",
                "sample_post_uid": "weibo:7",
                "sample_evidence": "长电今天继续走强",
                "sample_raw_text_excerpt": "原文里说长电科技和封测景气度继续上行。",
            }
        ]
    finally:
        conn.close()
