from __future__ import annotations
from typing import Any, cast

from alphavault.constants import SCHEMA_STANDARD, SCHEMA_WEIBO
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


def _source_conn(pg_conn) -> PostgresConnection:
    apply_cloud_schema(pg_conn, target="source", schema_name=SCHEMA_WEIBO)
    return PostgresConnection(pg_conn, schema_name=SCHEMA_WEIBO)


def _insert_source_post(
    conn: PostgresConnection,
    *,
    post_uid: str = "weibo:1",
    platform_post_id: str = "1001",
) -> None:
    conn.execute(
        """
INSERT INTO weibo.posts(
  post_uid, platform, platform_post_id, author, created_at, url, raw_text,
  final_status, processed_at, model, prompt_version, archived_at, ingested_at
)
VALUES (
  :post_uid, 'weibo', :platform_post_id, '老王', '2026-04-09 10:00:00',
  'https://example.com/post/1001', '茅台我先看看',
  'relevant', '2026-04-09 10:05:00', 'old-model', 'old-prompt',
  '2026-04-09 10:06:00', 1
)
""",
        {"post_uid": post_uid, "platform_post_id": platform_post_id},
    )


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
    monkeypatch.setattr(
        topic_prompt_module,
        "load_latest_pending_feedback",
        lambda _engine, *, post_uid: None,
        raising=False,
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
        "load_latest_pending_feedback",
        lambda _engine, *, post_uid: None,
        raising=False,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "resolve_rows_entity_matches",
        lambda _engine, _rows_by_post_uid: {"xueqiu:1": []},
    )
    monkeypatch.setattr(topic_prompt_module, "score_from_assertions", lambda _rows: 0.0)
    monkeypatch.setattr(
        topic_prompt_module,
        "run_postgres_transaction",
        lambda engine_or_conn, fn: fn(engine_or_conn),
        raising=False,
    )
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


def test_process_one_post_uid_topic_prompt_v4_passes_manual_feedback_hint_to_prompt_builder(
    monkeypatch,
) -> None:
    limiter = RateLimiter(rpm=0.0)
    seen_manual_feedback_hint: dict[str, object] = {}

    monkeypatch.setattr(
        topic_prompt_module,
        "load_cloud_post",
        lambda _engine, _post_uid: CloudPost(
            post_uid="weibo:1",
            platform="weibo",
            platform_post_id="1001",
            author="老王",
            created_at="2026-04-09 10:00:00",
            url="https://example.com/post/1001",
            raw_text="茅台我先看看",
            ai_retry_count=0,
        ),
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "thread_root_info_for_post",
        lambda **_kwargs: ("root:1001", "", ""),
    )

    def _fake_build_prompt(**kwargs):  # type: ignore[no-untyped-def]
        seen_manual_feedback_hint["value"] = kwargs.get("manual_feedback_hint")
        return (
            {"message_lookup": {("status", "1001"): {"text": "茅台我先看看"}}},
            0,
            "prompt",
            10,
            10,
            False,
            True,
        )

    monkeypatch.setattr(
        topic_prompt_module,
        "build_topic_prompt_v4_with_prompt_chars_limit",
        _fake_build_prompt,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "load_latest_pending_feedback",
        lambda _engine, *, post_uid: {
            "feedback_id": "fb-1",
            "post_uid": post_uid,
            "feedback_tag": "动作错了",
            "feedback_note": "原文是先看看，不是直接买入",
            "feedback_status": "pending",
            "entrypoint": "stock_research",
            "submitted_at": "2026-04-09 11:00:00",
            "applied_at": "",
        },
        raising=False,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "_call_ai_with_litellm",
        lambda **_kwargs: {"assertions": [], "mentions": []},
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "resolve_rows_entity_matches",
        lambda _engine, _rows_by_post_uid: {"weibo:1": []},
    )
    monkeypatch.setattr(topic_prompt_module, "score_from_assertions", lambda _rows: 0.0)
    monkeypatch.setattr(
        topic_prompt_module,
        "run_postgres_transaction",
        lambda engine_or_conn, fn: fn(engine_or_conn),
        raising=False,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "write_assertions_and_mark_done",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "mark_entity_page_dirty_from_assertions",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "mark_feedback_applied",
        lambda *_args, **_kwargs: 1,
        raising=False,
    )

    ok = topic_prompt_module.process_one_post_uid_topic_prompt_v4(
        engine=cast(Any, object()),
        post_uid="weibo:1",
        config=_build_config(),
        limiter=limiter,
    )

    assert ok is True
    assert seen_manual_feedback_hint["value"] == {
        "feedback_tag": "动作错了",
        "feedback_note": "原文是先看看，不是直接买入",
        "submitted_at": "2026-04-09 11:00:00",
    }


def test_process_one_post_uid_topic_prompt_v4_marks_feedback_applied_after_success(
    monkeypatch,
) -> None:
    limiter = RateLimiter(rpm=0.0)
    applied_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        topic_prompt_module,
        "load_cloud_post",
        lambda _engine, _post_uid: CloudPost(
            post_uid="weibo:1",
            platform="weibo",
            platform_post_id="1001",
            author="老王",
            created_at="2026-04-09 10:00:00",
            url="https://example.com/post/1001",
            raw_text="茅台我先看看",
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
            {"message_lookup": {("status", "1001"): {"text": "茅台我先看看"}}},
            0,
            "prompt",
            10,
            10,
            False,
            True,
        ),
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "load_latest_pending_feedback",
        lambda _engine, *, post_uid: {
            "feedback_id": "fb-1",
            "post_uid": post_uid,
            "feedback_tag": "动作错了",
            "feedback_note": "原文是先看看，不是直接买入",
            "feedback_status": "pending",
            "entrypoint": "stock_research",
            "submitted_at": "2026-04-09 11:00:00",
            "applied_at": "",
        },
        raising=False,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "_call_ai_with_litellm",
        lambda **_kwargs: {"assertions": [], "mentions": []},
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "resolve_rows_entity_matches",
        lambda _engine, _rows_by_post_uid: {"weibo:1": []},
    )
    monkeypatch.setattr(topic_prompt_module, "score_from_assertions", lambda _rows: 0.0)
    monkeypatch.setattr(topic_prompt_module, "now_str", lambda: "2026-04-09 11:05:00")
    monkeypatch.setattr(
        topic_prompt_module,
        "run_postgres_transaction",
        lambda engine_or_conn, fn: fn(engine_or_conn),
        raising=False,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "write_assertions_and_mark_done",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "mark_entity_page_dirty_from_assertions",
        lambda *_args, **_kwargs: None,
    )

    def _fake_mark_feedback_applied(_engine, **kwargs) -> int:  # type: ignore[no-untyped-def]
        applied_calls.append(dict(kwargs))
        return 1

    monkeypatch.setattr(
        topic_prompt_module,
        "mark_feedback_applied",
        _fake_mark_feedback_applied,
        raising=False,
    )

    ok = topic_prompt_module.process_one_post_uid_topic_prompt_v4(
        engine=cast(Any, object()),
        post_uid="weibo:1",
        config=_build_config(),
        limiter=limiter,
    )

    assert ok is True
    assert applied_calls == [
        {
            "feedback_id": "fb-1",
            "applied_at": "2026-04-09 11:05:00",
        }
    ]


def test_process_one_post_uid_topic_prompt_v4_wraps_done_write_and_feedback_apply_in_one_transaction(
    monkeypatch,
) -> None:
    limiter = RateLimiter(rpm=0.0)
    engine = cast(Any, object())
    transaction_conn = cast(Any, object())
    transaction_calls: list[object] = []
    write_engines: list[object] = []
    apply_engines: list[object] = []

    monkeypatch.setattr(
        topic_prompt_module,
        "load_cloud_post",
        lambda _engine, _post_uid: CloudPost(
            post_uid="weibo:1",
            platform="weibo",
            platform_post_id="1001",
            author="老王",
            created_at="2026-04-09 10:00:00",
            url="https://example.com/post/1001",
            raw_text="茅台我先看看",
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
            {"message_lookup": {("status", "1001"): {"text": "茅台我先看看"}}},
            0,
            "prompt",
            10,
            10,
            False,
            True,
        ),
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "load_latest_pending_feedback",
        lambda _engine, *, post_uid: {
            "feedback_id": "fb-1",
            "post_uid": post_uid,
            "feedback_tag": "动作错了",
            "feedback_note": "原文是先看看，不是直接买入",
            "feedback_status": "pending",
            "entrypoint": "stock_research",
            "submitted_at": "2026-04-09 11:00:00",
            "applied_at": "",
        },
        raising=False,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "_call_ai_with_litellm",
        lambda **_kwargs: {"assertions": [], "mentions": []},
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "resolve_rows_entity_matches",
        lambda _engine, _rows_by_post_uid: {"weibo:1": []},
    )
    monkeypatch.setattr(topic_prompt_module, "score_from_assertions", lambda _rows: 0.0)
    monkeypatch.setattr(topic_prompt_module, "now_str", lambda: "2026-04-09 11:05:00")

    def _fake_run_postgres_transaction(engine_or_conn, fn):  # type: ignore[no-untyped-def]
        transaction_calls.append(engine_or_conn)
        return fn(transaction_conn)

    def _fake_write_assertions_and_mark_done(engine_or_conn, **_kwargs) -> None:  # type: ignore[no-untyped-def]
        write_engines.append(engine_or_conn)

    def _fake_mark_feedback_applied(engine_or_conn, **_kwargs) -> int:  # type: ignore[no-untyped-def]
        apply_engines.append(engine_or_conn)
        return 1

    monkeypatch.setattr(
        topic_prompt_module,
        "run_postgres_transaction",
        _fake_run_postgres_transaction,
        raising=False,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "write_assertions_and_mark_done",
        _fake_write_assertions_and_mark_done,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "mark_feedback_applied",
        _fake_mark_feedback_applied,
        raising=False,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "mark_entity_page_dirty_from_assertions",
        lambda *_args, **_kwargs: None,
    )

    ok = topic_prompt_module.process_one_post_uid_topic_prompt_v4(
        engine=engine,
        post_uid="weibo:1",
        config=_build_config(),
        limiter=limiter,
    )

    assert ok is True
    assert transaction_calls == [engine]
    assert write_engines == [transaction_conn]
    assert apply_engines == [transaction_conn]


def test_process_one_post_uid_topic_prompt_v4_clips_manual_feedback_note_for_prompt(
    monkeypatch,
) -> None:
    limiter = RateLimiter(rpm=0.0)
    seen_manual_feedback_hint: dict[str, object] = {}
    long_note = "错" * 320

    monkeypatch.setattr(
        topic_prompt_module,
        "load_cloud_post",
        lambda _engine, _post_uid: CloudPost(
            post_uid="weibo:1",
            platform="weibo",
            platform_post_id="1001",
            author="老王",
            created_at="2026-04-09 10:00:00",
            url="https://example.com/post/1001",
            raw_text="茅台我先看看",
            ai_retry_count=0,
        ),
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "thread_root_info_for_post",
        lambda **_kwargs: ("root:1001", "", ""),
    )

    def _fake_build_prompt(**kwargs):  # type: ignore[no-untyped-def]
        seen_manual_feedback_hint["value"] = kwargs.get("manual_feedback_hint")
        return (
            {"message_lookup": {("status", "1001"): {"text": "茅台我先看看"}}},
            0,
            "prompt",
            10,
            10,
            False,
            True,
        )

    monkeypatch.setattr(
        topic_prompt_module,
        "build_topic_prompt_v4_with_prompt_chars_limit",
        _fake_build_prompt,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "load_latest_pending_feedback",
        lambda _engine, *, post_uid: {
            "feedback_id": "fb-1",
            "post_uid": post_uid,
            "feedback_tag": "摘要错了",
            "feedback_note": long_note,
            "feedback_status": "pending",
            "entrypoint": "stock_research",
            "submitted_at": "2026-04-09 11:00:00",
            "applied_at": "",
        },
        raising=False,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "_call_ai_with_litellm",
        lambda **_kwargs: {"assertions": [], "mentions": []},
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "resolve_rows_entity_matches",
        lambda _engine, _rows_by_post_uid: {"weibo:1": []},
    )
    monkeypatch.setattr(topic_prompt_module, "score_from_assertions", lambda _rows: 0.0)
    monkeypatch.setattr(
        topic_prompt_module,
        "run_postgres_transaction",
        lambda engine_or_conn, fn: fn(engine_or_conn),
        raising=False,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "write_assertions_and_mark_done",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "mark_entity_page_dirty_from_assertions",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "mark_feedback_applied",
        lambda *_args, **_kwargs: 1,
        raising=False,
    )

    ok = topic_prompt_module.process_one_post_uid_topic_prompt_v4(
        engine=cast(Any, object()),
        post_uid="weibo:1",
        config=_build_config(),
        limiter=limiter,
    )

    assert ok is True
    feedback_hint = seen_manual_feedback_hint["value"]
    assert isinstance(feedback_hint, dict)
    assert len(str(feedback_hint["feedback_note"])) == 300
    assert str(feedback_hint["feedback_note"]) == long_note[:300]


def test_process_one_post_uid_topic_prompt_v4_does_not_mark_feedback_applied_on_ai_error(
    monkeypatch,
) -> None:
    limiter = RateLimiter(rpm=0.0)
    applied_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        topic_prompt_module,
        "load_cloud_post",
        lambda _engine, _post_uid: CloudPost(
            post_uid="weibo:1",
            platform="weibo",
            platform_post_id="1001",
            author="老王",
            created_at="2026-04-09 10:00:00",
            url="https://example.com/post/1001",
            raw_text="茅台我先看看",
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
            {"message_lookup": {("status", "1001"): {"text": "茅台我先看看"}}},
            0,
            "prompt",
            10,
            10,
            False,
            True,
        ),
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "load_latest_pending_feedback",
        lambda _engine, *, post_uid: {
            "feedback_id": "fb-1",
            "post_uid": post_uid,
            "feedback_tag": "动作错了",
            "feedback_note": "原文是先看看，不是直接买入",
            "feedback_status": "pending",
            "entrypoint": "stock_research",
            "submitted_at": "2026-04-09 11:00:00",
            "applied_at": "",
        },
        raising=False,
    )

    def _fail_ai(**_kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("boom")

    monkeypatch.setattr(topic_prompt_module, "_call_ai_with_litellm", _fail_ai)

    def _fake_mark_feedback_applied(_engine, **kwargs) -> int:  # type: ignore[no-untyped-def]
        applied_calls.append(dict(kwargs))
        return 1

    monkeypatch.setattr(
        topic_prompt_module,
        "mark_feedback_applied",
        _fake_mark_feedback_applied,
        raising=False,
    )

    ok = topic_prompt_module.process_one_post_uid_topic_prompt_v4(
        engine=cast(Any, object()),
        post_uid="weibo:1",
        config=_build_config(),
        limiter=limiter,
    )

    assert ok is False
    assert applied_calls == []


def test_process_one_post_uid_topic_prompt_v4_rolls_back_done_write_when_feedback_apply_misses_row(
    monkeypatch,
    pg_conn,
) -> None:
    post_uid = "weibo:feedback-rollback"
    conn = _source_conn(pg_conn)
    _insert_source_post(
        conn,
        post_uid=post_uid,
        platform_post_id="feedback-rollback-1001",
    )
    limiter = RateLimiter(rpm=0.0)

    monkeypatch.setattr(
        topic_prompt_module,
        "thread_root_info_for_post",
        lambda **_kwargs: ("root:1001", "", ""),
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "build_topic_prompt_v4_with_prompt_chars_limit",
        lambda **_kwargs: (
            {"message_lookup": {("status", "1001"): {"text": "茅台我先看看"}}},
            0,
            "prompt",
            10,
            10,
            False,
            True,
        ),
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "load_latest_pending_feedback",
        lambda _engine, *, post_uid: {
            "feedback_id": "missing-feedback",
            "post_uid": post_uid,
            "feedback_tag": "动作错了",
            "feedback_note": "原文是先看看，不是直接买入",
            "feedback_status": "pending",
            "entrypoint": "stock_research",
            "submitted_at": "2026-04-09 11:00:00",
            "applied_at": "",
        },
        raising=False,
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "_call_ai_with_litellm",
        lambda **_kwargs: {"assertions": [], "mentions": []},
    )
    monkeypatch.setattr(
        topic_prompt_module,
        "resolve_rows_entity_matches",
        lambda _engine, _rows_by_post_uid: {"weibo:1": []},
    )
    monkeypatch.setattr(topic_prompt_module, "score_from_assertions", lambda _rows: 0.0)
    monkeypatch.setattr(topic_prompt_module, "now_str", lambda: "2026-04-09 11:05:00")
    monkeypatch.setattr(
        topic_prompt_module,
        "mark_entity_page_dirty_from_assertions",
        lambda *_args, **_kwargs: None,
    )

    ok = topic_prompt_module.process_one_post_uid_topic_prompt_v4(
        engine=cast(Any, conn),
        post_uid=post_uid,
        config=_build_config(),
        limiter=limiter,
    )

    row = (
        conn.execute(
            """
SELECT final_status, processed_at, model, prompt_version
FROM weibo.posts
WHERE post_uid = :post_uid
""",
            {"post_uid": post_uid},
        )
        .mappings()
        .fetchone()
    )

    assert ok is False
    assert row == {
        "final_status": "relevant",
        "processed_at": "2026-04-09 10:05:00",
        "model": "old-model",
        "prompt_version": "old-prompt",
    }


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
