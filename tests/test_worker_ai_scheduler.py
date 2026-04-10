from __future__ import annotations

import threading
from concurrent.futures import Future
from types import SimpleNamespace

from alphavault.worker import ai_processor as ai_processor_module
from alphavault.worker import runtime_cache as runtime_cache_module
from alphavault.worker import scheduler as scheduler_module
from alphavault.worker import worker_constants as worker_constants_module
from alphavault.worker.runtime_models import LLMConfig
from alphavault.worker.topic_prompt_v4 import build_topic_prompt_v4_llm_log_line


_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _config() -> LLMConfig:
    return LLMConfig(
        api_key="k",
        model="m",
        prompt_version="p",
        relevant_threshold=0.3,
        base_url="",
        api_mode="responses",
        ai_stream=True,
        ai_retries=3,
        ai_temperature=0.1,
        ai_reasoning_effort="low",
        ai_rpm=12.0,
        ai_timeout_seconds=30.0,
        trace_out=None,
        verbose=False,
    )


def test_compute_low_priority_budget_zero_when_rss_is_full() -> None:
    assert (
        scheduler_module.compute_low_priority_budget(ai_cap=6, rss_inflight_now=6) == 0
    )
    assert (
        scheduler_module.compute_low_priority_budget(ai_cap=6, rss_inflight_now=9) == 0
    )


def test_compute_low_priority_budget_uses_remaining_slots() -> None:
    assert (
        scheduler_module.compute_low_priority_budget(ai_cap=6, rss_inflight_now=2) == 4
    )


def test_scheduler_does_not_keep_backfill_budget_api() -> None:
    assert not hasattr(scheduler_module, "BACKFILL_MAX_STOCKS_PER_RUN_CAP")
    assert not hasattr(scheduler_module, "compute_backfill_max_stocks_per_run")
    assert not hasattr(
        worker_constants_module,
        "BACKFILL_CACHE_FALLBACK_INTERVAL_SECONDS",
    )


def test_compute_rss_available_slots_subtracts_low_inflight() -> None:
    assert (
        scheduler_module.compute_rss_available_slots(
            ai_cap=6,
            rss_inflight_now=1,
            low_inflight_now=2,
        )
        == 3
    )


def test_compute_rss_available_slots_never_negative() -> None:
    assert (
        scheduler_module.compute_rss_available_slots(
            ai_cap=6,
            rss_inflight_now=6,
            low_inflight_now=1,
        )
        == 0
    )


def test_should_continue_turns_false_when_rss_becomes_busy() -> None:
    state = {"rss_inflight_now": 1}
    should_continue = scheduler_module.build_low_priority_should_continue(
        ai_cap=4,
        rss_inflight_now_get=lambda: int(state["rss_inflight_now"]),
    )

    assert should_continue() is True
    state["rss_inflight_now"] = 4
    assert should_continue() is False


def test_should_continue_turns_false_when_rss_due_and_no_available_slots() -> None:
    state = {"rss_inflight_now": 0, "low_inflight_now": 4, "rss_due_now": True}
    should_continue = scheduler_module.build_low_priority_should_continue(
        ai_cap=4,
        rss_inflight_now_get=lambda: int(state["rss_inflight_now"]),
        low_inflight_now_get=lambda: int(state["low_inflight_now"]),
        has_due_ai_pending_get=lambda: bool(state["rss_due_now"]),
    )

    assert should_continue() is False


def test_memoize_bool_with_ttl_reuses_recent_result(monkeypatch) -> None:
    calls: list[str] = []
    now_state = {"now": 100.0}

    monkeypatch.setattr(
        runtime_cache_module.time,
        "time",
        lambda: float(now_state["now"]),
    )

    def _resolve() -> bool:
        calls.append("call")
        return True

    cached = runtime_cache_module.memoize_bool_with_ttl(
        resolver=_resolve,
        ttl_seconds=1.0,
    )
    assert cached() is True
    assert cached() is True
    assert calls == ["call"]

    now_state["now"] = 101.2
    assert cached() is True
    assert calls == ["call", "call"]


def test_build_topic_prompt_v4_llm_log_line_call_contains_id_and_author() -> None:
    line = build_topic_prompt_v4_llm_log_line(
        event="call",
        root_key="root:123",
        post_uid="weibo:1",
        author="博主A",
        locked_count=0,
    )

    assert "[llm] call topic_prompt_v4" in line
    assert "post_uid=weibo:1" in line
    assert "author=博主A" in line


def test_build_topic_prompt_v4_llm_log_line_done_contains_cost() -> None:
    line = build_topic_prompt_v4_llm_log_line(
        event="done",
        root_key="root:123",
        post_uid="weibo:1",
        author="博主A",
        locked_count=3,
        cost_seconds=12.3,
    )

    assert "[llm] done topic_prompt_v4" in line
    assert "post_uid=weibo:1" in line
    assert "author=博主A" in line
    assert "locked=3" in line
    assert "cost=12.3s" in line


def test_schedule_ai_prefers_redis_stream_when_available() -> None:
    called: dict[str, object] = {"ok": False}

    def _schedule_from_stream(**kwargs):  # type: ignore[no-untyped-def]
        called["ok"] = True
        called["consumer_name"] = kwargs.get("consumer_name")
        return 1, False

    inflight_futures: set[Future] = set()
    inflight_owner_by_future: dict[Future, str] = {}
    scheduled, has_error = scheduler_module.schedule_ai(
        executor=object(),
        engine=object(),
        platform="weibo",
        ai_cap=4,
        low_inflight_now_get=lambda: 0,
        inflight_futures=inflight_futures,
        inflight_owner_by_future=inflight_owner_by_future,
        inflight_owner="weibo",
        consumer_name="weibo:worker",
        wakeup_event=threading.Event(),
        config=_config(),
        limiter=object(),
        verbose=False,
        redis_client=object(),
        redis_queue_key="queue",
        schedule_ai_from_stream_fn=_schedule_from_stream,
    )

    assert has_error is False
    assert scheduled == 1
    assert called["ok"] is True
    assert called["consumer_name"] == "weibo:worker"


def test_schedule_ai_requires_redis_queue() -> None:
    inflight_futures: set[Future] = set()
    inflight_owner_by_future: dict[Future, str] = {}

    scheduled, has_error = scheduler_module.schedule_ai(
        executor=object(),
        engine=object(),
        platform="weibo",
        ai_cap=4,
        low_inflight_now_get=lambda: 0,
        inflight_futures=inflight_futures,
        inflight_owner_by_future=inflight_owner_by_future,
        inflight_owner="weibo",
        consumer_name="weibo:worker",
        wakeup_event=threading.Event(),
        config=_config(),
        limiter=object(),
        verbose=False,
        redis_client=None,
        redis_queue_key="",
        schedule_ai_from_stream_fn=lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("missing redis should fail before scheduling")
        ),
    )

    assert scheduled == 0
    assert has_error is True


def test_process_one_redis_payload_passes_empty_prefetched_recent() -> None:
    ack_calls: list[tuple[str, str]] = []
    seen: dict[str, object] = {}

    def _process_one_post_uid(**kwargs) -> bool:  # type: ignore[no-untyped-def]
        seen["prefetched_recent"] = kwargs.get("prefetched_recent")
        return True

    ai_processor_module.process_one_redis_payload(
        engine=object(),
        payload={"post_uid": "weibo:1"},
        message_id="1-0",
        redis_client=object(),
        redis_queue_key="queue",
        config=_config(),
        limiter=object(),
        verbose=False,
        payload_to_cloud_post_fn=lambda _payload: SimpleNamespace(
            post_uid="weibo:1",
            platform="weibo",
            platform_post_id="1",
            author="作者A",
            created_at="2026-03-28 10:00:00",
            url="https://example.com/post/1",
            raw_text="正文",
            ai_retry_count=1,
        ),
        process_one_post_uid_fn=_process_one_post_uid,
        mark_post_failed_fn=lambda **_kwargs: None,
        redis_ai_push_retry_fn=lambda *_args, **_kwargs: None,
        redis_ai_ack_fn=lambda _client, queue_key, message_id: ack_calls.append(
            (str(queue_key), str(message_id))
        ),
        redis_ai_ack_and_push_retry_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("success path should not use retry handoff")
        ),
        redis_ai_ack_and_clear_dedup_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("success path should not clear dedup")
        ),
        payload_retry_count_fn=lambda _payload: 0,
        backoff_seconds_fn=lambda _count: 1,
        now_epoch_fn=lambda: 100,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        max_retry_count=3,
    )

    assert seen["prefetched_recent"] == []
    assert ack_calls == [("queue", "1-0")]


def test_process_one_redis_payload_skips_ai_when_post_already_processed(capsys) -> None:
    ack_calls: list[tuple[str, str]] = []
    guard_calls: list[str] = []

    def _is_post_already_processed_success(
        _engine: object,
        *,
        post_uid: str,
    ) -> bool:
        guard_calls.append(str(post_uid))
        return True

    ai_processor_module.process_one_redis_payload(
        engine=object(),
        payload={"post_uid": "weibo:done"},
        message_id="done-1",
        redis_client=object(),
        redis_queue_key="queue",
        config=_config(),
        limiter=object(),
        verbose=True,
        payload_to_cloud_post_fn=lambda _payload: SimpleNamespace(
            post_uid="weibo:done",
            platform="weibo",
            platform_post_id="done",
            author="作者Done",
            created_at="2026-03-28 10:00:00",
            url="https://example.com/post/done",
            raw_text="正文",
            ai_retry_count=1,
        ),
        process_one_post_uid_fn=lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("already processed path should not call AI")
        ),
        mark_post_failed_fn=lambda **_kwargs: None,
        redis_ai_push_retry_fn=lambda *_args, **_kwargs: None,
        redis_ai_ack_fn=lambda _client, queue_key, message_id: ack_calls.append(
            (str(queue_key), str(message_id))
        ),
        redis_ai_ack_and_push_retry_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("already processed path should not use retry handoff")
        ),
        redis_ai_ack_and_clear_dedup_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("already processed path should not clear dedup")
        ),
        payload_retry_count_fn=lambda _payload: 0,
        backoff_seconds_fn=lambda _count: 1,
        now_epoch_fn=lambda: 100,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        max_retry_count=3,
        is_post_already_processed_success_fn=_is_post_already_processed_success,
    )

    assert guard_calls == ["weibo:done"]
    assert ack_calls == [("queue", "done-1")]
    captured = capsys.readouterr()
    assert "[ai] skip_db_already_processed_success" in captured.out
    assert "post_uid=weibo:done" in captured.out


def test_process_one_redis_payload_skip_db_guard_does_not_recheck_database() -> None:
    ack_calls: list[tuple[str, str]] = []
    process_calls: list[str] = []

    def _process_one_post_uid(**kwargs) -> bool:  # type: ignore[no-untyped-def]
        process_calls.append(str(kwargs.get("post_uid") or ""))
        return True

    ai_processor_module.process_one_redis_payload(
        engine=object(),
        payload={"post_uid": "weibo:requeue", "skip_db_processed_guard": True},
        message_id="requeue-1",
        redis_client=object(),
        redis_queue_key="queue",
        config=_config(),
        limiter=object(),
        verbose=False,
        payload_to_cloud_post_fn=lambda _payload: SimpleNamespace(
            post_uid="weibo:requeue",
            platform="weibo",
            platform_post_id="requeue",
            author="作者Requeue",
            created_at="2026-03-28 10:00:00",
            url="https://example.com/post/requeue",
            raw_text="正文",
            ai_retry_count=1,
        ),
        process_one_post_uid_fn=_process_one_post_uid,
        mark_post_failed_fn=lambda **_kwargs: None,
        redis_ai_push_retry_fn=lambda *_args, **_kwargs: None,
        redis_ai_ack_fn=lambda _client, queue_key, message_id: ack_calls.append(
            (str(queue_key), str(message_id))
        ),
        redis_ai_ack_and_push_retry_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("success path should not use retry handoff")
        ),
        redis_ai_ack_and_clear_dedup_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("success path should not clear dedup")
        ),
        payload_retry_count_fn=lambda _payload: 0,
        backoff_seconds_fn=lambda _count: 1,
        now_epoch_fn=lambda: 100,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        max_retry_count=3,
        is_post_already_processed_success_fn=lambda _engine, *, post_uid: (
            _ for _ in ()
        ).throw(
            AssertionError(
                f"skip-db-guard path should not recheck database: {post_uid}"
            )
        ),
    )

    assert process_calls == ["weibo:requeue"]
    assert ack_calls == [("queue", "requeue-1")]


def test_process_one_redis_payload_acks_when_payload_cannot_build_cloud_post() -> None:
    ack_calls: list[str] = []

    ai_processor_module.process_one_redis_payload(
        engine=object(),
        payload={"post_uid": ""},
        message_id="1-0",
        redis_client=object(),
        redis_queue_key="queue",
        config=_config(),
        limiter=object(),
        verbose=False,
        payload_to_cloud_post_fn=lambda _payload: None,
        process_one_post_uid_fn=lambda **_kwargs: True,
        mark_post_failed_fn=lambda **_kwargs: None,
        redis_ai_push_retry_fn=lambda *_args, **_kwargs: None,
        redis_ai_ack_fn=lambda _client, _key, message_id: ack_calls.append(
            str(message_id)
        ),
        redis_ai_ack_and_push_retry_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("invalid payload path should not use retry handoff")
        ),
        redis_ai_ack_and_clear_dedup_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("invalid payload path should not clear dedup")
        ),
        payload_retry_count_fn=lambda _payload: 0,
        backoff_seconds_fn=lambda _count: 1,
        now_epoch_fn=lambda: 100,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        max_retry_count=3,
    )

    assert ack_calls == ["1-0"]


def test_process_one_redis_payload_pushes_retry_before_limit() -> None:
    retry_handoff_calls: list[tuple[dict[str, object], int, str]] = []
    failed_calls: list[dict[str, object]] = []

    ai_processor_module.process_one_redis_payload(
        engine=object(),
        payload={"post_uid": "weibo:retry"},
        message_id="2-0",
        redis_client=object(),
        redis_queue_key="queue",
        config=_config(),
        limiter=object(),
        verbose=False,
        payload_to_cloud_post_fn=lambda _payload: SimpleNamespace(
            post_uid="weibo:retry",
            platform="weibo",
            platform_post_id="2",
            author="作者B",
            created_at="2026-03-28 10:00:00",
            url="https://example.com/post/2",
            raw_text="正文",
            ai_retry_count=1,
        ),
        process_one_post_uid_fn=lambda **_kwargs: False,
        mark_post_failed_fn=lambda **kwargs: failed_calls.append(kwargs),
        redis_ai_push_retry_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("retry branch should use atomic handoff helper")
        ),
        redis_ai_ack_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("retry branch should not ack separately")
        ),
        redis_ai_ack_and_clear_dedup_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("retry branch should not clear dedup")
        ),
        redis_ai_ack_and_push_retry_fn=lambda _client,
        _queue_key,
        *,
        message_id,
        payload,
        next_retry_at: retry_handoff_calls.append(
            (dict(payload), int(next_retry_at), str(message_id))
        ),
        payload_retry_count_fn=lambda _payload: 0,
        backoff_seconds_fn=lambda _count: 30,
        now_epoch_fn=lambda: 100,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        max_retry_count=3,
    )

    assert failed_calls == []
    assert retry_handoff_calls == [
        (
            {
                "post_uid": "weibo:retry",
                "retry_count": 1,
                "next_retry_at": 130,
            },
            130,
            "2-0",
        )
    ]


def test_process_one_redis_payload_marks_failed_at_retry_limit() -> None:
    retry_calls: list[tuple[dict[str, object], int]] = []
    final_ack_calls: list[tuple[str, str]] = []
    failed_calls: list[dict[str, object]] = []

    def _payload_retry_count(payload: dict[str, object]) -> int:
        return int(str(payload.get("retry_count") or 0))

    ai_processor_module.process_one_redis_payload(
        engine=object(),
        payload={"post_uid": "weibo:failed", "retry_count": 3},
        message_id="3-0",
        redis_client=object(),
        redis_queue_key="queue",
        config=_config(),
        limiter=object(),
        verbose=False,
        payload_to_cloud_post_fn=lambda _payload: SimpleNamespace(
            post_uid="weibo:failed",
            platform="weibo",
            platform_post_id="3",
            author="作者C",
            created_at="2026-03-28 10:00:00",
            url="https://example.com/post/3",
            raw_text="正文",
            ai_retry_count=3,
        ),
        process_one_post_uid_fn=lambda **_kwargs: False,
        mark_post_failed_fn=lambda **kwargs: failed_calls.append(kwargs),
        redis_ai_push_retry_fn=lambda _client,
        _queue_key,
        *,
        payload,
        next_retry_at: retry_calls.append((dict(payload), int(next_retry_at))),
        redis_ai_ack_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("retry limit path should use final ack helper")
        ),
        redis_ai_ack_and_push_retry_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("retry limit path should not use retry handoff")
        ),
        redis_ai_ack_and_clear_dedup_fn=lambda _client,
        _queue_key,
        *,
        message_id,
        post_uid: final_ack_calls.append((str(message_id), str(post_uid))),
        payload_retry_count_fn=_payload_retry_count,
        backoff_seconds_fn=lambda _count: 30,
        now_epoch_fn=lambda: 100,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        max_retry_count=3,
    )

    assert retry_calls == []
    assert final_ack_calls == [("3-0", "weibo:failed")]
    assert len(failed_calls) == 1
    assert failed_calls[0]["post_uid"] == "weibo:failed"
    assert failed_calls[0]["model"] == "m"
    assert failed_calls[0]["prompt_version"] == "p"


def test_process_one_redis_payload_does_not_ack_when_retry_enqueue_errors() -> None:
    ack_calls: list[str] = []
    failed_calls: list[dict[str, object]] = []

    def _raise_retry(*_args, **_kwargs) -> None:
        raise RuntimeError("redis unavailable")

    ai_processor_module.process_one_redis_payload(
        engine=object(),
        payload={"post_uid": "weibo:retry-error"},
        message_id="4-0",
        redis_client=object(),
        redis_queue_key="queue",
        config=_config(),
        limiter=object(),
        verbose=False,
        payload_to_cloud_post_fn=lambda _payload: SimpleNamespace(
            post_uid="weibo:retry-error",
            platform="weibo",
            platform_post_id="4",
            author="作者D",
            created_at="2026-03-28 10:00:00",
            url="https://example.com/post/4",
            raw_text="正文",
            ai_retry_count=1,
        ),
        process_one_post_uid_fn=lambda **_kwargs: False,
        mark_post_failed_fn=lambda **kwargs: failed_calls.append(kwargs),
        redis_ai_push_retry_fn=_raise_retry,
        redis_ai_ack_fn=lambda _client, _key, message_id: ack_calls.append(
            str(message_id)
        ),
        redis_ai_ack_and_push_retry_fn=_raise_retry,
        redis_ai_ack_and_clear_dedup_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("retry handoff error path should not clear dedup")
        ),
        payload_retry_count_fn=lambda _payload: 0,
        backoff_seconds_fn=lambda _count: 30,
        now_epoch_fn=lambda: 100,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        max_retry_count=3,
    )

    assert ack_calls == []
    assert failed_calls == []
