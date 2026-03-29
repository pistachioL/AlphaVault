from __future__ import annotations

import json
import threading
from concurrent.futures import Future
from pathlib import Path

import pytest

from alphavault.rss.utils import RateLimiter
from alphavault.worker import worker as worker_module
from alphavault.worker.worker import (
    LLMConfig,
    _LowPriorityAISlotGate,
    _build_topic_prompt_v3_llm_log_line,
    _build_low_priority_should_continue,
    _mark_spool_flush_retry,
    _mark_spool_flush_started,
    _mark_spool_item_ingested,
    _request_spool_flush,
    _build_source_turso_error,
    _compute_backfill_max_stocks_per_run,
    _collect_rss_ingest_result,
    _compute_low_priority_budget,
    _compute_rss_available_slots,
    _schedule_ai,
    _should_start_spool_flush,
    _should_fast_retry_for_periodic_job,
    _should_wait_with_event,
    WorkerSourceConfig,
    WorkerSourceRuntime,
)


def test_compute_low_priority_budget_zero_when_rss_is_full() -> None:
    assert _compute_low_priority_budget(ai_cap=6, rss_inflight_now=6) == 0
    assert _compute_low_priority_budget(ai_cap=6, rss_inflight_now=9) == 0


def test_compute_low_priority_budget_uses_remaining_slots() -> None:
    assert _compute_low_priority_budget(ai_cap=6, rss_inflight_now=2) == 4


def test_compute_backfill_max_stocks_per_run_follows_low_budget() -> None:
    assert _compute_backfill_max_stocks_per_run(low_budget=0) == 1
    assert _compute_backfill_max_stocks_per_run(low_budget=3) == 3
    assert _compute_backfill_max_stocks_per_run(low_budget=100) == 32


def test_compute_rss_available_slots_subtracts_low_inflight() -> None:
    assert (
        _compute_rss_available_slots(
            ai_cap=6,
            rss_inflight_now=1,
            low_inflight_now=2,
        )
        == 3
    )


def test_compute_rss_available_slots_never_negative() -> None:
    assert (
        _compute_rss_available_slots(
            ai_cap=6,
            rss_inflight_now=6,
            low_inflight_now=1,
        )
        == 0
    )


def test_should_continue_turns_false_when_rss_becomes_busy() -> None:
    state = {"rss_inflight_now": 1}
    should_continue = _build_low_priority_should_continue(
        ai_cap=4,
        rss_inflight_now_get=lambda: int(state["rss_inflight_now"]),
    )

    assert should_continue() is True
    state["rss_inflight_now"] = 4
    assert should_continue() is False


def test_should_continue_turns_false_when_rss_due_and_no_available_slots() -> None:
    state = {"rss_inflight_now": 0, "low_inflight_now": 4, "rss_due_now": True}
    should_continue = _build_low_priority_should_continue(
        ai_cap=4,
        rss_inflight_now_get=lambda: int(state["rss_inflight_now"]),
        low_inflight_now_get=lambda: int(state["low_inflight_now"]),
        has_due_ai_pending_get=lambda: bool(state["rss_due_now"]),
    )

    assert should_continue() is False


def test_low_priority_slot_gate_respects_dynamic_cap() -> None:
    state = {"cap": 2}
    gate = _LowPriorityAISlotGate(cap_getter=lambda: int(state["cap"]))

    assert gate.try_acquire() is True
    assert gate.try_acquire() is True
    assert gate.try_acquire() is False
    assert gate.inflight() == 2

    gate.release()
    assert gate.inflight() == 1
    state["cap"] = 1
    assert gate.try_acquire() is False

    gate.release()
    assert gate.inflight() == 0
    assert gate.try_acquire() is True


def test_should_fast_retry_for_periodic_job_true_when_has_more() -> None:
    assert _should_fast_retry_for_periodic_job(has_more=True) is True


def test_should_fast_retry_for_periodic_job_false_when_no_more() -> None:
    assert _should_fast_retry_for_periodic_job(has_more=False) is False


def test_build_source_turso_error_excludes_ingest_enqueue_error() -> None:
    assert (
        _build_source_turso_error(
            maintenance_error=False,
            spool_flush_error=False,
            schedule_error=False,
            alias_sync_error=False,
            backfill_cache_error=False,
            relation_cache_error=False,
            stock_hot_error=False,
        )
        is False
    )


def test_build_source_turso_error_includes_spool_flush_error() -> None:
    assert (
        _build_source_turso_error(
            maintenance_error=False,
            spool_flush_error=True,
            schedule_error=False,
            alias_sync_error=False,
            backfill_cache_error=False,
            relation_cache_error=False,
            stock_hot_error=False,
        )
        is True
    )


def test_collect_rss_ingest_result_skips_pending_future() -> None:
    class _PendingFuture:
        def done(self) -> bool:
            return False

        def result(self) -> tuple[int, bool]:
            raise AssertionError("result should not be called for pending future")

    pending = _PendingFuture()
    next_future, inserted, finished, error = _collect_rss_ingest_result(
        source_name="weibo",
        future=pending,  # type: ignore[arg-type]
        engine=object(),  # type: ignore[arg-type]
        verbose=False,
    )

    assert next_future is pending
    assert inserted == 0
    assert finished is False
    assert error is False


def test_collect_rss_ingest_result_reads_finished_tuple() -> None:
    done_future: Future = Future()
    done_future.set_result((7, True))
    next_future, inserted, finished, error = _collect_rss_ingest_result(
        source_name="weibo",
        future=done_future,
        engine=object(),  # type: ignore[arg-type]
        verbose=False,
    )

    assert next_future is None
    assert inserted == 7
    assert finished is True
    assert error is True


def test_collect_rss_ingest_result_marks_nonfatal_exception_as_error() -> None:
    failed_future: Future = Future()
    failed_future.set_exception(RuntimeError("boom"))
    next_future, inserted, finished, error = _collect_rss_ingest_result(
        source_name="weibo",
        future=failed_future,
        engine=object(),  # type: ignore[arg-type]
        verbose=False,
    )

    assert next_future is None
    assert inserted == 0
    assert finished is True
    assert error is True


def test_collect_rss_ingest_result_reraises_fatal_exception() -> None:
    failed_future: Future = Future()
    failed_future.set_exception(KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        _collect_rss_ingest_result(
            source_name="weibo",
            future=failed_future,
            engine=object(),  # type: ignore[arg-type]
            verbose=False,
        )


def test_should_wait_with_event_true_when_only_rss_is_inflight() -> None:
    assert (
        _should_wait_with_event(
            ai_inflight=False,
            any_alias_inflight=False,
            any_backfill_inflight=False,
            any_relation_inflight=False,
            any_stock_hot_inflight=False,
            any_rss_inflight=True,
            any_spool_flush_inflight=False,
        )
        is True
    )


def test_should_wait_with_event_true_when_only_spool_flush_is_inflight() -> None:
    assert (
        _should_wait_with_event(
            ai_inflight=False,
            any_alias_inflight=False,
            any_backfill_inflight=False,
            any_relation_inflight=False,
            any_stock_hot_inflight=False,
            any_rss_inflight=False,
            any_spool_flush_inflight=True,
        )
        is True
    )


def test_should_wait_with_event_false_when_all_queues_idle() -> None:
    assert (
        _should_wait_with_event(
            ai_inflight=False,
            any_alias_inflight=False,
            any_backfill_inflight=False,
            any_relation_inflight=False,
            any_stock_hot_inflight=False,
            any_rss_inflight=False,
            any_spool_flush_inflight=False,
        )
        is False
    )


def test_build_topic_prompt_v3_llm_log_line_call_contains_id_and_author() -> None:
    line = _build_topic_prompt_v3_llm_log_line(
        event="call_api",
        root_key="src:abc",
        post_uid="weibo:1",
        author="博主A",
        locked_count=2,
    )

    assert "[llm] call_api topic_prompt_v3" in line
    assert "root_key=src:abc" in line
    assert "post_uid=weibo:1" in line
    assert "author=博主A" in line
    assert "locked=2" in line


def test_build_topic_prompt_v3_llm_log_line_done_contains_cost() -> None:
    line = _build_topic_prompt_v3_llm_log_line(
        event="done",
        root_key="src:abc",
        post_uid="weibo:1",
        author="博主A",
        locked_count=3,
        cost_seconds=12.34,
    )

    assert "[llm] done topic_prompt_v3" in line
    assert "post_uid=weibo:1" in line
    assert "author=博主A" in line
    assert "locked=3" in line
    assert "cost=12.3s" in line


def test_schedule_ai_dedups_due_queue(monkeypatch) -> None:
    scheduled_post_uids: list[str] = []

    class _FakeExecutor:
        def submit(self, fn, **kwargs):  # type: ignore[no-untyped-def]
            scheduled_post_uids.append(str(kwargs.get("post_uid") or ""))
            fut: Future = Future()
            fut.set_result(None)
            return fut

    monkeypatch.setattr(
        worker_module,
        "select_due_post_uids",
        lambda *_args, **_kwargs: ["weibo:1", "weibo:1", "weibo:2"],
    )
    monkeypatch.setattr(
        worker_module,
        "try_mark_ai_running",
        lambda *_args, **_kwargs: True,
    )

    config = LLMConfig(
        api_key="k",
        model="m",
        prompt_version="p",
        relevant_threshold=0.3,
        base_url="",
        api_mode="responses",
        ai_stream=True,
        ai_retries=0,
        ai_temperature=0.1,
        ai_reasoning_effort="low",
        ai_rpm=12.0,
        ai_timeout_seconds=30.0,
        trace_out=None,
        verbose=False,
    )
    inflight_futures: set[Future] = set()
    inflight_owner_by_future: dict[Future, str] = {}
    scheduled, has_error = _schedule_ai(
        _FakeExecutor(),  # type: ignore[arg-type]
        engine=object(),  # type: ignore[arg-type]
        platform="weibo",
        ai_cap=4,
        low_inflight_now_get=lambda: 0,
        inflight_futures=inflight_futures,
        inflight_owner_by_future=inflight_owner_by_future,
        inflight_owner="weibo",
        wakeup_event=threading.Event(),
        config=config,
        limiter=RateLimiter(100.0),
        verbose=False,
    )

    assert has_error is False
    assert scheduled == 2
    assert scheduled_post_uids == ["weibo:1", "weibo:2"]


def test_schedule_ai_prefers_redis_queue_when_available(monkeypatch, tmp_path) -> None:
    scheduled_payloads: list[dict[str, object]] = []
    redis_messages = [
        json.dumps(
            {
                "post_uid": "weibo:1",
                "platform": "weibo",
                "platform_post_id": "1",
                "author": "作者A",
                "created_at": "2026-03-28 10:00:00",
                "url": "https://example.com/post/1",
                "raw_text": "文本1",
                "display_md": "",
                "ingested_at": 100,
                "retry_count": 0,
            },
            ensure_ascii=False,
        )
    ]

    class _FakeExecutor:
        def submit(self, fn, **kwargs):  # type: ignore[no-untyped-def]
            del fn
            payload = kwargs.get("payload")
            if isinstance(payload, dict):
                scheduled_payloads.append(payload)
            fut: Future = Future()
            fut.set_result(None)
            return fut

    monkeypatch.setattr(
        worker_module,
        "select_due_post_uids",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("redis mode should not call select_due_post_uids")
        ),
    )
    monkeypatch.setattr(
        worker_module,
        "redis_ai_move_due_delayed_to_ready",
        lambda *_args, **_kwargs: 0,
    )
    monkeypatch.setattr(
        worker_module,
        "redis_ai_requeue_processing",
        lambda *_args, **_kwargs: 0,
    )
    monkeypatch.setattr(
        worker_module,
        "redis_ai_pop_to_processing",
        lambda *_args, **_kwargs: redis_messages.pop(0) if redis_messages else None,
    )
    monkeypatch.setattr(
        worker_module, "upsert_pending_post", lambda *_args, **_kwargs: None
    )

    config = LLMConfig(
        api_key="k",
        model="m",
        prompt_version="p",
        relevant_threshold=0.3,
        base_url="",
        api_mode="responses",
        ai_stream=True,
        ai_retries=0,
        ai_temperature=0.1,
        ai_reasoning_effort="low",
        ai_rpm=12.0,
        ai_timeout_seconds=30.0,
        trace_out=None,
        verbose=False,
    )
    inflight_futures: set[Future] = set()
    inflight_owner_by_future: dict[Future, str] = {}
    scheduled, has_error = _schedule_ai(
        _FakeExecutor(),  # type: ignore[arg-type]
        engine=object(),  # type: ignore[arg-type]
        platform="weibo",
        ai_cap=4,
        low_inflight_now_get=lambda: 0,
        inflight_futures=inflight_futures,
        inflight_owner_by_future=inflight_owner_by_future,
        inflight_owner="weibo",
        wakeup_event=threading.Event(),
        config=config,
        limiter=RateLimiter(100.0),
        verbose=False,
        redis_client=object(),
        redis_queue_key="queue",
        spool_dir=tmp_path,
    )

    assert has_error is False
    assert scheduled == 1
    assert len(scheduled_payloads) == 1
    assert str(scheduled_payloads[0].get("post_uid") or "") == "weibo:1"


def _build_source_runtime() -> WorkerSourceRuntime:
    return WorkerSourceRuntime(
        config=WorkerSourceConfig(
            name="weibo",
            platform="weibo",
            rss_urls=["https://example.com/rss"],
            author="",
            user_id=None,
            database_url="libsql://example.turso.io",
            auth_token="token",
        ),
        engine=object(),  # type: ignore[arg-type]
        spool_dir=Path("/tmp"),
        redis_queue_key="queue",
        rss_next_ingest_at=0.0,
    )


def test_spool_flush_trigger_tracks_new_items_without_losing_signal() -> None:
    source = _build_source_runtime()
    wakeup_event = threading.Event()

    assert _should_start_spool_flush(source=source) is False

    _mark_spool_item_ingested(source=source, wakeup_event=wakeup_event)
    assert wakeup_event.is_set() is True
    assert _should_start_spool_flush(source=source) is True

    _mark_spool_flush_started(source=source)
    assert _should_start_spool_flush(source=source) is False

    _mark_spool_item_ingested(source=source, wakeup_event=wakeup_event)
    assert _should_start_spool_flush(source=source) is True


def test_spool_flush_retry_retriggers_without_new_items() -> None:
    source = _build_source_runtime()

    _request_spool_flush(source=source)
    assert _should_start_spool_flush(source=source) is True

    _mark_spool_flush_started(source=source)
    assert _should_start_spool_flush(source=source) is False

    _mark_spool_flush_retry(source=source, has_more=False, has_error=True)
    assert _should_start_spool_flush(source=source) is True


def test_process_one_redis_payload_skips_turso_recent_load_on_cache_hit(
    monkeypatch, tmp_path
) -> None:
    load_calls: list[str] = []
    ack_calls: list[str] = []

    monkeypatch.setattr(
        worker_module,
        "redis_author_recent_push",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        worker_module,
        "redis_author_recent_load",
        lambda *_args, **_kwargs: [{"post_uid": "weibo:hist"}],
    )
    monkeypatch.setattr(
        worker_module,
        "load_recent_posts_by_author",
        lambda *_args, **_kwargs: load_calls.append("called"),
    )
    monkeypatch.setattr(
        worker_module,
        "_process_one_post_uid",
        lambda *_args, **_kwargs: True,
    )

    def _fake_ack_and_cleanup(*_args, **_kwargs) -> bool:  # type: ignore[no-untyped-def]
        ack_calls.append("acked")
        return True

    monkeypatch.setattr(
        worker_module,
        "redis_ai_ack_and_cleanup",
        _fake_ack_and_cleanup,
    )

    payload: dict[str, object] = {
        "post_uid": "weibo:1",
        "platform": "weibo",
        "platform_post_id": "1",
        "author": "作者A",
        "created_at": "2026-03-28 10:00:00",
        "url": "https://example.com/post/1",
        "raw_text": "正文",
        "display_md": "正文",
        "ingested_at": 100,
    }
    config = LLMConfig(
        api_key="k",
        model="m",
        prompt_version="p",
        relevant_threshold=0.3,
        base_url="",
        api_mode="responses",
        ai_stream=True,
        ai_retries=0,
        ai_temperature=0.1,
        ai_reasoning_effort="low",
        ai_rpm=12.0,
        ai_timeout_seconds=30.0,
        trace_out=None,
        verbose=False,
    )
    worker_module._process_one_redis_payload(
        engine=object(),  # type: ignore[arg-type]
        payload=payload,
        processing_msg="msg-1",
        redis_client=object(),
        redis_queue_key="queue",
        spool_dir=tmp_path,
        config=config,
        limiter=RateLimiter(100.0),
        verbose=False,
    )

    assert load_calls == []
    assert ack_calls == ["acked"]


def test_process_one_redis_payload_marks_empty_author_cache_on_first_miss(
    monkeypatch, tmp_path
) -> None:
    empty_mark_calls: list[str] = []

    monkeypatch.setattr(
        worker_module,
        "redis_author_recent_push",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        worker_module,
        "redis_author_recent_is_marked_empty",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        worker_module,
        "redis_author_recent_load",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        worker_module,
        "load_recent_posts_by_author",
        lambda *_args, **_kwargs: [],
    )

    def _fake_mark_empty(*_args, **kwargs) -> bool:  # type: ignore[no-untyped-def]
        empty_mark_calls.append(str(kwargs.get("author") or ""))
        return True

    monkeypatch.setattr(
        worker_module,
        "redis_author_recent_mark_empty",
        _fake_mark_empty,
    )
    monkeypatch.setattr(
        worker_module,
        "_process_one_post_uid",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        worker_module,
        "redis_ai_ack_and_cleanup",
        lambda *_args, **_kwargs: True,
    )

    payload: dict[str, object] = {
        "post_uid": "weibo:2",
        "platform": "weibo",
        "platform_post_id": "2",
        "author": "作者B",
        "created_at": "2026-03-28 10:00:00",
        "url": "https://example.com/post/2",
        "raw_text": "正文",
        "display_md": "正文",
        "ingested_at": 100,
    }
    config = LLMConfig(
        api_key="k",
        model="m",
        prompt_version="p",
        relevant_threshold=0.3,
        base_url="",
        api_mode="responses",
        ai_stream=True,
        ai_retries=0,
        ai_temperature=0.1,
        ai_reasoning_effort="low",
        ai_rpm=12.0,
        ai_timeout_seconds=30.0,
        trace_out=None,
        verbose=False,
    )
    worker_module._process_one_redis_payload(
        engine=object(),  # type: ignore[arg-type]
        payload=payload,
        processing_msg="msg-2",
        redis_client=object(),
        redis_queue_key="queue",
        spool_dir=tmp_path,
        config=config,
        limiter=RateLimiter(100.0),
        verbose=False,
    )

    assert empty_mark_calls == ["作者B"]
