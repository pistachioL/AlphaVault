from __future__ import annotations

from concurrent.futures import Future

import pytest

from alphavault.worker.worker import (
    _LowPriorityAISlotGate,
    _build_topic_prompt_v3_llm_log_line,
    _build_low_priority_should_continue,
    _compute_backfill_max_stocks_per_run,
    _collect_rss_ingest_result,
    _compute_low_priority_budget,
    _compute_rss_available_slots,
    _should_fast_retry_for_periodic_job,
    _should_wait_with_event,
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
