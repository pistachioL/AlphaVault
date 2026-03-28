from __future__ import annotations

from alphavault.worker.worker import (
    _LowPriorityAISlotGate,
    _build_topic_prompt_v3_llm_log_line,
    _build_low_priority_should_continue,
    _compute_low_priority_budget,
    _compute_rss_available_slots,
    _should_fast_retry_for_periodic_job,
)


def test_compute_low_priority_budget_zero_when_rss_is_full() -> None:
    assert _compute_low_priority_budget(ai_cap=6, rss_inflight_now=6) == 0
    assert _compute_low_priority_budget(ai_cap=6, rss_inflight_now=9) == 0


def test_compute_low_priority_budget_uses_remaining_slots() -> None:
    assert _compute_low_priority_budget(ai_cap=6, rss_inflight_now=2) == 4


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
