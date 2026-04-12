from __future__ import annotations

from concurrent.futures import Future
import json
import logging
import threading
from typing import Callable

from alphavault.rss.utils import RateLimiter
from alphavault.worker import periodic_jobs
from alphavault.worker import scheduler as scheduler_module
from alphavault.worker import worker_loop_ai


def test_dedup_post_uids_keeps_order_and_removes_empty() -> None:
    out = scheduler_module.dedup_post_uids(["", "weibo:1", "weibo:1", "weibo:2"])
    assert out == ["weibo:1", "weibo:2"]


def test_schedule_ai_from_stream_acks_bad_payload_and_schedules_valid_one() -> None:
    class _FakeExecutor:
        def __init__(self) -> None:
            self.scheduled_payloads: list[dict[str, object]] = []

        def submit(self, fn, **kwargs):  # type: ignore[no-untyped-def]
            del fn
            payload = kwargs.get("payload")
            if isinstance(payload, dict):
                self.scheduled_payloads.append(payload)
            fut: Future = Future()
            fut.set_result(None)
            return fut

    stream_messages = [
        {"message_id": "1-0", "payload": "not json"},
        {
            "message_id": "2-0",
            "payload": json.dumps(
                {"post_uid": "weibo:1", "platform": "weibo"},
                ensure_ascii=False,
            ),
        },
    ]
    acked: list[str] = []

    executor = _FakeExecutor()
    inflight_futures: set[Future] = set()
    inflight_owner_by_future: dict[Future, str] = {}
    scheduled, has_error = scheduler_module.schedule_ai_from_stream(
        executor=executor,  # type: ignore[arg-type]
        engine=object(),
        ai_cap=4,
        low_inflight_now_get=lambda: 0,
        inflight_futures=inflight_futures,
        inflight_owner_by_future=inflight_owner_by_future,
        inflight_owner="weibo",
        consumer_name="weibo:worker",
        wakeup_event=threading.Event(),
        config=object(),
        limiter=object(),
        redis_client=object(),
        redis_queue_key="queue",
        prune_inflight_futures_fn=lambda futures, owners: None,
        compute_rss_available_slots_fn=lambda **_kwargs: 2,
        move_due_retry_to_stream_fn=lambda *_args, **_kwargs: 0,
        claim_stuck_messages_fn=lambda *_args, **_kwargs: [],
        read_group_messages_fn=lambda *_args, **_kwargs: list(stream_messages),
        ack_message_fn=lambda _client, _key, message_id: acked.append(str(message_id)),
        process_one_redis_payload_fn=lambda **_kwargs: None,
        fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        stuck_seconds=1000,
    )

    assert has_error is False
    assert scheduled == 1
    assert acked == ["1-0"]
    assert len(executor.scheduled_payloads) == 1
    assert str(executor.scheduled_payloads[0].get("post_uid") or "") == "weibo:1"


def test_schedule_ai_from_stream_moves_due_then_claims_then_reads() -> None:
    seen_order: list[str] = []

    class _FakeExecutor:
        def submit(self, fn, **kwargs):  # type: ignore[no-untyped-def]
            del fn, kwargs
            fut: Future = Future()
            fut.set_result(None)
            return fut

    def _move_due(*_args, **_kwargs) -> int:
        seen_order.append("move_due")
        return 0

    def _claim(*_args, **_kwargs) -> list[dict[str, str]]:
        seen_order.append("claim")
        return []

    def _read(*_args, **_kwargs) -> list[dict[str, str]]:
        seen_order.append("read")
        return []

    scheduled, has_error = scheduler_module.schedule_ai_from_stream(
        executor=_FakeExecutor(),  # type: ignore[arg-type]
        engine=object(),
        ai_cap=1,
        low_inflight_now_get=lambda: 0,
        inflight_futures=set(),
        inflight_owner_by_future={},
        inflight_owner="weibo",
        consumer_name="weibo:worker",
        wakeup_event=threading.Event(),
        config=object(),
        limiter=object(),
        redis_client=object(),
        redis_queue_key="queue",
        prune_inflight_futures_fn=lambda futures, owners: None,
        compute_rss_available_slots_fn=lambda **_kwargs: 1,
        move_due_retry_to_stream_fn=_move_due,
        claim_stuck_messages_fn=_claim,
        read_group_messages_fn=_read,
        ack_message_fn=lambda *_args, **_kwargs: None,
        process_one_redis_payload_fn=lambda **_kwargs: None,
        fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        stuck_seconds=1000,
    )

    assert scheduled == 0
    assert has_error is False
    assert seen_order == ["move_due", "claim", "read"]


def test_schedule_ai_from_stream_skips_redis_when_request_slot_unavailable() -> None:
    seen_order: list[str] = []

    class _FakeReservation:
        def wait(self) -> None:
            seen_order.append("wait")

        def cancel(self) -> None:
            seen_order.append("cancel")

    class _FakeLimiter:
        def has_limit(self) -> bool:
            return True

        def try_reserve(self):  # type: ignore[no-untyped-def]
            seen_order.append("try_reserve")
            return None

    class _FakeExecutor:
        def submit(self, fn, **kwargs):  # type: ignore[no-untyped-def]
            del fn, kwargs
            seen_order.append("submit")
            fut: Future = Future()
            fut.set_result(None)
            return fut

    def _move_due(*_args, **_kwargs) -> int:
        seen_order.append("move_due")
        return 0

    def _claim(*_args, **_kwargs) -> list[dict[str, str]]:
        seen_order.append("claim")
        return []

    def _read(*_args, **_kwargs) -> list[dict[str, str]]:
        seen_order.append("read")
        return []

    scheduled, has_error = scheduler_module.schedule_ai_from_stream(
        executor=_FakeExecutor(),  # type: ignore[arg-type]
        engine=object(),
        ai_cap=2,
        low_inflight_now_get=lambda: 0,
        inflight_futures=set(),
        inflight_owner_by_future={},
        inflight_owner="weibo",
        consumer_name="weibo:worker",
        wakeup_event=threading.Event(),
        config=object(),
        limiter=_FakeLimiter(),
        redis_client=object(),
        redis_queue_key="queue",
        prune_inflight_futures_fn=lambda futures, owners: None,
        compute_rss_available_slots_fn=lambda **_kwargs: 2,
        move_due_retry_to_stream_fn=_move_due,
        claim_stuck_messages_fn=_claim,
        read_group_messages_fn=_read,
        ack_message_fn=lambda *_args, **_kwargs: None,
        process_one_redis_payload_fn=lambda **_kwargs: None,
        fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        stuck_seconds=1000,
    )

    assert scheduled == 0
    assert has_error is False
    assert seen_order == ["try_reserve"]


def test_schedule_ai_from_stream_reads_only_one_message_per_request_slot() -> None:
    read_counts: list[int] = []

    class _FakeReservation:
        def wait(self) -> None:
            return None

        def cancel(self) -> None:
            return None

    class _FakeLimiter:
        def has_limit(self) -> bool:
            return True

        def try_reserve(self):  # type: ignore[no-untyped-def]
            return _FakeReservation()

    class _FakeExecutor:
        def __init__(self) -> None:
            self.scheduled_payloads: list[dict[str, object]] = []

        def submit(self, fn, **kwargs):  # type: ignore[no-untyped-def]
            del fn
            payload = kwargs.get("payload")
            if isinstance(payload, dict):
                self.scheduled_payloads.append(payload)
            fut: Future = Future()
            fut.set_result(None)
            return fut

    stream_messages = [
        {
            "message_id": "1-0",
            "payload": json.dumps({"post_uid": "weibo:1"}, ensure_ascii=False),
        },
        {
            "message_id": "2-0",
            "payload": json.dumps({"post_uid": "weibo:2"}, ensure_ascii=False),
        },
    ]
    executor = _FakeExecutor()

    def _read(*_args, **kwargs) -> list[dict[str, str]]:
        read_counts.append(int(kwargs.get("count") or 0))
        return list(stream_messages)

    scheduled, has_error = scheduler_module.schedule_ai_from_stream(
        executor=executor,  # type: ignore[arg-type]
        engine=object(),
        ai_cap=4,
        low_inflight_now_get=lambda: 0,
        inflight_futures=set(),
        inflight_owner_by_future={},
        inflight_owner="weibo",
        consumer_name="weibo:worker",
        wakeup_event=threading.Event(),
        config=object(),
        limiter=_FakeLimiter(),
        redis_client=object(),
        redis_queue_key="queue",
        prune_inflight_futures_fn=lambda futures, owners: None,
        compute_rss_available_slots_fn=lambda **_kwargs: 4,
        move_due_retry_to_stream_fn=lambda *_args, **_kwargs: 0,
        claim_stuck_messages_fn=lambda *_args, **_kwargs: [],
        read_group_messages_fn=_read,
        ack_message_fn=lambda *_args, **_kwargs: None,
        process_one_redis_payload_fn=lambda **_kwargs: None,
        fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        stuck_seconds=1000,
    )

    assert scheduled == 1
    assert has_error is False
    assert read_counts == [1]
    assert len(executor.scheduled_payloads) == 1
    assert str(executor.scheduled_payloads[0].get("post_uid") or "") == "weibo:1"


def test_schedule_ai_from_stream_unlimited_rate_limiter_keeps_available_count() -> None:
    read_counts: list[int] = []

    class _FakeExecutor:
        def __init__(self) -> None:
            self.scheduled_payloads: list[dict[str, object]] = []

        def submit(self, fn, **kwargs):  # type: ignore[no-untyped-def]
            del fn
            payload = kwargs.get("payload")
            if isinstance(payload, dict):
                self.scheduled_payloads.append(payload)
            fut: Future = Future()
            fut.set_result(None)
            return fut

    stream_messages = [
        {
            "message_id": "1-0",
            "payload": json.dumps({"post_uid": "weibo:1"}, ensure_ascii=False),
        },
        {
            "message_id": "2-0",
            "payload": json.dumps({"post_uid": "weibo:2"}, ensure_ascii=False),
        },
    ]
    executor = _FakeExecutor()

    def _read(*_args, **kwargs) -> list[dict[str, str]]:
        read_counts.append(int(kwargs.get("count") or 0))
        return list(stream_messages)

    scheduled, has_error = scheduler_module.schedule_ai_from_stream(
        executor=executor,  # type: ignore[arg-type]
        engine=object(),
        ai_cap=4,
        low_inflight_now_get=lambda: 0,
        inflight_futures=set(),
        inflight_owner_by_future={},
        inflight_owner="weibo",
        consumer_name="weibo:worker",
        wakeup_event=threading.Event(),
        config=object(),
        limiter=RateLimiter(0),
        redis_client=object(),
        redis_queue_key="queue",
        prune_inflight_futures_fn=lambda futures, owners: None,
        compute_rss_available_slots_fn=lambda **_kwargs: 4,
        move_due_retry_to_stream_fn=lambda *_args, **_kwargs: 0,
        claim_stuck_messages_fn=lambda *_args, **_kwargs: [],
        read_group_messages_fn=_read,
        ack_message_fn=lambda *_args, **_kwargs: None,
        process_one_redis_payload_fn=lambda **_kwargs: None,
        fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        stuck_seconds=1000,
    )

    assert scheduled == 2
    assert has_error is False
    assert read_counts == [4]
    assert len(executor.scheduled_payloads) == 2


def test_schedule_ai_from_stream_empty_poll_releases_reserved_request_slot() -> None:
    class _FakeExecutor:
        def __init__(self) -> None:
            self.scheduled_payloads: list[dict[str, object]] = []

        def submit(self, fn, **kwargs):  # type: ignore[no-untyped-def]
            del fn
            payload = kwargs.get("payload")
            if isinstance(payload, dict):
                self.scheduled_payloads.append(payload)
            fut: Future = Future()
            fut.set_result(None)
            return fut

    limiter = RateLimiter(12)
    executor = _FakeExecutor()
    inflight_futures: set[Future] = set()
    inflight_owner_by_future: dict[Future, str] = {}
    read_counts: list[tuple[str, int]] = []

    def _first_read(*_args, **kwargs) -> list[dict[str, str]]:
        read_counts.append(("first", int(kwargs.get("count") or 0)))
        return []

    def _second_read(*_args, **kwargs) -> list[dict[str, str]]:
        read_counts.append(("second", int(kwargs.get("count") or 0)))
        return [
            {
                "message_id": "1-0",
                "payload": json.dumps({"post_uid": "weibo:1"}, ensure_ascii=False),
            }
        ]

    def _schedule(
        read_group_messages_fn: Callable[..., list[dict[str, str]]],
    ) -> tuple[int, bool]:
        return scheduler_module.schedule_ai_from_stream(
            executor=executor,  # type: ignore[arg-type]
            engine=object(),
            ai_cap=4,
            low_inflight_now_get=lambda: 0,
            inflight_futures=inflight_futures,
            inflight_owner_by_future=inflight_owner_by_future,
            inflight_owner="weibo",
            consumer_name="weibo:worker",
            wakeup_event=threading.Event(),
            config=object(),
            limiter=limiter,
            redis_client=object(),
            redis_queue_key="queue",
            prune_inflight_futures_fn=lambda futures, owners: None,
            compute_rss_available_slots_fn=lambda **_kwargs: 4,
            move_due_retry_to_stream_fn=lambda *_args, **_kwargs: 0,
            claim_stuck_messages_fn=lambda *_args, **_kwargs: [],
            read_group_messages_fn=read_group_messages_fn,
            ack_message_fn=lambda *_args, **_kwargs: None,
            process_one_redis_payload_fn=lambda **_kwargs: None,
            fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
            stuck_seconds=1000,
        )

    first_scheduled, first_has_error = _schedule(_first_read)
    second_scheduled, second_has_error = _schedule(_second_read)

    assert first_scheduled == 0
    assert first_has_error is False
    assert second_scheduled == 1
    assert second_has_error is False
    assert read_counts == [("first", 1), ("second", 1)]
    assert len(executor.scheduled_payloads) == 1


def test_schedule_ai_for_source_tracks_inflight_owner_by_source_name(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_schedule_ai(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return 0, False

    monkeypatch.setattr(worker_loop_ai.scheduler, "schedule_ai", _fake_schedule_ai)

    has_error = worker_loop_ai.schedule_ai_for_source(
        source=type(
            "_Source",
            (),
            {
                "config": type(
                    "_Config",
                    (),
                    {"name": "weibo-main", "platform": "weibo"},
                )(),
                "redis_queue_key": "queue",
            },
        )(),
        active_engine=None,
        platform="weibo",
        ctx=type(
            "_Ctx",
            (),
            {
                "ai_cap": 4,
                "config": object(),
                "limiter": object(),
                "redis_client": object(),
                "stuck_seconds": 1000,
            },
        )(),
        ai_executor=object(),  # type: ignore[arg-type]
        inflight_futures=set(),
        inflight_owner_by_future={},
        wakeup_event=threading.Event(),
    )

    assert has_error is False
    assert captured["inflight_owner"] == "weibo-main"
    assert "weibo-main" in str(captured["consumer_name"])


def test_prune_inflight_futures_logs_exception_and_clears_future(
    caplog,
) -> None:
    future: Future = Future()
    future.set_exception(RuntimeError("boom"))
    inflight_futures: set[Future] = {future}
    inflight_owner_by_future = {future: "xueqiu"}

    with caplog.at_level(logging.WARNING):
        periodic_jobs.prune_inflight_futures(
            inflight_futures,
            inflight_owner_by_future,
        )

    assert "[ai] future_error owner=xueqiu RuntimeError: boom" in caplog.text
    assert inflight_futures == set()
    assert inflight_owner_by_future == {}
