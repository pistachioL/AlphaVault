from __future__ import annotations

from concurrent.futures import Future
import json
import threading

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
        verbose=False,
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
        verbose=False,
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
                "verbose": False,
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
    capsys,
) -> None:
    future: Future = Future()
    future.set_exception(RuntimeError("boom"))
    inflight_futures: set[Future] = {future}
    inflight_owner_by_future = {future: "xueqiu"}

    periodic_jobs.prune_inflight_futures(
        inflight_futures,
        inflight_owner_by_future,
    )

    captured = capsys.readouterr()
    assert "[ai] future_error owner=xueqiu RuntimeError: boom" in captured.out
    assert inflight_futures == set()
    assert inflight_owner_by_future == {}
