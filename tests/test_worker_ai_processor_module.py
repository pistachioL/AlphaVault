from __future__ import annotations

from concurrent.futures import Future
import json
import threading

from alphavault.worker import scheduler as scheduler_module
from alphavault.worker import worker_loop_ai


def test_dedup_post_uids_keeps_order_and_removes_empty() -> None:
    out = scheduler_module.dedup_post_uids(["", "weibo:1", "weibo:1", "weibo:2"])
    assert out == ["weibo:1", "weibo:2"]


def test_schedule_ai_from_redis_acks_bad_payload_and_schedules_valid_one() -> None:
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

    queue_messages = [
        "not json",
        json.dumps({"post_uid": "weibo:1", "platform": "weibo"}, ensure_ascii=False),
    ]
    acked: list[str] = []

    executor = _FakeExecutor()
    inflight_futures: set[Future] = set()
    inflight_owner_by_future: dict[Future, str] = {}
    scheduled, has_error = scheduler_module.schedule_ai_from_redis(
        executor=executor,  # type: ignore[arg-type]
        engine=object(),
        ai_cap=4,
        low_inflight_now_get=lambda: 0,
        inflight_futures=inflight_futures,
        inflight_owner_by_future=inflight_owner_by_future,
        inflight_owner="weibo",
        wakeup_event=threading.Event(),
        config=object(),
        limiter=object(),
        verbose=False,
        redis_client=object(),
        redis_queue_key="queue",
        spool_dir="/tmp",
        prune_inflight_futures_fn=lambda futures, owners: None,
        compute_rss_available_slots_fn=lambda **_kwargs: 2,
        pop_to_processing_fn=lambda *_args, **_kwargs: (
            queue_messages.pop(0) if queue_messages else None
        ),
        ack_processing_fn=lambda _client, _key, msg: acked.append(str(msg)),
        process_one_redis_payload_fn=lambda **_kwargs: None,
        fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
    )

    assert has_error is False
    assert scheduled == 1
    assert acked == ["not json"]
    assert len(executor.scheduled_payloads) == 1
    assert str(executor.scheduled_payloads[0].get("post_uid") or "") == "weibo:1"


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
                "spool_dir": "/tmp",
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
                "stuck_seconds": 60,
            },
        )(),
        ai_executor=object(),  # type: ignore[arg-type]
        inflight_futures=set(),
        inflight_owner_by_future={},
        wakeup_event=threading.Event(),
    )

    assert has_error is False
    assert captured["inflight_owner"] == "weibo-main"
