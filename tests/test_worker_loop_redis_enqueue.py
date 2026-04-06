from __future__ import annotations

from collections import deque
import threading
from types import SimpleNamespace

from alphavault.worker import worker_loop_redis_enqueue


def _source_runtime():
    return SimpleNamespace(
        redis_enqueue_state_lock=threading.Lock(),
        redis_enqueue_pending=deque(),
        redis_enqueue_need_retry=False,
    )


def test_submit_redis_enqueue_job_counts_pushed_and_duplicate(monkeypatch) -> None:
    source = _source_runtime()
    source.redis_enqueue_pending.extend(
        [
            {"post_uid": "weibo:1", "raw_text": "A"},
            {"post_uid": "weibo:2", "raw_text": "B"},
        ]
    )
    statuses = iter([(1, False), (0, False)])

    monkeypatch.setattr(
        worker_loop_redis_enqueue,
        "_try_push_payload_to_ai_ready",
        lambda **_kwargs: next(statuses),
    )

    stats = worker_loop_redis_enqueue.submit_redis_enqueue_job(
        source=source,
        redis_client=object(),
        redis_queue_key="test:q",
        verbose=False,
    )

    assert stats == {
        "attempted": 2,
        "pushed": 1,
        "duplicates": 1,
        "has_more": False,
        "has_error": False,
    }
    assert list(source.redis_enqueue_pending) == []


def test_submit_redis_enqueue_job_requeues_current_payload_on_error(
    monkeypatch,
) -> None:
    source = _source_runtime()
    first = {"post_uid": "weibo:1", "raw_text": "A"}
    second = {"post_uid": "weibo:2", "raw_text": "B"}
    source.redis_enqueue_pending.extend([first, second])

    monkeypatch.setattr(
        worker_loop_redis_enqueue,
        "_try_push_payload_to_ai_ready",
        lambda **_kwargs: (0, True),
    )

    stats = worker_loop_redis_enqueue.submit_redis_enqueue_job(
        source=source,
        redis_client=object(),
        redis_queue_key="test:q",
        verbose=False,
    )

    assert stats == {
        "attempted": 1,
        "pushed": 0,
        "duplicates": 0,
        "has_more": True,
        "has_error": True,
    }
    assert list(source.redis_enqueue_pending) == [first, second]
