from __future__ import annotations

from collections import deque
import threading
from types import SimpleNamespace

from alphavault.worker import periodic_jobs as periodic_jobs_module


def _source_runtime():
    return SimpleNamespace(
        spool_state_lock=threading.Lock(),
        spool_seq_written=0,
        spool_seq_scheduled=0,
        spool_need_retry=False,
        redis_enqueue_state_lock=threading.Lock(),
        redis_enqueue_pending=deque(),
        redis_enqueue_need_retry=False,
    )


def test_spool_flush_state_transitions() -> None:
    source = _source_runtime()
    wakeup_event = threading.Event()

    assert periodic_jobs_module.should_start_spool_flush(source=source) is False

    periodic_jobs_module.mark_spool_item_ingested(
        source=source, wakeup_event=wakeup_event
    )
    assert wakeup_event.is_set() is True
    assert periodic_jobs_module.should_start_spool_flush(source=source) is True

    periodic_jobs_module.mark_spool_flush_started(source=source)
    assert periodic_jobs_module.should_start_spool_flush(source=source) is False

    periodic_jobs_module.mark_spool_flush_retry(
        source=source,
        has_more=False,
        has_error=True,
    )
    assert periodic_jobs_module.should_start_spool_flush(source=source) is True


def test_redis_enqueue_state_transitions() -> None:
    source = _source_runtime()
    wakeup_event = threading.Event()
    payload = {"post_uid": "weibo:1", "raw_text": "正文"}

    assert periodic_jobs_module.should_start_redis_enqueue(source=source) is False

    periodic_jobs_module.enqueue_redis_payload(
        source=source,
        payload=payload,
        wakeup_event=wakeup_event,
    )
    assert wakeup_event.is_set() is True
    assert periodic_jobs_module.should_start_redis_enqueue(source=source) is True

    periodic_jobs_module.mark_redis_enqueue_started(source=source)
    assert source.redis_enqueue_need_retry is False

    popped = periodic_jobs_module.pop_next_redis_enqueue_payload(source=source)
    assert popped == payload
    assert periodic_jobs_module.should_start_redis_enqueue(source=source) is False

    periodic_jobs_module.restore_redis_enqueue_payload(
        source=source,
        payload=payload,
        wakeup_event=wakeup_event,
    )
    periodic_jobs_module.mark_redis_enqueue_retry(
        source=source,
        has_more=True,
        has_error=True,
    )
    assert periodic_jobs_module.should_start_redis_enqueue(source=source) is True
