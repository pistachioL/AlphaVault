from __future__ import annotations

from types import SimpleNamespace
from typing import cast

from alphavault.worker.runtime_models import WorkerSourceRuntime
from alphavault.worker import worker_loop_source_tick
from alphavault.worker.worker_loop_models import (
    SourceTickContext,
    SourceTickExecutors,
    SourceTickState,
)


def test_worker_source_runtime_has_no_local_cache_fields() -> None:
    fields = WorkerSourceRuntime.__dataclass_fields__

    assert "local_cache_ready" not in fields
    assert "local_cache_rebuild_next_at" not in fields


def test_schedule_rss_and_spool_also_schedules_redis_enqueue(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        worker_loop_source_tick,
        "maybe_schedule_rss_ingest",
        lambda **_kwargs: calls.append("rss"),
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "maybe_schedule_spool_flush",
        lambda **_kwargs: calls.append("spool"),
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "maybe_schedule_redis_enqueue",
        lambda **_kwargs: calls.append("redis"),
    )

    worker_loop_source_tick._schedule_rss_and_spool(
        source=SimpleNamespace(config=SimpleNamespace(platform="weibo")),
        active_engine=object(),
        ctx=cast(SourceTickContext, SimpleNamespace()),
        execs=cast(
            SourceTickExecutors,
            SimpleNamespace(
                rss_executor=object(),
                spool_executor=object(),
                redis_enqueue_executor=object(),
            ),
        ),
        state=cast(
            SourceTickState,
            SimpleNamespace(
                wakeup_event=object(),
                inflight_futures=set(),
                inflight_owner_by_future={},
            ),
        ),
    )

    assert calls == ["rss", "spool", "redis"]


def test_run_source_maintenance_only_treats_same_source_ai_as_running(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_run_maintenance_if_due(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return False

    monkeypatch.setattr(
        worker_loop_source_tick,
        "run_maintenance_if_due",
        _fake_run_maintenance_if_due,
    )

    worker_loop_source_tick._run_source_maintenance(
        source=SimpleNamespace(
            config=SimpleNamespace(name="weibo-main", platform="weibo"),
            redis_enqueue_future=None,
            spool_flush_future=None,
        ),
        source_name="weibo-main",
        active_engine=object(),
        ctx=cast(SourceTickContext, SimpleNamespace()),
        state=cast(
            SourceTickState,
            SimpleNamespace(
                inflight_owner_by_future={object(): "weibo-main"},
            ),
        ),
    )

    assert captured["source_has_running_jobs"] is True
