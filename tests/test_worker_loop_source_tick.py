from __future__ import annotations

from types import SimpleNamespace
from typing import cast

from alphavault.worker import worker_loop_source_tick
from alphavault.worker.worker_loop_models import (
    SourceTickContext,
    SourceTickExecutors,
    SourceTickState,
)


def test_ensure_local_cache_ready_rebuilds_from_outbox_on_success(monkeypatch) -> None:
    source = SimpleNamespace(
        local_cache_ready=False,
        local_cache_rebuild_next_at=0.0,
    )
    ctx = cast(SourceTickContext, SimpleNamespace(now=100.0, verbose=False))
    calls: list[tuple[object, str]] = []

    def _fake_rebuild(
        engine: object, *, source_name: str, verbose: bool
    ) -> dict[str, int | bool]:
        del verbose
        calls.append((engine, source_name))
        return {"processed": 2, "inserted": 3, "cursor": 9, "has_error": False}

    monkeypatch.setattr(
        worker_loop_source_tick,
        "rebuild_local_cache_from_outbox",
        _fake_rebuild,
    )

    worker_loop_source_tick._ensure_local_cache_ready(
        source=source,
        active_engine=object(),
        source_name="weibo",
        ctx=ctx,
    )

    assert calls == [(calls[0][0], "weibo")]
    assert source.local_cache_ready is True
    assert source.local_cache_rebuild_next_at == 0.0


def test_ensure_local_cache_ready_sets_retry_when_rebuild_fails(monkeypatch) -> None:
    source = SimpleNamespace(
        local_cache_ready=False,
        local_cache_rebuild_next_at=0.0,
    )
    ctx = cast(SourceTickContext, SimpleNamespace(now=100.0, verbose=False))

    monkeypatch.setattr(
        worker_loop_source_tick,
        "rebuild_local_cache_from_outbox",
        lambda *_args, **_kwargs: {
            "processed": 0,
            "inserted": 0,
            "cursor": 0,
            "has_error": True,
        },
    )

    worker_loop_source_tick._ensure_local_cache_ready(
        source=source,
        active_engine=object(),
        source_name="weibo",
        ctx=ctx,
    )

    assert source.local_cache_ready is False
    assert source.local_cache_rebuild_next_at == 400.0


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
