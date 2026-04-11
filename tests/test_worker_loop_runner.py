from __future__ import annotations

from types import SimpleNamespace
from typing import cast

from alphavault.rss.utils import RateLimiter
from alphavault.worker import worker_loop_runner
from alphavault.worker import worker_loop_source_tick
from alphavault.worker.worker_loop_models import (
    SourceTickContext,
    SourceTickExecutors,
    SourceTickState,
)


def _tick_ctx() -> SourceTickContext:
    return cast(
        SourceTickContext,
        SimpleNamespace(
            args=SimpleNamespace(),
            config=object(),
            limiter=object(),
            redis_client=object(),
            ai_cap=2,
            limit_or_none=None,
            stuck_seconds=60,
            verbose=False,
            rss_active_hours=None,
            rss_interval_seconds=600.0,
            rss_feed_sleep_seconds=0.0,
            worker_interval_seconds=600.0,
            maintenance_next_at=600.0,
            now=0.0,
            do_maintenance=False,
            due_ai_pending_get=None,
        ),
    )


def test_open_executors_does_not_create_removed_queue_executors() -> None:
    with worker_loop_runner._open_executors(ai_cap=1, source_count=1) as execs:
        assert hasattr(execs, "ai_executor")
        assert hasattr(execs, "stock_hot_executor")
        assert hasattr(execs, "rss_executor")
        assert not hasattr(execs, "redis_enqueue_executor")
        assert not hasattr(execs, "spool_executor")


def test_run_sources_once_schedules_all_rss_before_any_ai(monkeypatch) -> None:
    events: list[str] = []

    def _record_maintenance(**kwargs):  # type: ignore[no-untyped-def]
        events.append(f"maintenance:{kwargs['source'].config.name}")
        return False

    def _record_ai(**kwargs):  # type: ignore[no-untyped-def]
        events.append(f"ai:{kwargs['source'].config.name}")
        return False

    monkeypatch.setattr(
        worker_loop_runner,
        "_build_tick_ctx",
        lambda **_kwargs: _tick_ctx(),
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_ensure_active_engine",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_collect_source_finished_jobs",
        lambda **_kwargs: (
            {
                "maintenance_error": False,
                "redis_enqueue_error": False,
                "spool_flush_error": False,
                "schedule_error": False,
                "stock_hot_error": False,
            },
            False,
        ),
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_schedule_rss_and_spool",
        lambda **kwargs: events.append(f"rss:{kwargs['source'].config.name}"),
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_run_source_maintenance",
        _record_maintenance,
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_run_source_ai_schedule",
        _record_ai,
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_run_source_low_priority",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_any_inflight",
        lambda **_kwargs: False,
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_save_cycle_progress",
        lambda **_kwargs: None,
    )

    loop_ctx = cast(
        worker_loop_runner.WorkerLoopContext,
        SimpleNamespace(
            sources=[
                SimpleNamespace(config=SimpleNamespace(name="weibo", platform="weibo")),
                SimpleNamespace(
                    config=SimpleNamespace(name="xueqiu", platform="xueqiu")
                ),
            ]
        ),
    )

    worker_loop_runner._run_sources_once(
        loop_ctx=loop_ctx,
        worker_interval_seconds=600.0,
        maintenance_next_at=600.0,
        now=0.0,
        do_maintenance=False,
        execs=cast(SourceTickExecutors, SimpleNamespace()),
        state=cast(
            SourceTickState,
            SimpleNamespace(
                wakeup_event=object(),
                inflight_futures=set(),
                inflight_owner_by_future={},
            ),
        ),
    )

    assert events == [
        "rss:weibo",
        "rss:xueqiu",
        "maintenance:weibo",
        "maintenance:xueqiu",
        "ai:weibo",
        "ai:xueqiu",
    ]


def test_run_sources_once_keeps_finalizing_later_sources_when_first_has_inflight(
    monkeypatch,
) -> None:
    events: list[str] = []

    def _record_maintenance(**kwargs):  # type: ignore[no-untyped-def]
        events.append(f"maintenance:{kwargs['source'].config.name}")
        return False

    def _record_ai(**kwargs):  # type: ignore[no-untyped-def]
        events.append(f"ai:{kwargs['source'].config.name}")
        return False

    monkeypatch.setattr(
        worker_loop_runner,
        "_build_tick_ctx",
        lambda **_kwargs: _tick_ctx(),
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_ensure_active_engine",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_collect_source_finished_jobs",
        lambda **_kwargs: (
            {
                "maintenance_error": False,
                "redis_enqueue_error": False,
                "spool_flush_error": False,
                "schedule_error": False,
                "stock_hot_error": False,
            },
            False,
        ),
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_schedule_rss_and_spool",
        lambda **kwargs: events.append(f"rss:{kwargs['source'].config.name}"),
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_run_source_maintenance",
        _record_maintenance,
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_run_source_ai_schedule",
        _record_ai,
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_run_source_low_priority",
        lambda **kwargs: events.append(f"low:{kwargs['source'].config.name}"),
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_any_inflight",
        lambda **kwargs: kwargs["source"].config.name == "weibo",
    )
    monkeypatch.setattr(
        worker_loop_source_tick,
        "_save_cycle_progress",
        lambda **kwargs: events.append(f"progress:{kwargs['source'].config.name}"),
    )

    loop_ctx = cast(
        worker_loop_runner.WorkerLoopContext,
        SimpleNamespace(
            sources=[
                SimpleNamespace(config=SimpleNamespace(name="weibo", platform="weibo")),
                SimpleNamespace(
                    config=SimpleNamespace(name="xueqiu", platform="xueqiu")
                ),
            ]
        ),
    )

    any_inflight = worker_loop_runner._run_sources_once(
        loop_ctx=loop_ctx,
        worker_interval_seconds=600.0,
        maintenance_next_at=600.0,
        now=0.0,
        do_maintenance=False,
        execs=cast(SourceTickExecutors, SimpleNamespace()),
        state=cast(
            SourceTickState,
            SimpleNamespace(
                wakeup_event=object(),
                inflight_futures=set(),
                inflight_owner_by_future={},
            ),
        ),
    )

    assert any_inflight is True
    assert events == [
        "rss:weibo",
        "rss:xueqiu",
        "maintenance:weibo",
        "maintenance:xueqiu",
        "ai:weibo",
        "low:weibo",
        "progress:weibo",
        "ai:xueqiu",
        "low:xueqiu",
        "progress:xueqiu",
    ]


def test_run_worker_forever_recovers_redis_pending_on_start(monkeypatch) -> None:
    reset_calls: list[tuple[str, bool]] = []

    monkeypatch.setattr(
        worker_loop_runner,
        "redis_ai_reset_consumer_group",
        lambda _client, queue_key, *, verbose: reset_calls.append((queue_key, verbose)),
    )
    monkeypatch.setattr(
        worker_loop_runner, "log_source_runtime", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        worker_loop_runner, "_build_due_ai_cached_by_source", lambda **_kwargs: {}
    )
    monkeypatch.setattr(
        worker_loop_runner, "_run_worker_loop_forever", lambda **_kw: None
    )

    worker_loop_runner.run_worker_forever(
        args=SimpleNamespace(verbose=True, limit=0, ai_stuck_seconds=3600),
        sources=cast(
            list,
            [
                SimpleNamespace(redis_queue_key="queue:a"),
                SimpleNamespace(redis_queue_key="queue:b"),
                SimpleNamespace(redis_queue_key="queue:a"),
            ],
        ),
        config=cast(worker_loop_runner.LLMConfig, object()),
        limiter=RateLimiter(0.0),
        ai_cap=1,
        redis_client=object(),
        rss_active_hours=None,
        rss_interval_seconds=600.0,
        rss_feed_sleep_seconds=0.0,
        worker_active_hours=None,
        worker_interval_seconds=600.0,
    )

    assert reset_calls == [
        ("queue:a", True),
        ("queue:b", True),
    ]


def test_run_worker_forever_skips_recover_without_redis(monkeypatch) -> None:
    reset_calls: list[str] = []

    monkeypatch.setattr(
        worker_loop_runner,
        "redis_ai_reset_consumer_group",
        lambda *_args, **_kwargs: reset_calls.append("called"),
    )
    monkeypatch.setattr(
        worker_loop_runner, "log_source_runtime", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        worker_loop_runner, "_build_due_ai_cached_by_source", lambda **_kwargs: {}
    )
    monkeypatch.setattr(
        worker_loop_runner, "_run_worker_loop_forever", lambda **_kw: None
    )

    worker_loop_runner.run_worker_forever(
        args=SimpleNamespace(verbose=False, limit=0, ai_stuck_seconds=3600),
        sources=cast(list, [SimpleNamespace(redis_queue_key="queue:a")]),
        config=cast(worker_loop_runner.LLMConfig, object()),
        limiter=RateLimiter(0.0),
        ai_cap=1,
        redis_client=None,
        rss_active_hours=None,
        rss_interval_seconds=600.0,
        rss_feed_sleep_seconds=0.0,
        worker_active_hours=None,
        worker_interval_seconds=600.0,
    )

    assert reset_calls == []
