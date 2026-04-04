from __future__ import annotations

from types import SimpleNamespace

from alphavault.worker import maintenance as maintenance_module
from alphavault.worker import redis_queue as redis_queue_module


def _source_runtime(
    *,
    queue_key: str = "queue",
    next_at: float = 0.0,
    empty_checks: int = 0,
):
    return SimpleNamespace(
        redis_queue_key=queue_key,
        redis_due_maintenance_next_at=float(next_at),
        redis_due_maintenance_empty_checks=int(empty_checks),
        maintenance_recovery_cycle_count=0,
        maintenance_recovery_force_next=False,
    )


def test_run_turso_maintenance_skips_recovery_and_uses_injected_redis_steps() -> None:
    source_engine = object()
    recovered, flushed_redis, has_error = maintenance_module.run_turso_maintenance(
        engine=source_engine,
        platform="weibo",
        spool_dir="unused",
        redis_client=object(),
        redis_queue_key="queue",
        stuck_seconds=3600,
        verbose=False,
        do_recovery=False,
        now_fn=lambda: 100.0,
        recover_spool_to_turso_and_redis_fn=lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("recovery should be skipped")
        ),
        recover_stuck_ai_tasks_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("recovery should be skipped")
        ),
        recover_done_without_processed_at_fn=lambda *_args, **_kwargs: (
            _ for _ in ()
        ).throw(AssertionError("recovery should be skipped")),
        load_unprocessed_posts_for_requeue_fn=lambda *_args, **_kwargs: (
            _ for _ in ()
        ).throw(AssertionError("recovery should be skipped")),
        redis_try_push_ai_dedup_status_fn=lambda *_args, **_kwargs: (
            _ for _ in ()
        ).throw(AssertionError("recovery should be skipped")),
        resolve_redis_dedup_ttl_seconds_fn=lambda: 123,
        maybe_dispose_turso_engine_on_transient_error_fn=lambda **_kwargs: None,
        redis_ai_requeue_processing_fn=lambda *_args, **_kwargs: 1,
        fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        redis_ai_requeue_max_items=200,
    )

    assert recovered == 0
    assert flushed_redis == 1
    assert has_error is False


def test_run_turso_maintenance_recovers_spool_before_requeueing_posts() -> None:
    call_order: list[str] = []

    def _fake_recover_spool(**_kwargs):
        call_order.append("spool")
        return 1, 1, 0, False

    def _fake_recover_running(*_args, **_kwargs) -> int:
        call_order.append("recover_running")
        return 2

    def _fake_recover_done(*_args, **_kwargs) -> int:
        call_order.append("recover_done")
        return 3

    def _fake_load_posts(*_args, **_kwargs) -> list[dict[str, object]]:
        call_order.append("load_posts")
        return [
            {
                "post_uid": "weibo:1",
                "platform": "weibo",
                "platform_post_id": "1",
                "author": "作者A",
                "created_at": "2026-04-04 10:00:00",
                "url": "https://example.com/1",
                "raw_text": "文本1",
                "display_md": "文本1",
                "ai_retry_count": 0,
            }
        ]

    def _fake_push_posts(*_args, **_kwargs) -> str:
        call_order.append("push_posts")
        return redis_queue_module.REDIS_PUSH_STATUS_PUSHED

    def _fake_requeue_processing(*_args, **_kwargs) -> int:
        call_order.append("requeue_processing")
        return 4

    recovered, flushed_redis, has_error = maintenance_module.run_turso_maintenance(
        engine=object(),
        platform="weibo",
        spool_dir="unused",
        redis_client=object(),
        redis_queue_key="queue",
        stuck_seconds=3600,
        verbose=False,
        do_recovery=True,
        now_fn=lambda: 100.0,
        recover_spool_to_turso_and_redis_fn=_fake_recover_spool,
        recover_stuck_ai_tasks_fn=_fake_recover_running,
        recover_done_without_processed_at_fn=_fake_recover_done,
        load_unprocessed_posts_for_requeue_fn=_fake_load_posts,
        redis_try_push_ai_dedup_status_fn=_fake_push_posts,
        resolve_redis_dedup_ttl_seconds_fn=lambda: 123,
        maybe_dispose_turso_engine_on_transient_error_fn=lambda **_kwargs: None,
        redis_ai_requeue_processing_fn=_fake_requeue_processing,
        fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        redis_ai_requeue_max_items=200,
    )

    assert call_order == [
        "spool",
        "recover_running",
        "recover_done",
        "requeue_processing",
        "load_posts",
        "push_posts",
    ]
    assert recovered == 6
    assert flushed_redis == 6
    assert has_error is False


def test_maybe_run_redis_due_maintenance_uses_backoff_and_resets_on_success() -> None:
    source = _source_runtime(next_at=0.0, empty_checks=0)
    moved_calls = {"count": 0}

    def _move_due(*_args, **_kwargs) -> int:
        moved_calls["count"] += 1
        return 0 if moved_calls["count"] == 1 else 2

    moved, has_error = maintenance_module.maybe_run_redis_due_maintenance(
        source=source,
        redis_client=object(),
        worker_interval_seconds=90.0,
        verbose=False,
        now_fn=lambda: 100.0,
        move_due_delayed_to_ready_fn=_move_due,
        fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        max_items=200,
    )
    assert moved == 0
    assert has_error is False
    assert source.redis_due_maintenance_empty_checks == 1
    assert source.redis_due_maintenance_next_at == 130.0

    source.redis_due_maintenance_next_at = 100.0
    moved, has_error = maintenance_module.maybe_run_redis_due_maintenance(
        source=source,
        redis_client=object(),
        worker_interval_seconds=90.0,
        verbose=False,
        now_fn=lambda: 100.0,
        move_due_delayed_to_ready_fn=_move_due,
        fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        max_items=200,
    )
    assert moved == 2
    assert has_error is False
    assert source.redis_due_maintenance_empty_checks == 0
    assert source.redis_due_maintenance_next_at == 100.0
