from __future__ import annotations

from types import SimpleNamespace

from alphavault.worker import maintenance as maintenance_module


def _source_runtime(
    *, queue_key: str = "queue", next_at: float = 0.0, empty_checks: int = 0
):
    return SimpleNamespace(
        redis_queue_key=queue_key,
        redis_due_maintenance_next_at=float(next_at),
        redis_due_maintenance_empty_checks=int(empty_checks),
    )


def test_run_source_db_maintenance_runs_retry_and_claim_steps() -> None:
    call_order: list[str] = []

    def _move_due(*_args, **_kwargs) -> int:
        call_order.append("move_due")
        return 2

    def _claim_stuck(*_args, **_kwargs) -> list[dict[str, object]]:
        call_order.append("claim_stuck")
        return [
            {"message_id": "1-0", "payload": '{"post_uid":"weibo:1"}'},
            {"message_id": "2-0", "payload": '{"post_uid":"weibo:2"}'},
        ]

    recovered, flushed_redis, has_error = maintenance_module.run_source_db_maintenance(
        engine=object(),
        redis_client=object(),
        redis_queue_key="queue",
        now_fn=lambda: 100.0,
        move_due_retry_to_stream_fn=_move_due,
        claim_stuck_messages_fn=_claim_stuck,
        fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        redis_ai_requeue_max_items=200,
        claim_min_idle_ms=1000,
        consumer_name="weibo:worker",
    )

    assert call_order == ["move_due", "claim_stuck"]
    assert recovered == 2
    assert flushed_redis == 2
    assert has_error is False


def test_run_source_db_maintenance_keeps_other_step_running_after_nonfatal_error() -> (
    None
):
    def _raise_move(*_args, **_kwargs) -> int:
        raise RuntimeError("redis unavailable")

    recovered, flushed_redis, has_error = maintenance_module.run_source_db_maintenance(
        engine=object(),
        redis_client=object(),
        redis_queue_key="queue",
        now_fn=lambda: 100.0,
        move_due_retry_to_stream_fn=_raise_move,
        claim_stuck_messages_fn=lambda *_args, **_kwargs: [
            {"message_id": "1-0", "payload": '{"post_uid":"weibo:1"}'}
        ],
        fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        redis_ai_requeue_max_items=200,
        claim_min_idle_ms=1000,
        consumer_name="weibo:worker",
    )

    assert recovered == 1
    assert flushed_redis == 0
    assert has_error is True


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
        now_fn=lambda: 100.0,
        move_due_retry_to_stream_fn=_move_due,
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
        now_fn=lambda: 100.0,
        move_due_retry_to_stream_fn=_move_due,
        fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        max_items=200,
    )
    assert moved == 2
    assert has_error is False
    assert source.redis_due_maintenance_empty_checks == 0
    assert source.redis_due_maintenance_next_at == 100.0
