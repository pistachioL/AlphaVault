from __future__ import annotations

import importlib
from pathlib import Path

from alphavault.worker import redis_queue


def test_default_redis_dedup_ttl_is_7_days() -> None:
    assert redis_queue.DEFAULT_REDIS_DEDUP_TTL_SECONDS == 7 * 24 * 3600


def test_redis_ai_due_count_sums_ready_and_due_delayed() -> None:
    class _FakeClient:
        def llen(self, key: str) -> int:
            assert key.endswith(":ai:ready")
            return 3

        def zcount(self, key: str, min_score: str, max_score: int) -> int:
            del min_score, max_score
            assert key.endswith(":ai:delayed")
            return 2

    count = redis_queue.redis_ai_due_count(
        _FakeClient(),
        "test:q",
        now_epoch=123,
    )
    assert count == 5


def test_redis_ai_move_due_delayed_to_ready_moves_messages() -> None:
    class _FakePipeline:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str | int]] = []

        def zrem(self, key: str, msg: str) -> "_FakePipeline":
            self.calls.append(("zrem", key, msg))
            return self

        def lpush(self, key: str, msg: str) -> "_FakePipeline":
            self.calls.append(("lpush", key, msg))
            return self

        def ltrim(self, key: str, start: int, end: int) -> "_FakePipeline":
            self.calls.append(("ltrim", key, f"{start}:{end}"))
            return self

        def execute(self) -> list[int]:
            return [1] * len(self.calls)

    class _FakeClient:
        def __init__(self) -> None:
            self.pipeline_instance = _FakePipeline()

        def zrangebyscore(
            self,
            key: str,
            *,
            min: str,
            max: int,
            start: int,
            num: int,
        ) -> list[str]:
            del min, max, start, num
            assert key.endswith(":ai:delayed")
            return ["m1", "m2"]

        def pipeline(self) -> _FakePipeline:
            return self.pipeline_instance

    client = _FakeClient()
    moved = redis_queue.redis_ai_move_due_delayed_to_ready(
        client,
        "test:q",
        now_epoch=123,
        max_items=10,
        verbose=False,
    )
    assert moved == 2
    assert client.pipeline_instance.calls == [
        ("zrem", "test:q:ai:delayed", "m1"),
        ("lpush", "test:q:ai:ready", "m1"),
        ("zrem", "test:q:ai:delayed", "m2"),
        ("lpush", "test:q:ai:ready", "m2"),
        ("ltrim", "test:q:ai:ready", "0:19999"),
    ]


def test_redis_ai_ack_and_cleanup_only_acks_processing(
    monkeypatch, tmp_path: Path
) -> None:
    ack_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        redis_queue,
        "redis_ai_ack_processing",
        lambda _client, _queue_key, msg: ack_calls.append(("ack", str(msg))),
    )

    ok = redis_queue.redis_ai_ack_and_cleanup(
        object(),
        "test:q",
        msg="msg-1",
        post_uid="weibo:1",
        spool_dir=tmp_path,
        verbose=False,
    )
    assert ok is True
    assert ack_calls == [("ack", "msg-1")]


def test_resolve_redis_dedup_ttl_seconds_reads_env_on_module_load(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DEDUP_TTL_SECONDS", "123")
    reloaded = importlib.reload(redis_queue)
    assert reloaded.resolve_redis_dedup_ttl_seconds() == 123
    monkeypatch.delenv("REDIS_DEDUP_TTL_SECONDS", raising=False)
    importlib.reload(redis_queue)
