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


def test_redis_ai_ack_and_cleanup_acks_processing_and_deletes_spool(
    monkeypatch, tmp_path: Path
) -> None:
    ack_calls: list[tuple[str, str]] = []
    spool_file = tmp_path / f"{redis_queue.sha1_short('weibo:1')}.json"
    spool_file.write_text("{}", encoding="utf-8")

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
    assert spool_file.exists() is False


def test_redis_ai_try_claim_and_release_lease_uses_token() -> None:
    class _FakeClient:
        def __init__(self) -> None:
            self.values: dict[str, str] = {}
            self.eval_calls: list[tuple[str, int, str, str]] = []

        def set(self, key: str, value: str, *, nx: bool, ex: int) -> bool:
            assert nx is True
            assert ex == 120
            if key in self.values:
                return False
            self.values[key] = value
            return True

        def eval(self, script: str, numkeys: int, key: str, token: str) -> int:
            self.eval_calls.append((script, numkeys, key, token))
            if self.values.get(key) != token:
                return 0
            self.values.pop(key, None)
            return 1

    client = _FakeClient()
    token = redis_queue.redis_ai_try_claim_lease(
        client,
        "test:q",
        post_uid="weibo:1",
        lease_seconds=120,
    )
    assert token != ""
    assert client.values["test:q:lease:ai:weibo:1"] == token
    assert (
        redis_queue.redis_ai_release_lease(
            client,
            "test:q",
            post_uid="weibo:1",
            lease_token=token,
            verbose=False,
        )
        is True
    )
    assert len(client.eval_calls) == 1
    assert client.eval_calls[0][1:] == (1, "test:q:lease:ai:weibo:1", token)
    assert client.values == {}


def test_redis_ai_requeue_processing_only_moves_messages_without_lease() -> None:
    lease_msg = '{"post_uid":"weibo:1"}'
    stale_msg = '{"post_uid":"weibo:2"}'

    class _FakeClient:
        def __init__(self) -> None:
            self.processing = [lease_msg, stale_msg]
            self.ready: list[str] = []
            self.leases = {"test:q:lease:ai:weibo:1": "lease-token"}
            self.lrange_calls: list[tuple[str, int, int]] = []

        def llen(self, key: str) -> int:
            assert key.endswith(":ai:processing")
            return len(self.processing)

        def lrange(self, key: str, start: int, end: int) -> list[str]:
            assert key.endswith(":ai:processing")
            self.lrange_calls.append((key, start, end))
            return list(self.processing[start : end + 1])

        def exists(self, key: str) -> int:
            return 1 if key in self.leases else 0

        def lrem(self, key: str, count: int, msg: str) -> int:
            assert key.endswith(":ai:processing")
            assert count == 1
            try:
                self.processing.remove(msg)
            except ValueError:
                return 0
            return 1

        def lpush(self, key: str, msg: str) -> int:
            assert key.endswith(":ai:ready")
            self.ready.insert(0, msg)
            return len(self.ready)

    client = _FakeClient()
    moved = redis_queue.redis_ai_requeue_processing_without_lease(
        client,
        "test:q",
        max_items=10,
        verbose=False,
    )
    assert moved == 1
    assert client.processing == [lease_msg]
    assert client.ready == [stale_msg]
    assert client.lrange_calls == [("test:q:ai:processing", 0, 1)]


def test_redis_ai_requeue_processing_checks_oldest_messages_not_just_head() -> None:
    head_with_lease = '{"post_uid":"weibo:new"}'
    older_without_lease = '{"post_uid":"weibo:old"}'
    scan_batch_size = redis_queue.REDIS_AI_REQUEUE_SCAN_BATCH_SIZE
    tail_with_lease = [
        f'{{"post_uid":"weibo:lease:{idx}"}}' for idx in range(scan_batch_size)
    ]

    class _FakeClient:
        def __init__(self) -> None:
            self.processing = [head_with_lease, older_without_lease, *tail_with_lease]
            self.ready: list[str] = []
            self.leases = {
                "test:q:lease:ai:weibo:new": "lease-token",
                **{
                    f"test:q:lease:ai:weibo:lease:{idx}": "lease-token"
                    for idx in range(scan_batch_size)
                },
            }
            self.lrange_calls: list[tuple[str, int, int]] = []

        def llen(self, key: str) -> int:
            assert key.endswith(":ai:processing")
            return len(self.processing)

        def lrange(self, key: str, start: int, end: int) -> list[str]:
            assert key.endswith(":ai:processing")
            self.lrange_calls.append((key, start, end))
            return list(self.processing[start : end + 1])

        def exists(self, key: str) -> int:
            return 1 if key in self.leases else 0

        def lrem(self, key: str, count: int, msg: str) -> int:
            assert key.endswith(":ai:processing")
            assert count == 1
            try:
                self.processing.remove(msg)
            except ValueError:
                return 0
            return 1

        def lpush(self, key: str, msg: str) -> int:
            assert key.endswith(":ai:ready")
            self.ready.insert(0, msg)
            return len(self.ready)

    client = _FakeClient()
    moved = redis_queue.redis_ai_requeue_processing_without_lease(
        client,
        "test:q",
        max_items=1,
        verbose=False,
    )
    assert moved == 1
    assert older_without_lease not in client.processing
    assert client.ready == [older_without_lease]
    assert client.lrange_calls == [
        ("test:q:ai:processing", 2, scan_batch_size + 1),
        ("test:q:ai:processing", 0, 1),
    ]


def test_redis_ai_requeue_processing_keeps_scan_batch_size_fixed() -> None:
    scan_batch_size = redis_queue.REDIS_AI_REQUEUE_SCAN_BATCH_SIZE
    extra_count = 50
    max_items = scan_batch_size + 10
    processing = [
        f'{{"post_uid":"weibo:{idx}"}}' for idx in range(scan_batch_size + extra_count)
    ]

    class _FakeClient:
        def __init__(self) -> None:
            self.processing = list(processing)
            self.ready: list[str] = []
            self.leases = {
                f"test:q:lease:ai:weibo:{idx}": "lease-token"
                for idx in range(scan_batch_size + extra_count)
            }
            self.lrange_calls: list[tuple[str, int, int]] = []

        def llen(self, key: str) -> int:
            assert key.endswith(":ai:processing")
            return len(self.processing)

        def lrange(self, key: str, start: int, end: int) -> list[str]:
            assert key.endswith(":ai:processing")
            self.lrange_calls.append((key, start, end))
            return list(self.processing[start : end + 1])

        def exists(self, key: str) -> int:
            return 1 if key in self.leases else 0

        def lrem(self, key: str, count: int, msg: str) -> int:
            del key, count, msg
            raise AssertionError("all messages still have lease, should not lrem")

        def lpush(self, key: str, msg: str) -> int:
            del key, msg
            raise AssertionError("all messages still have lease, should not lpush")

    client = _FakeClient()
    moved = redis_queue.redis_ai_requeue_processing_without_lease(
        client,
        "test:q",
        max_items=max_items,
        verbose=False,
    )
    assert moved == 0
    assert client.lrange_calls[0] == (
        "test:q:ai:processing",
        extra_count,
        scan_batch_size + extra_count - 1,
    )


def test_resolve_redis_dedup_ttl_seconds_reads_env_on_module_load(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DEDUP_TTL_SECONDS", "123")
    reloaded = importlib.reload(redis_queue)
    assert reloaded.resolve_redis_dedup_ttl_seconds() == 123
    monkeypatch.delenv("REDIS_DEDUP_TTL_SECONDS", raising=False)
    importlib.reload(redis_queue)
