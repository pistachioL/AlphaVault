from __future__ import annotations

from pathlib import Path
from typing import cast

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


def test_redis_ai_ack_and_cleanup_calls_ack_and_spool_delete(
    monkeypatch, tmp_path: Path
) -> None:
    ack_calls: list[tuple[str, str]] = []
    deleted: list[str] = []

    monkeypatch.setattr(
        redis_queue,
        "redis_ai_ack_processing",
        lambda _client, _queue_key, msg: ack_calls.append(("ack", str(msg))),
    )
    monkeypatch.setattr(
        redis_queue,
        "spool_delete",
        lambda _spool_dir, post_uid: deleted.append(str(post_uid)),
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
    assert deleted == ["weibo:1"]


def test_resolve_redis_dedup_ttl_seconds_reads_env(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_DEDUP_TTL_SECONDS", "123")
    assert redis_queue.resolve_redis_dedup_ttl_seconds() == 123


def test_redis_author_recent_push_and_load_roundtrip() -> None:
    _PipelinePayload = tuple[str, str] | tuple[str, int, int] | tuple[str, int]

    class _FakePipeline:
        def __init__(self, parent) -> None:  # type: ignore[no-untyped-def]
            self.parent = parent
            self.ops: list[tuple[str, _PipelinePayload]] = []

        def lpush(self, key: str, msg: str) -> "_FakePipeline":
            self.ops.append(("lpush", (key, msg)))
            return self

        def ltrim(self, key: str, start: int, end: int) -> "_FakePipeline":
            self.ops.append(("ltrim", (key, start, end)))
            return self

        def expire(self, key: str, ttl: int) -> "_FakePipeline":
            self.ops.append(("expire", (key, ttl)))
            return self

        def execute(self) -> list[int]:
            for op, payload in self.ops:
                if op == "lpush":
                    key, msg = cast(tuple[str, str], payload)
                    self.parent.lpush(key, msg)
                elif op == "ltrim":
                    key, start, end = cast(tuple[str, int, int], payload)
                    self.parent.ltrim(key, start, end)
                elif op == "expire":
                    key, ttl = cast(tuple[str, int], payload)
                    self.parent.expire(key, ttl)
            return [1] * len(self.ops)

    class _FakeClient:
        def __init__(self) -> None:
            self.data: dict[str, list[str]] = {}

        def pipeline(self) -> _FakePipeline:
            return _FakePipeline(self)

        def lpush(self, key: str, msg: str) -> int:
            self.data.setdefault(key, [])
            self.data[key].insert(0, msg)
            return len(self.data[key])

        def ltrim(self, key: str, start: int, end: int) -> None:
            rows = self.data.get(key, [])
            self.data[key] = rows[start : end + 1]

        def expire(self, key: str, ttl: int) -> bool:
            del key, ttl
            return True

        def lrange(self, key: str, start: int, end: int) -> list[str]:
            rows = self.data.get(key, [])
            if end < 0:
                end = len(rows) - 1
            return list(rows[start : end + 1])

    client = _FakeClient()
    ok = redis_queue.redis_author_recent_push(
        client,
        "test:q",
        payload={"author": "作者A", "post_uid": "weibo:1"},
        ttl_seconds=600,
        max_items=5,
    )
    assert ok is True
    rows = redis_queue.redis_author_recent_load(
        client,
        "test:q",
        author="作者A",
        limit=5,
    )
    assert len(rows) == 1
    assert rows[0]["post_uid"] == "weibo:1"


def test_redis_author_recent_empty_marker_avoids_false_miss() -> None:
    class _FakePipeline:
        def __init__(self, parent) -> None:  # type: ignore[no-untyped-def]
            self.parent = parent
            self.ops: list[tuple[str, object]] = []

        def lpush(self, key: str, msg: str) -> "_FakePipeline":
            self.ops.append(("lpush", (key, msg)))
            return self

        def ltrim(self, key: str, start: int, end: int) -> "_FakePipeline":
            self.ops.append(("ltrim", (key, start, end)))
            return self

        def execute(self) -> list[int]:
            for op, payload in self.ops:
                if op == "lpush":
                    key, msg = cast(tuple[str, str], payload)
                    self.parent.lpush(key, msg)
                elif op == "ltrim":
                    key, start, end = cast(tuple[str, int, int], payload)
                    self.parent.ltrim(key, start, end)
            return [1] * len(self.ops)

    class _FakeClient:
        def __init__(self) -> None:
            self.kv: dict[str, str] = {}
            self.lists: dict[str, list[str]] = {}

        def pipeline(self) -> _FakePipeline:
            return _FakePipeline(self)

        def set(self, key: str, value: str, ex: int | None = None) -> bool:
            del ex
            self.kv[key] = value
            return True

        def exists(self, key: str) -> int:
            return 1 if (key in self.kv or key in self.lists) else 0

        def delete(self, key: str) -> int:
            removed = 0
            if key in self.kv:
                del self.kv[key]
                removed += 1
            if key in self.lists:
                del self.lists[key]
                removed += 1
            return removed

        def lrange(self, key: str, start: int, end: int) -> list[str]:
            rows = self.lists.get(key, [])
            if end < 0:
                end = len(rows) - 1
            return list(rows[start : end + 1])

        def lpush(self, key: str, msg: str) -> int:
            self.lists.setdefault(key, [])
            self.lists[key].insert(0, msg)
            return len(self.lists[key])

        def ltrim(self, key: str, start: int, end: int) -> None:
            rows = self.lists.get(key, [])
            self.lists[key] = rows[start : end + 1]

        def expire(self, key: str, ttl: int) -> bool:
            del key, ttl
            return True

    client = _FakeClient()
    marked = redis_queue.redis_author_recent_mark_empty(
        client,
        "test:q",
        author="作者A",
        ttl_seconds=600,
    )
    assert marked is True
    assert (
        redis_queue.redis_author_recent_is_marked_empty(
            client,
            "test:q",
            author="作者A",
        )
        is True
    )
    assert (
        redis_queue.redis_author_recent_load(
            client,
            "test:q",
            author="作者A",
            limit=50,
        )
        == []
    )

    push_ok = redis_queue.redis_author_recent_push(
        client,
        "test:q",
        payload={"author": "作者A", "post_uid": "weibo:1"},
        ttl_seconds=600,
        max_items=5,
    )
    assert push_ok is True
    rows = redis_queue.redis_author_recent_load(
        client,
        "test:q",
        author="作者A",
        limit=5,
    )
    assert len(rows) == 1
    assert rows[0]["post_uid"] == "weibo:1"
