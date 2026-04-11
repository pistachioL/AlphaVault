from __future__ import annotations

from alphavault.worker import redis_stream_queue


def test_redis_try_push_ai_message_status_dedups_and_pushes() -> None:
    class _FakeClient:
        def __init__(self) -> None:
            self.set_calls: list[tuple[str, str, bool, int]] = []
            self.xadd_calls: list[tuple[str, dict[str, str], int | None, bool]] = []
            self.xlen_calls: list[str] = []
            self.zcard_calls: list[str] = []

        def xgroup_create(
            self, stream: str, group: str, id: str = "0", mkstream: bool = False
        ) -> bool:
            del stream, group, id, mkstream
            return True

        def xlen(self, key: str) -> int:
            self.xlen_calls.append(key)
            return 0

        def zcard(self, key: str) -> int:
            self.zcard_calls.append(key)
            return 0

        def set(self, key: str, value: str, *, nx: bool, ex: int) -> bool:
            self.set_calls.append((key, value, nx, ex))
            return True

        def xadd(
            self,
            key: str,
            fields: dict[str, str],
            *,
            maxlen: int | None = None,
            approximate: bool = True,
        ) -> str:
            self.xadd_calls.append((key, dict(fields), maxlen, approximate))
            return "1-0"

        def delete(self, key: str) -> int:
            del key
            return 1

    client = _FakeClient()
    status = redis_stream_queue.redis_try_push_ai_message_status(
        client,
        "queue",
        post_uid="weibo:1",
        payload={"post_uid": "weibo:1", "platform": "weibo"},
        ttl_seconds=123,
        queue_maxlen=999,
        verbose=False,
    )

    assert status == redis_stream_queue.REDIS_PUSH_STATUS_PUSHED
    assert client.set_calls[0][0].startswith("queue:dedup:")
    assert client.set_calls[0][2:] == (True, 123)
    assert client.xadd_calls == [
        (
            "queue:ai:stream",
            {
                "payload": '{"post_uid": "weibo:1", "platform": "weibo"}',
                "post_uid": "weibo:1",
            },
            None,
            True,
        )
    ]
    assert client.xlen_calls == ["queue:ai:stream"]
    assert client.zcard_calls == ["queue:ai:retry"]


def test_redis_try_push_ai_message_status_returns_error_when_backlog_is_full() -> None:
    class _FakeClient:
        def __init__(self) -> None:
            self.deleted_keys: list[str] = []
            self.xadd_called = False

        def set(self, key: str, value: str, *, nx: bool, ex: int) -> bool:
            del key, value, nx, ex
            return True

        def xlen(self, key: str) -> int:
            assert key == "queue:ai:stream"
            return 4

        def zcard(self, key: str) -> int:
            assert key == "queue:ai:retry"
            return 1

        def delete(self, key: str) -> int:
            self.deleted_keys.append(key)
            return 1

        def xgroup_create(
            self, stream: str, group: str, id: str = "0", mkstream: bool = False
        ) -> bool:
            del stream, group, id, mkstream
            return True

        def xadd(
            self,
            key: str,
            fields: dict[str, str],
            *,
            maxlen: int | None = None,
            approximate: bool = True,
        ) -> str:
            del key, fields, maxlen, approximate
            self.xadd_called = True
            return "1-0"

    client = _FakeClient()
    status = redis_stream_queue.redis_try_push_ai_message_status(
        client,
        "queue",
        post_uid="weibo:1",
        payload={"post_uid": "weibo:1"},
        ttl_seconds=123,
        queue_maxlen=5,
        verbose=False,
    )

    assert status == redis_stream_queue.REDIS_PUSH_STATUS_ERROR
    assert client.xadd_called is False
    assert client.deleted_keys == [
        redis_stream_queue._redis_dedup_key("queue", "weibo:1")
    ]


def test_redis_ai_read_group_messages_creates_group_and_reads_payloads() -> None:
    class _FakeClient:
        def __init__(self) -> None:
            self.group_calls: list[tuple[str, str, str, bool]] = []

        def xgroup_create(
            self, stream: str, group: str, id: str = "0", mkstream: bool = False
        ) -> bool:
            self.group_calls.append((stream, group, id, mkstream))
            return True

        def xreadgroup(
            self,
            groupname: str,
            consumername: str,
            streams: dict[str, str],
            count: int,
            block: int | None = None,
        ) -> list[list[object]]:
            del block
            assert groupname == redis_stream_queue.REDIS_AI_CONSUMER_GROUP
            assert consumername == "weibo:worker"
            assert streams == {"queue:ai:stream": ">"}
            assert count == 2
            return [
                [
                    "queue:ai:stream",
                    [
                        (
                            "1-0",
                            {
                                "payload": '{"post_uid":"weibo:1"}',
                                "post_uid": "weibo:1",
                            },
                        )
                    ],
                ]
            ]

    client = _FakeClient()
    messages = redis_stream_queue.redis_ai_read_group_messages(
        client,
        "queue",
        consumer_name="weibo:worker",
        count=2,
    )

    assert client.group_calls == [
        (
            "queue:ai:stream",
            redis_stream_queue.REDIS_AI_CONSUMER_GROUP,
            "0",
            True,
        )
    ]
    assert messages == [{"message_id": "1-0", "payload": '{"post_uid":"weibo:1"}'}]


def test_redis_ai_read_group_messages_fails_fast_on_bad_fields_shape() -> None:
    class _FakeClient:
        def xgroup_create(
            self, stream: str, group: str, id: str = "0", mkstream: bool = False
        ) -> bool:
            del stream, group, id, mkstream
            return True

        def xreadgroup(
            self,
            groupname: str,
            consumername: str,
            streams: dict[str, str],
            count: int,
            block: int | None = None,
        ) -> list[list[object]]:
            del groupname, consumername, streams, count, block
            return [["queue:ai:stream", [["1-0", ["bad-fields"]]]]]

    try:
        redis_stream_queue.redis_ai_read_group_messages(
            _FakeClient(),
            "queue",
            consumer_name="weibo:worker",
            count=1,
        )
    except RuntimeError as err:
        assert "unexpected_redis_stream_shape" in str(err)
        assert "stage=stream_fields" in str(err)
    else:
        raise AssertionError("expected RuntimeError")


def test_redis_ai_claim_stuck_messages_reads_xautoclaim_result() -> None:
    class _FakeClient:
        def xgroup_create(
            self, stream: str, group: str, id: str = "0", mkstream: bool = False
        ) -> bool:
            del stream, group, id, mkstream
            return True

        def xautoclaim(
            self,
            key: str,
            groupname: str,
            consumername: str,
            min_idle_time: int,
            start_id: str = "0-0",
            count: int | None = None,
        ) -> tuple[str, list[tuple[str, dict[str, str]]], list[str]]:
            assert key == "queue:ai:stream"
            assert groupname == redis_stream_queue.REDIS_AI_CONSUMER_GROUP
            assert consumername == "weibo:worker"
            assert min_idle_time == 1000
            assert start_id == "0-0"
            assert count == 3
            return (
                "0-0",
                [("9-0", {"payload": '{"post_uid":"weibo:9"}'})],
                [],
            )

    messages = redis_stream_queue.redis_ai_claim_stuck_messages(
        _FakeClient(),
        "queue",
        consumer_name="weibo:worker",
        min_idle_ms=1000,
        count=3,
    )

    assert messages == [{"message_id": "9-0", "payload": '{"post_uid":"weibo:9"}'}]


def test_redis_ai_move_due_retries_to_stream_moves_messages() -> None:
    class _FakeClient:
        def __init__(self) -> None:
            self.removed: list[str] = []
            self.added: list[str] = []

        def xgroup_create(
            self, stream: str, group: str, id: str = "0", mkstream: bool = False
        ) -> bool:
            del stream, group, id, mkstream
            return True

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
            assert key == "queue:ai:retry"
            return ['{"post_uid":"weibo:1"}', '{"post_uid":"weibo:2"}']

        def zrem(self, key: str, member: str) -> int:
            assert key == "queue:ai:retry"
            self.removed.append(member)
            return 1

        def xadd(
            self,
            key: str,
            fields: dict[str, str],
            *,
            maxlen: int | None = None,
            approximate: bool = True,
        ) -> str:
            assert maxlen is None
            del approximate
            assert key == "queue:ai:stream"
            self.added.append(str(fields["payload"]))
            return "1-0"

    client = _FakeClient()
    moved = redis_stream_queue.redis_ai_move_due_retries_to_stream(
        client,
        "queue",
        now_epoch=123,
        max_items=10,
        verbose=False,
    )

    assert moved == 2
    assert client.removed == ['{"post_uid":"weibo:1"}', '{"post_uid":"weibo:2"}']
    assert client.added == ['{"post_uid":"weibo:1"}', '{"post_uid":"weibo:2"}']


def test_redis_ai_ack_and_push_retry_runs_in_one_transaction() -> None:
    class _FakePipeline:
        def __init__(self) -> None:
            self.ops: list[tuple[str, object, object]] = []

        def zadd(self, key: str, mapping: dict[str, int]) -> "_FakePipeline":
            self.ops.append(("zadd", key, dict(mapping)))
            return self

        def xack(self, key: str, group: str, message_id: str) -> "_FakePipeline":
            self.ops.append(("xack", key, (group, message_id)))
            return self

        def xdel(self, key: str, message_id: str) -> "_FakePipeline":
            self.ops.append(("xdel", key, message_id))
            return self

        def execute(self) -> list[int]:
            self.ops.append(("execute", None, None))
            return [1, 1, 1]

    class _FakeClient:
        def __init__(self) -> None:
            self.transaction_calls: list[bool] = []
            self.pipeline_obj = _FakePipeline()

        def pipeline(self, transaction: bool = True) -> _FakePipeline:
            self.transaction_calls.append(bool(transaction))
            return self.pipeline_obj

    client = _FakeClient()
    redis_stream_queue.redis_ai_ack_and_push_retry(
        client,
        "queue",
        message_id="2-0",
        payload={"post_uid": "weibo:retry", "retry_count": 1, "next_retry_at": 130},
        next_retry_at=130,
    )

    assert client.transaction_calls == [True]
    assert client.pipeline_obj.ops == [
        (
            "zadd",
            "queue:ai:retry",
            {
                '{"post_uid": "weibo:retry", "retry_count": 1, "next_retry_at": 130}': 130
            },
        ),
        (
            "xack",
            "queue:ai:stream",
            (redis_stream_queue.REDIS_AI_CONSUMER_GROUP, "2-0"),
        ),
        ("xdel", "queue:ai:stream", "2-0"),
        ("execute", None, None),
    ]


def test_redis_ai_ack_runs_in_one_transaction_and_deletes_message() -> None:
    class _FakePipeline:
        def __init__(self) -> None:
            self.ops: list[tuple[str, object, object]] = []

        def xack(self, key: str, group: str, message_id: str) -> "_FakePipeline":
            self.ops.append(("xack", key, (group, message_id)))
            return self

        def xdel(self, key: str, message_id: str) -> "_FakePipeline":
            self.ops.append(("xdel", key, message_id))
            return self

        def execute(self) -> list[int]:
            self.ops.append(("execute", None, None))
            return [1, 1]

    class _FakeClient:
        def __init__(self) -> None:
            self.transaction_calls: list[bool] = []
            self.pipeline_obj = _FakePipeline()

        def pipeline(self, transaction: bool = True) -> _FakePipeline:
            self.transaction_calls.append(bool(transaction))
            return self.pipeline_obj

    client = _FakeClient()
    redis_stream_queue.redis_ai_ack(client, "queue", "7-0")

    assert client.transaction_calls == [True]
    assert client.pipeline_obj.ops == [
        (
            "xack",
            "queue:ai:stream",
            (redis_stream_queue.REDIS_AI_CONSUMER_GROUP, "7-0"),
        ),
        ("xdel", "queue:ai:stream", "7-0"),
        ("execute", None, None),
    ]


def test_redis_ai_ack_and_clear_dedup_runs_in_one_transaction() -> None:
    class _FakePipeline:
        def __init__(self) -> None:
            self.ops: list[tuple[str, object, object]] = []

        def xack(self, key: str, group: str, message_id: str) -> "_FakePipeline":
            self.ops.append(("xack", key, (group, message_id)))
            return self

        def xdel(self, key: str, message_id: str) -> "_FakePipeline":
            self.ops.append(("xdel", key, message_id))
            return self

        def delete(self, key: str) -> "_FakePipeline":
            self.ops.append(("delete", key, None))
            return self

        def execute(self) -> list[int]:
            self.ops.append(("execute", None, None))
            return [1, 1, 1]

    class _FakeClient:
        def __init__(self) -> None:
            self.transaction_calls: list[bool] = []
            self.pipeline_obj = _FakePipeline()

        def pipeline(self, transaction: bool = True) -> _FakePipeline:
            self.transaction_calls.append(bool(transaction))
            return self.pipeline_obj

    client = _FakeClient()
    redis_stream_queue.redis_ai_ack_and_clear_dedup(
        client,
        "queue",
        message_id="5-0",
        post_uid="weibo:5",
    )

    assert client.transaction_calls == [True]
    assert client.pipeline_obj.ops == [
        (
            "xack",
            "queue:ai:stream",
            (redis_stream_queue.REDIS_AI_CONSUMER_GROUP, "5-0"),
        ),
        ("xdel", "queue:ai:stream", "5-0"),
        ("delete", redis_stream_queue._redis_dedup_key("queue", "weibo:5"), None),
        ("execute", None, None),
    ]


def test_redis_ai_pressure_snapshot_counts_pending_unread_and_retry() -> None:
    class _FakeClient:
        def xgroup_create(
            self, stream: str, group: str, id: str = "0", mkstream: bool = False
        ) -> bool:
            del stream, group, id, mkstream
            return True

        def xpending(self, key: str, groupname: str) -> dict[str, int]:
            assert key == "queue:ai:stream"
            assert groupname == redis_stream_queue.REDIS_AI_CONSUMER_GROUP
            return {"pending": 3}

        def xinfo_groups(self, key: str) -> list[dict[str, object]]:
            assert key == "queue:ai:stream"
            return [
                {
                    "name": redis_stream_queue.REDIS_AI_CONSUMER_GROUP,
                    "lag": 5,
                }
            ]

        def zcard(self, key: str) -> int:
            assert key == "queue:ai:retry"
            return 2

    snapshot = redis_stream_queue.redis_ai_pressure_snapshot(_FakeClient(), "queue")

    assert snapshot == {
        "pending_count": 3,
        "unread_count": 5,
        "retry_count": 2,
        "total_backlog": 10,
    }
