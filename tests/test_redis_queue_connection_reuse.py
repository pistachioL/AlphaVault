from __future__ import annotations

import json
from typing import cast

from alphavault.db.turso_db import TursoEngine
from alphavault.worker import redis_queue


def test_flush_redis_to_turso_reuses_single_connection(monkeypatch, tmp_path) -> None:
    engine_marker = cast(TursoEngine, object())
    conn_marker = object()
    connect_calls: list[object] = []
    seen_conn_ids: list[int] = []
    messages = [
        json.dumps(
            {
                "post_uid": "weibo:1",
                "platform": "weibo",
                "platform_post_id": "1",
                "author": "作者A",
                "created_at": "2026-03-28 10:00:00",
                "url": "https://example.com/post/1",
                "raw_text": "文本1",
                "display_md": "",
                "ingested_at": 100,
            },
            ensure_ascii=False,
        )
    ]

    class _ConnContext:
        def __enter__(self):
            return conn_marker

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    def _fake_connect(engine):
        connect_calls.append(engine)
        return _ConnContext()

    def _fake_pop_to_processing(client, queue_key):
        del client, queue_key
        if not messages:
            return None
        return messages.pop(0)

    def _fake_cloud_post_is_processed_or_newer(conn, post_uid, payload_ingested_at):
        del post_uid, payload_ingested_at
        assert conn is conn_marker
        seen_conn_ids.append(id(conn))
        return False

    def _fake_upsert(conn, **kwargs) -> None:
        del kwargs
        assert conn is conn_marker
        seen_conn_ids.append(id(conn))

    monkeypatch.setattr(redis_queue, "turso_connect_autocommit", _fake_connect)
    monkeypatch.setattr(
        redis_queue,
        "_redis_requeue_processing",
        lambda client, queue_key, max_items, verbose: 0,
    )
    monkeypatch.setattr(
        redis_queue, "_redis_pop_to_processing", _fake_pop_to_processing
    )
    monkeypatch.setattr(
        redis_queue,
        "_ack_and_cleanup",
        lambda client, queue_key, msg, post_uid, spool_dir, verbose: True,
    )
    monkeypatch.setattr(
        redis_queue,
        "_cloud_post_is_processed_or_newer",
        _fake_cloud_post_is_processed_or_newer,
    )
    monkeypatch.setattr(redis_queue, "upsert_pending_post", _fake_upsert)

    processed, turso_error = redis_queue.flush_redis_to_turso(
        client=object(),
        queue_key="test:q",
        spool_dir=tmp_path,
        engine=engine_marker,
        max_items=5,
        verbose=False,
    )

    assert processed == 1
    assert turso_error is False
    assert connect_calls == [engine_marker]
    assert len(seen_conn_ids) == 2
