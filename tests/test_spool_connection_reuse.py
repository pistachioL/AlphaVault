from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import cast

from alphavault.db.postgres_db import PostgresEngine
from alphavault.worker import spool
from alphavault.worker import redis_stream_queue as redis_stream_queue_module


def _build_payload(
    post_uid: str, platform_post_id: str, author: str, ingested_at: int
) -> dict:
    return {
        "post_uid": post_uid,
        "platform": "weibo",
        "platform_post_id": platform_post_id,
        "author": author,
        "created_at": "2026-03-28 10:00:00",
        "url": f"https://example.com/post/{platform_post_id}",
        "raw_text": f"文本{platform_post_id}",
        "ingested_at": ingested_at,
    }


def _write_processing_file(tmp_path: Path, post_uid: str, payload: dict) -> Path:
    path = tmp_path / f"{spool.sha1_short(post_uid)}.json.processing"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def test_claim_spool_file_refreshes_processing_mtime(tmp_path) -> None:
    json_path = spool.spool_write(
        tmp_path,
        "weibo:claim-mtime",
        _build_payload("weibo:claim-mtime", "claim-mtime", "作者M", 999),
    )
    stale_ts = int(time.time()) - 3600
    os.utime(json_path, (stale_ts, stale_ts))

    claimed_path = spool._claim_spool_file(json_path)

    assert claimed_path is not None
    assert not json_path.exists()
    assert claimed_path.exists()
    assert int(claimed_path.stat().st_mtime) >= int(time.time()) - 5


def test_flush_spool_to_source_db_reuses_single_connection(
    monkeypatch, tmp_path
) -> None:
    engine_marker = cast(PostgresEngine, object())
    conn_marker = object()
    connect_calls: list[object] = []
    upsert_conn_ids: list[int] = []

    class _ConnContext:
        def __enter__(self):
            return conn_marker

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    def _fake_connect(engine):
        connect_calls.append(engine)
        return _ConnContext()

    def _fake_upsert(conn, **kwargs) -> None:
        del kwargs
        assert conn is conn_marker
        upsert_conn_ids.append(id(conn))

    spool.spool_write(tmp_path, "weibo:1", _build_payload("weibo:1", "1", "作者A", 100))
    spool.spool_write(tmp_path, "weibo:2", _build_payload("weibo:2", "2", "作者B", 101))

    monkeypatch.setattr(
        spool, "postgres_connect_autocommit", _fake_connect, raising=False
    )
    monkeypatch.setattr(spool, "upsert_pending_post", _fake_upsert)

    processed, source_db_error = spool.flush_spool_to_source_db(
        spool_dir=tmp_path,
        engine=engine_marker,
        max_items=10,
    )

    assert processed == 2
    assert source_db_error is False
    assert connect_calls == [engine_marker]
    assert len(upsert_conn_ids) == 2
    assert len(list(tmp_path.glob("*.json"))) == 2
    assert list(tmp_path.glob("*.json.processing")) == []


def test_flush_spool_to_source_db_claims_file_before_upsert(
    monkeypatch, tmp_path
) -> None:
    engine_marker = cast(PostgresEngine, object())
    conn_marker = object()

    class _ConnContext:
        def __enter__(self):
            return conn_marker

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    def _fake_connect(_engine):
        return _ConnContext()

    def _fake_upsert(conn, **kwargs) -> None:
        del kwargs
        assert conn is conn_marker
        assert len(list(tmp_path.glob("*.json"))) == 0
        assert len(list(tmp_path.glob("*.json.processing"))) == 1

    spool.spool_write(
        tmp_path, "weibo:10", _build_payload("weibo:10", "10", "作者A", 110)
    )
    monkeypatch.setattr(
        spool, "postgres_connect_autocommit", _fake_connect, raising=False
    )
    monkeypatch.setattr(spool, "upsert_pending_post", _fake_upsert)

    processed, source_db_error = spool.flush_spool_to_source_db(
        spool_dir=tmp_path,
        engine=engine_marker,
        max_items=10,
    )

    assert processed == 1
    assert source_db_error is False
    assert len(list(tmp_path.glob("*.json"))) == 1
    assert list(tmp_path.glob("*.json.processing")) == []


def test_flush_spool_to_source_db_recovers_stale_processing_file(
    monkeypatch, tmp_path
) -> None:
    engine_marker = cast(PostgresEngine, object())
    conn_marker = object()
    seen_post_uids: list[str] = []

    class _ConnContext:
        def __enter__(self):
            return conn_marker

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    def _fake_connect(_engine):
        return _ConnContext()

    def _fake_upsert(conn, **kwargs) -> None:
        assert conn is conn_marker
        seen_post_uids.append(str(kwargs.get("post_uid") or ""))

    processing_path = _write_processing_file(
        tmp_path,
        "weibo:20",
        _build_payload("weibo:20", "20", "作者B", 120),
    )
    stale_ts = int(time.time()) - 3600
    os.utime(processing_path, (stale_ts, stale_ts))

    monkeypatch.setattr(
        spool, "postgres_connect_autocommit", _fake_connect, raising=False
    )
    monkeypatch.setattr(spool, "upsert_pending_post", _fake_upsert)

    processed, source_db_error = spool.flush_spool_to_source_db(
        spool_dir=tmp_path,
        engine=engine_marker,
        max_items=10,
    )

    assert processed == 1
    assert source_db_error is False
    assert seen_post_uids == ["weibo:20"]
    assert len(list(tmp_path.glob("*.json"))) == 1
    assert list(tmp_path.glob("*.json.processing")) == []


def test_flush_spool_to_source_db_skips_fresh_processing_file(
    monkeypatch, tmp_path
) -> None:
    engine_marker = cast(PostgresEngine, object())
    conn_marker = object()
    seen_post_uids: list[str] = []

    class _ConnContext:
        def __enter__(self):
            return conn_marker

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    def _fake_connect(_engine):
        return _ConnContext()

    def _fake_upsert(conn, **kwargs) -> None:
        assert conn is conn_marker
        seen_post_uids.append(str(kwargs.get("post_uid") or ""))

    _write_processing_file(
        tmp_path,
        "weibo:30",
        _build_payload("weibo:30", "30", "作者C", 130),
    )

    monkeypatch.setattr(
        spool, "postgres_connect_autocommit", _fake_connect, raising=False
    )
    monkeypatch.setattr(spool, "upsert_pending_post", _fake_upsert)

    processed, source_db_error = spool.flush_spool_to_source_db(
        spool_dir=tmp_path,
        engine=engine_marker,
        max_items=10,
    )

    assert processed == 0
    assert source_db_error is False
    assert seen_post_uids == []
    assert list(tmp_path.glob("*.json")) == []
    assert len(list(tmp_path.glob("*.json.processing"))) == 1


def test_flush_spool_to_source_db_restores_json_when_write_fails(
    monkeypatch, tmp_path
) -> None:
    engine_marker = cast(PostgresEngine, object())
    conn_marker = object()

    class _ConnContext:
        def __enter__(self):
            return conn_marker

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    def _fake_connect(_engine):
        return _ConnContext()

    def _fake_upsert(_conn, **_kwargs) -> None:
        raise RuntimeError("boom")

    spool.spool_write(
        tmp_path, "weibo:40", _build_payload("weibo:40", "40", "作者D", 140)
    )
    monkeypatch.setattr(
        spool, "postgres_connect_autocommit", _fake_connect, raising=False
    )
    monkeypatch.setattr(spool, "upsert_pending_post", _fake_upsert)

    processed, source_db_error = spool.flush_spool_to_source_db(
        spool_dir=tmp_path,
        engine=engine_marker,
        max_items=10,
    )

    assert processed == 0
    assert source_db_error is True
    assert len(list(tmp_path.glob("*.json"))) == 1
    assert list(tmp_path.glob("*.json.processing")) == []


def test_flush_spool_to_source_db_keeps_json_when_redis_push_succeeds(
    monkeypatch, tmp_path
) -> None:
    engine_marker = cast(PostgresEngine, object())
    conn_marker = object()

    class _ConnContext:
        def __enter__(self):
            return conn_marker

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    def _fake_connect(_engine):
        return _ConnContext()

    def _fake_upsert(_conn, **_kwargs) -> None:
        raise RuntimeError("boom")

    spool.spool_write(
        tmp_path, "weibo:50", _build_payload("weibo:50", "50", "作者E", 150)
    )
    monkeypatch.setattr(
        spool, "postgres_connect_autocommit", _fake_connect, raising=False
    )
    monkeypatch.setattr(spool, "upsert_pending_post", _fake_upsert)
    monkeypatch.setattr(
        redis_stream_queue_module,
        "redis_try_push_ai_message_status",
        lambda *_args, **_kwargs: redis_stream_queue_module.REDIS_PUSH_STATUS_PUSHED,
    )

    processed, source_db_error = spool.flush_spool_to_source_db(
        spool_dir=tmp_path,
        engine=engine_marker,
        max_items=10,
        redis_client=object(),
        redis_queue_key="test:q",
        delete_spool_on_redis_push=False,
    )

    assert processed == 1
    assert source_db_error is False
    assert len(list(tmp_path.glob("*.json"))) == 1
    assert list(tmp_path.glob("*.json.processing")) == []


def test_restore_claimed_file_keeps_old_and_new_json(tmp_path) -> None:
    post_uid = "weibo:restore-conflict"
    old_payload = _build_payload(post_uid, "50", "作者E", 150)
    new_payload = _build_payload(post_uid, "51", "作者E2", 151)

    json_path = spool.spool_write(tmp_path, post_uid, old_payload)
    claimed_path = spool._claim_spool_file(json_path)
    assert claimed_path is not None

    target_path = spool.spool_write(tmp_path, post_uid, new_payload)
    spool._restore_claimed_file_for_retry(
        claimed_path=claimed_path,
        target_path=target_path,
    )

    json_files = sorted(tmp_path.glob("*.json"))
    assert len(json_files) == 2
    platform_post_ids = sorted(
        str(json.loads(path.read_text(encoding="utf-8")).get("platform_post_id") or "")
        for path in json_files
    )
    assert platform_post_ids == ["50", "51"]
    assert (
        len([path for path in json_files if spool.SPOOL_RETRY_MARKER in path.name]) == 1
    )
    final_payload = json.loads(target_path.read_text(encoding="utf-8"))
    assert final_payload["platform_post_id"] == "51"
    assert list(tmp_path.glob("*.json.processing")) == []


def test_recover_spool_to_source_db_and_redis_requeues_pending_and_deletes_done(
    monkeypatch, tmp_path
) -> None:
    engine_marker = cast(PostgresEngine, object())
    conn_marker = object()
    requeued: list[str] = []

    class _ConnContext:
        def __enter__(self):
            return conn_marker

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    def _fake_connect(_engine):
        return _ConnContext()

    def _fake_load_processed_at(conn, *, post_uid: str):
        assert conn is conn_marker
        return {
            "weibo:missing": None,
            "weibo:pending": "",
            "weibo:done": "2026-04-04 10:00:00",
        }[post_uid]

    def _fake_upsert(conn, **kwargs) -> None:
        raise AssertionError("should not upsert pending post during spool recovery")

    spool.spool_write(
        tmp_path,
        "weibo:missing",
        _build_payload("weibo:missing", "60", "作者F", 160),
    )
    spool.spool_write(
        tmp_path,
        "weibo:pending",
        _build_payload("weibo:pending", "61", "作者G", 161),
    )
    spool.spool_write(
        tmp_path,
        "weibo:done",
        _build_payload("weibo:done", "62", "作者H", 162),
    )

    monkeypatch.setattr(
        spool, "postgres_connect_autocommit", _fake_connect, raising=False
    )
    monkeypatch.setattr(spool, "load_post_processed_at", _fake_load_processed_at)
    monkeypatch.setattr(spool, "upsert_pending_post", _fake_upsert)

    def _fake_requeue_status(_client, _queue_key, *, post_uid, **_kwargs):  # type: ignore[no-untyped-def]
        requeued.append(str(post_uid))
        return redis_stream_queue_module.REDIS_PUSH_STATUS_PUSHED

    monkeypatch.setattr(
        redis_stream_queue_module,
        "redis_try_push_ai_message_status",
        _fake_requeue_status,
    )

    handled_posts, queued_redis, deleted_done, has_error = (
        spool.recover_spool_to_source_db_and_redis(
            spool_dir=tmp_path,
            engine=engine_marker,
            max_items=10,
            redis_client=object(),
            redis_queue_key="test:q",
        )
    )

    assert handled_posts == 3
    assert queued_redis == 2
    assert deleted_done == 1
    assert has_error is False
    assert sorted(requeued) == ["weibo:missing", "weibo:pending"]
    assert list(tmp_path.glob("*.json")) == []
    assert list(tmp_path.glob("*.json.processing")) == []


def test_recover_spool_to_source_db_and_redis_keeps_json_when_ai_requeue_fails(
    monkeypatch, tmp_path
) -> None:
    engine_marker = cast(PostgresEngine, object())
    conn_marker = object()

    class _ConnContext:
        def __enter__(self):
            return conn_marker

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    def _fake_connect(_engine):
        return _ConnContext()

    spool.spool_write(
        tmp_path,
        "weibo:pending-error",
        _build_payload("weibo:pending-error", "63", "作者I", 163),
    )

    monkeypatch.setattr(
        spool, "postgres_connect_autocommit", _fake_connect, raising=False
    )
    monkeypatch.setattr(
        spool,
        "load_post_processed_at",
        lambda conn, *, post_uid: "" if conn is conn_marker else post_uid,
    )
    monkeypatch.setattr(
        redis_stream_queue_module,
        "redis_try_push_ai_message_status",
        lambda *_args, **_kwargs: redis_stream_queue_module.REDIS_PUSH_STATUS_ERROR,
    )

    handled_posts, queued_redis, deleted_done, has_error = (
        spool.recover_spool_to_source_db_and_redis(
            spool_dir=tmp_path,
            engine=engine_marker,
            max_items=10,
            redis_client=object(),
            redis_queue_key="test:q",
        )
    )

    assert handled_posts == 0
    assert queued_redis == 0
    assert deleted_done == 0
    assert has_error is True
    assert len(list(tmp_path.glob("*.json"))) == 1
    assert list(tmp_path.glob("*.json.processing")) == []


def test_recover_spool_to_source_db_and_redis_keeps_json_when_ai_requeue_is_duplicate(
    monkeypatch, tmp_path
) -> None:
    engine_marker = cast(PostgresEngine, object())
    conn_marker = object()

    class _ConnContext:
        def __enter__(self):
            return conn_marker

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    def _fake_connect(_engine):
        return _ConnContext()

    spool.spool_write(
        tmp_path,
        "weibo:pending-duplicate",
        _build_payload("weibo:pending-duplicate", "64", "作者J", 164),
    )

    monkeypatch.setattr(
        spool, "postgres_connect_autocommit", _fake_connect, raising=False
    )
    monkeypatch.setattr(
        spool,
        "load_post_processed_at",
        lambda conn, *, post_uid: "" if conn is conn_marker else post_uid,
    )
    monkeypatch.setattr(
        redis_stream_queue_module,
        "redis_try_push_ai_message_status",
        lambda *_args, **_kwargs: redis_stream_queue_module.REDIS_PUSH_STATUS_DUPLICATE,
    )

    handled_posts, queued_redis, deleted_done, has_error = (
        spool.recover_spool_to_source_db_and_redis(
            spool_dir=tmp_path,
            engine=engine_marker,
            max_items=10,
            redis_client=object(),
            redis_queue_key="test:q",
        )
    )

    assert handled_posts == 0
    assert queued_redis == 0
    assert deleted_done == 0
    assert has_error is False
    assert len(list(tmp_path.glob("*.json"))) == 1
    assert list(tmp_path.glob("*.json.processing")) == []


def test_recover_spool_to_source_db_and_redis_reports_claim_file_error(
    monkeypatch, tmp_path
) -> None:
    engine_marker = cast(PostgresEngine, object())
    conn_marker = object()

    class _ConnContext:
        def __enter__(self):
            return conn_marker

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    def _fake_connect(_engine):
        return _ConnContext()

    json_path = spool.spool_write(
        tmp_path,
        "weibo:claim-error",
        _build_payload("weibo:claim-error", "65", "作者K", 165),
    )
    original_rename = Path.rename

    def _fake_rename(self: Path, target: Path):  # type: ignore[no-untyped-def]
        if self == json_path:
            raise PermissionError("denied")
        return original_rename(self, target)

    monkeypatch.setattr(
        spool, "postgres_connect_autocommit", _fake_connect, raising=False
    )
    monkeypatch.setattr(Path, "rename", _fake_rename)

    handled_posts, queued_redis, deleted_done, has_error = (
        spool.recover_spool_to_source_db_and_redis(
            spool_dir=tmp_path,
            engine=engine_marker,
            max_items=10,
            redis_client=object(),
            redis_queue_key="test:q",
        )
    )

    assert handled_posts == 0
    assert queued_redis == 0
    assert deleted_done == 0
    assert has_error is True
    assert json_path.exists()
    assert list(tmp_path.glob("*.json.processing")) == []


def test_flush_spool_to_source_db_reports_stale_processing_stat_error(
    monkeypatch, tmp_path
) -> None:
    engine_marker = cast(PostgresEngine, object())
    processing_path = _write_processing_file(
        tmp_path,
        "weibo:stale-stat-error",
        _build_payload("weibo:stale-stat-error", "66", "作者L", 166),
    )
    stale_ts = int(time.time()) - 3600
    os.utime(processing_path, (stale_ts, stale_ts))
    original_stat = Path.stat

    def _fake_stat(self: Path, *args, **kwargs):  # type: ignore[no-untyped-def]
        if self == processing_path:
            raise PermissionError("stat denied")
        return original_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", _fake_stat)

    processed, has_error = spool.flush_spool_to_source_db(
        spool_dir=tmp_path,
        engine=engine_marker,
        max_items=10,
    )

    assert processed == 0
    assert has_error is True
    assert list(tmp_path.glob("*.json")) == []
    assert len(list(tmp_path.glob("*.json.processing"))) == 1


def test_recover_spool_to_source_db_and_redis_reports_stale_processing_rename_error(
    monkeypatch, tmp_path
) -> None:
    engine_marker = cast(PostgresEngine, object())
    post_uid = "weibo:stale-rename-error"
    processing_path = _write_processing_file(
        tmp_path,
        post_uid,
        _build_payload(post_uid, "67", "作者M", 167),
    )
    stale_ts = int(time.time()) - 3600
    os.utime(processing_path, (stale_ts, stale_ts))
    original_rename = Path.rename

    def _fake_rename(self: Path, target: Path):  # type: ignore[no-untyped-def]
        if self == processing_path:
            raise PermissionError("rename denied")
        return original_rename(self, target)

    monkeypatch.setattr(Path, "rename", _fake_rename)

    handled_posts, queued_redis, deleted_done, has_error = (
        spool.recover_spool_to_source_db_and_redis(
            spool_dir=tmp_path,
            engine=engine_marker,
            max_items=10,
            redis_client=object(),
            redis_queue_key="test:q",
        )
    )

    assert handled_posts == 0
    assert queued_redis == 0
    assert deleted_done == 0
    assert has_error is True
    assert list(tmp_path.glob("*.json")) == []
    assert len(list(tmp_path.glob("*.json.processing"))) == 1
