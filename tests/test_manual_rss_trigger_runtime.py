from __future__ import annotations

from pathlib import Path
from typing import cast

from alphavault.worker import manual_rss_trigger as trigger
from alphavault.worker.cli import RSSSourceConfig


def _build_source_config() -> RSSSourceConfig:
    return RSSSourceConfig(
        name="weibo",
        platform="weibo",
        rss_urls=["https://example.com/rss"],
        database_url="postgres://example",
        auth_token="token",
        author="",
        user_id=None,
    )


def test_run_manual_ingest_for_source_uses_stream_only(monkeypatch, tmp_path) -> None:
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        trigger, "ensure_postgres_engine", lambda *_args, **_kwargs: object()
    )

    def _fake_ingest(**kwargs):  # type: ignore[no-untyped-def]
        seen.update(kwargs)
        return 2, False

    monkeypatch.setattr(trigger, "ingest_rss_many_once", _fake_ingest)

    result = trigger._run_manual_ingest_for_source(
        source=_build_source_config(),
        base_spool_dir=Path(tmp_path),
        base_redis_queue_key="q",
        redis_client=object(),
        multi_source=False,
        rss_timeout=60.0,
        rss_retries=5,
        rss_feed_sleep_seconds=0.0,
    )

    assert result["accepted"] == 2
    assert result["enqueue_error"] is False
    assert seen["redis_queue_key"] == "q"
    assert Path(str(seen["spool_dir"])) == Path(tmp_path)


def test_run_manual_db_requeue_once_dry_run_reads_failed_rows_without_enqueue(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trigger,
        "resolve_rss_source_configs",
        lambda _args: [_build_source_config()],
    )
    monkeypatch.setattr(
        trigger, "ensure_postgres_engine", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(trigger, "try_get_redis", lambda: (object(), "queue"))
    monkeypatch.setattr(
        trigger,
        "load_failed_post_queue_rows",
        lambda _engine, *, limit: [
            {
                "post_uid": "weibo:1",
                "platform": "weibo",
                "platform_post_id": "1",
                "author": "作者A",
                "created_at": "2026-04-09 10:00:00",
                "url": "https://example.com/1",
                "raw_text": "正文1",
            }
        ][:limit],
    )
    monkeypatch.setattr(
        trigger,
        "load_unprocessed_post_queue_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("dry run failed mode should not read legacy rows")
        ),
    )
    monkeypatch.setattr(
        trigger,
        "redis_ai_pressure_snapshot",
        lambda *_args, **_kwargs: {
            "pending_count": 0,
            "unread_count": 0,
            "retry_count": 0,
            "total_backlog": 0,
        },
    )
    monkeypatch.setattr(
        trigger,
        "redis_try_push_ai_message_status",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("dry run should not enqueue")
        ),
    )

    result = trigger.run_manual_db_requeue_once(
        mode="failed",
        platform=None,
        limit=10,
        dry_run=True,
    )

    assert result["mode"] == "failed"
    assert result["dry_run"] is True
    assert result["scanned_total"] == 1
    assert result["enqueued_total"] == 0


def test_run_manual_db_requeue_once_enqueues_rows_when_capacity_allows(
    monkeypatch,
) -> None:
    pushed: list[dict[str, object]] = []

    def _push_status(
        _client,
        _queue_key,
        *,
        post_uid,
        payload,
        ttl_seconds,
        queue_maxlen,
    ) -> str:
        del ttl_seconds, queue_maxlen
        pushed.append(
            {
                "post_uid": str(post_uid or ""),
                "payload": dict(payload),
            }
        )
        return trigger.REDIS_PUSH_STATUS_PUSHED

    monkeypatch.setattr(
        trigger,
        "resolve_rss_source_configs",
        lambda _args: [_build_source_config()],
    )
    monkeypatch.setattr(
        trigger, "ensure_postgres_engine", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(trigger, "try_get_redis", lambda: (object(), "queue"))
    monkeypatch.setattr(trigger, "resolve_redis_ai_queue_maxlen", lambda: 100)
    monkeypatch.setattr(trigger, "resolve_redis_dedup_ttl_seconds", lambda: 123)
    monkeypatch.setattr(
        trigger,
        "load_failed_post_queue_rows",
        lambda _engine, *, limit: [
            {
                "post_uid": "weibo:1",
                "platform": "weibo",
                "platform_post_id": "1",
                "author": "作者A",
                "created_at": "2026-04-09 10:00:00",
                "url": "https://example.com/1",
                "raw_text": "正文1",
            },
            {
                "post_uid": "weibo:2",
                "platform": "weibo",
                "platform_post_id": "2",
                "author": "作者B",
                "created_at": "2026-04-09 10:01:00",
                "url": "https://example.com/2",
                "raw_text": "正文2",
            },
        ][:limit],
    )
    monkeypatch.setattr(
        trigger,
        "redis_ai_pressure_snapshot",
        lambda *_args, **_kwargs: {
            "pending_count": 1,
            "unread_count": 1,
            "retry_count": 0,
            "total_backlog": 2,
        },
    )
    monkeypatch.setattr(
        trigger,
        "redis_try_push_ai_message_status",
        _push_status,
    )

    result = trigger.run_manual_db_requeue_once(
        mode="failed",
        platform="weibo",
        limit=2,
        dry_run=False,
    )

    assert result["enqueued_total"] == 2
    assert [item["post_uid"] for item in pushed] == ["weibo:1", "weibo:2"]
    assert all(
        bool(cast(dict[str, object], item["payload"]).get("skip_db_processed_guard"))
        for item in pushed
    )


def test_run_manual_db_requeue_once_skips_duplicate_rows_without_error(
    monkeypatch,
) -> None:
    pushed: list[str] = []

    monkeypatch.setattr(
        trigger,
        "resolve_rss_source_configs",
        lambda _args: [_build_source_config()],
    )
    monkeypatch.setattr(
        trigger, "ensure_postgres_engine", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(trigger, "try_get_redis", lambda: (object(), "queue"))
    monkeypatch.setattr(trigger, "resolve_redis_ai_queue_maxlen", lambda: 100)
    monkeypatch.setattr(trigger, "resolve_redis_dedup_ttl_seconds", lambda: 123)
    monkeypatch.setattr(
        trigger,
        "load_failed_post_queue_rows",
        lambda _engine, *, limit: [
            {
                "post_uid": "weibo:1",
                "platform": "weibo",
                "platform_post_id": "1",
                "author": "作者A",
                "created_at": "2026-04-09 10:00:00",
                "url": "https://example.com/1",
                "raw_text": "正文1",
            },
            {
                "post_uid": "weibo:2",
                "platform": "weibo",
                "platform_post_id": "2",
                "author": "作者B",
                "created_at": "2026-04-09 10:01:00",
                "url": "https://example.com/2",
                "raw_text": "正文2",
            },
        ][:limit],
    )
    monkeypatch.setattr(
        trigger,
        "redis_ai_pressure_snapshot",
        lambda *_args, **_kwargs: {
            "pending_count": 0,
            "unread_count": 0,
            "retry_count": 0,
            "total_backlog": 0,
        },
    )

    def _push_status(
        _client,
        _queue_key,
        *,
        post_uid,
        payload,
        ttl_seconds,
        queue_maxlen,
    ) -> str:
        del payload, ttl_seconds, queue_maxlen
        resolved_post_uid = str(post_uid or "")
        if resolved_post_uid == "weibo:1":
            return trigger.REDIS_PUSH_STATUS_DUPLICATE
        pushed.append(resolved_post_uid)
        return trigger.REDIS_PUSH_STATUS_PUSHED

    monkeypatch.setattr(
        trigger,
        "redis_try_push_ai_message_status",
        _push_status,
    )

    result = trigger.run_manual_db_requeue_once(
        mode="failed",
        platform="weibo",
        limit=2,
        dry_run=False,
    )

    assert result["scanned_total"] == 2
    assert result["enqueued_total"] == 1
    assert pushed == ["weibo:2"]


def test_run_manual_db_requeue_once_skips_when_queue_is_full(monkeypatch) -> None:
    monkeypatch.setattr(
        trigger,
        "resolve_rss_source_configs",
        lambda _args: [_build_source_config()],
    )
    monkeypatch.setattr(
        trigger, "ensure_postgres_engine", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(trigger, "try_get_redis", lambda: (object(), "queue"))
    monkeypatch.setattr(trigger, "resolve_redis_ai_queue_maxlen", lambda: 100)
    monkeypatch.setattr(
        trigger,
        "redis_ai_pressure_snapshot",
        lambda *_args, **_kwargs: {
            "pending_count": 40,
            "unread_count": 40,
            "retry_count": 20,
            "total_backlog": 100,
        },
    )
    monkeypatch.setattr(
        trigger,
        "load_failed_post_queue_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("full queue should skip DB scan")
        ),
    )

    result = trigger.run_manual_db_requeue_once(
        mode="failed",
        platform="weibo",
        limit=10,
        dry_run=False,
    )

    assert result["enqueued_total"] == 0
    source_results = cast(list[dict[str, object]], result["sources"])
    assert source_results[0]["skipped_pressure"] is True
