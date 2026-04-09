from __future__ import annotations

from pathlib import Path

from alphavault.worker import manual_rss_trigger as trigger
from alphavault.worker.cli import RSSSourceConfig


def _build_source_config() -> RSSSourceConfig:
    return RSSSourceConfig(
        name="weibo",
        platform="weibo",
        rss_urls=["https://example.com/rss"],
        database_url="libsql://example.turso.io",
        auth_token="token",
        author="",
        user_id=None,
    )


def test_run_manual_ingest_for_source_recovers_spool_to_redis(
    monkeypatch, tmp_path
) -> None:
    recover_calls: list[int] = []
    (Path(tmp_path) / "pending.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        trigger, "ensure_postgres_engine", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(
        trigger,
        "ingest_rss_many_once",
        lambda **_kwargs: (2, False),
    )

    def _fake_recover_spool(**kwargs):  # type: ignore[no-untyped-def]
        recover_calls.append(int(kwargs.get("max_items", 0)))
        return 0, 0, 0, False

    monkeypatch.setattr(
        trigger,
        "recover_spool_to_turso_and_redis",
        _fake_recover_spool,
        raising=False,
    )

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
    assert len(recover_calls) >= 1


def test_recover_source_spool_to_redis_keeps_looping_when_files_remain(
    monkeypatch, tmp_path
) -> None:
    recover_calls = {"count": 0}
    pending_path = Path(tmp_path) / "pending.json"
    pending_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(trigger, "MANUAL_SPOOL_RECOVER_BATCH_SIZE", 2)

    def _fake_recover_spool(**_kwargs):  # type: ignore[no-untyped-def]
        recover_calls["count"] += 1
        if int(recover_calls["count"]) == 1:
            return 1, 1, 0, False
        pending_path.unlink(missing_ok=True)
        return 1, 1, 0, False

    monkeypatch.setattr(
        trigger,
        "recover_spool_to_turso_and_redis",
        _fake_recover_spool,
        raising=False,
    )

    flushed, has_error = trigger._recover_source_spool_to_redis(
        spool_dir=Path(tmp_path),
        engine=object(),  # type: ignore[arg-type]
        redis_client=object(),
        redis_queue_key="q",
    )

    assert flushed == 2
    assert has_error is False
    assert int(recover_calls["count"]) == 2


def test_recover_source_spool_to_redis_does_not_fail_when_new_file_appears(
    monkeypatch, tmp_path
) -> None:
    recover_calls = {"count": 0}
    existing_path = Path(tmp_path) / "existing-duplicate.json"
    pending_path = Path(tmp_path) / "new-pending.json"
    existing_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(trigger, "MANUAL_SPOOL_RECOVER_BATCH_SIZE", 2)

    def _fake_recover_spool(**_kwargs):  # type: ignore[no-untyped-def]
        recover_calls["count"] += 1
        if int(recover_calls["count"]) == 1:
            pending_path.write_text("{}", encoding="utf-8")
            return 0, 0, 0, False
        if int(recover_calls["count"]) == 2:
            pending_path.unlink(missing_ok=True)
            return 1, 1, 0, False
        return 0, 0, 0, False

    monkeypatch.setattr(
        trigger,
        "recover_spool_to_turso_and_redis",
        _fake_recover_spool,
        raising=False,
    )

    flushed, has_error = trigger._recover_source_spool_to_redis(
        spool_dir=Path(tmp_path),
        engine=object(),  # type: ignore[arg-type]
        redis_client=object(),
        redis_queue_key="q",
    )

    assert flushed == 1
    assert has_error is False
    assert int(recover_calls["count"]) == 3


def test_recover_source_spool_to_redis_duplicate_file_is_not_reported_as_error(
    monkeypatch, tmp_path
) -> None:
    pending_path = Path(tmp_path) / "duplicate.json"
    pending_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(trigger, "MANUAL_SPOOL_RECOVER_BATCH_SIZE", 2)

    def _fake_recover_spool(**_kwargs):  # type: ignore[no-untyped-def]
        return 0, 0, 0, False

    monkeypatch.setattr(
        trigger,
        "recover_spool_to_turso_and_redis",
        _fake_recover_spool,
        raising=False,
    )

    flushed, has_error = trigger._recover_source_spool_to_redis(
        spool_dir=Path(tmp_path),
        engine=object(),  # type: ignore[arg-type]
        redis_client=object(),
        redis_queue_key="q",
    )

    assert flushed == 0
    assert has_error is False
    assert pending_path.exists()


def test_recover_source_spool_to_redis_runs_when_only_processing_file_exists(
    monkeypatch, tmp_path
) -> None:
    recover_calls = {"count": 0}
    processing_path = Path(tmp_path) / "pending.json.processing"
    processing_path.write_text("{}", encoding="utf-8")

    def _fake_recover_spool(**_kwargs):  # type: ignore[no-untyped-def]
        recover_calls["count"] += 1
        processing_path.unlink(missing_ok=True)
        return 0, 0, 0, False

    monkeypatch.setattr(
        trigger,
        "recover_spool_to_turso_and_redis",
        _fake_recover_spool,
        raising=False,
    )

    flushed, has_error = trigger._recover_source_spool_to_redis(
        spool_dir=Path(tmp_path),
        engine=object(),  # type: ignore[arg-type]
        redis_client=object(),
        redis_queue_key="q",
    )

    assert flushed == 0
    assert has_error is False
    assert int(recover_calls["count"]) == 1


def test_recover_source_spool_to_redis_expands_scan_when_front_batch_is_duplicate(
    monkeypatch, tmp_path
) -> None:
    recover_calls: list[int] = []
    duplicate_a = Path(tmp_path) / "a-duplicate.json"
    duplicate_b = Path(tmp_path) / "b-duplicate.json"
    recoverable = Path(tmp_path) / "c-recoverable.json"
    duplicate_a.write_text("{}", encoding="utf-8")
    duplicate_b.write_text("{}", encoding="utf-8")
    recoverable.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(trigger, "MANUAL_SPOOL_RECOVER_BATCH_SIZE", 2)

    def _fake_recover_spool(**kwargs):  # type: ignore[no-untyped-def]
        max_items = int(kwargs.get("max_items", 0))
        recover_calls.append(max_items)
        if len(recover_calls) == 1:
            return 0, 0, 0, False
        if len(recover_calls) == 2:
            recoverable.unlink(missing_ok=True)
            return 1, 1, 0, False
        return 0, 0, 0, False

    monkeypatch.setattr(
        trigger,
        "recover_spool_to_turso_and_redis",
        _fake_recover_spool,
        raising=False,
    )

    flushed, has_error = trigger._recover_source_spool_to_redis(
        spool_dir=Path(tmp_path),
        engine=object(),  # type: ignore[arg-type]
        redis_client=object(),
        redis_queue_key="q",
    )

    assert flushed == 1
    assert has_error is False
    assert len(recover_calls) >= 2
    assert max(recover_calls) >= 3
