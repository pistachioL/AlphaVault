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


def test_run_manual_ingest_for_source_flushes_spool_to_turso(
    monkeypatch, tmp_path
) -> None:
    flush_calls: list[int] = []

    monkeypatch.setattr(
        trigger, "ensure_turso_engine", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(
        trigger,
        "ingest_rss_many_once",
        lambda **_kwargs: (2, False),
    )

    def _fake_flush_spool_to_turso(**kwargs):  # type: ignore[no-untyped-def]
        flush_calls.append(int(kwargs.get("max_items", 0)))
        return 0, False

    monkeypatch.setattr(
        trigger,
        "flush_spool_to_turso",
        _fake_flush_spool_to_turso,
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
    assert len(flush_calls) >= 1
