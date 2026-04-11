from __future__ import annotations

import logging
import sys
from types import SimpleNamespace
from typing import cast

import pytest
import requests

from alphavault.db.postgres_db import PostgresEngine
from alphavault.rss import utils as rss_utils
from alphavault.worker import ingest
from alphavault.worker import worker_loop_runtime
from alphavault.worker.cli import parse_args
from alphavault.worker.cli import RSSSourceConfig


def test_parse_args_rss_defaults(monkeypatch) -> None:
    monkeypatch.delenv("RSS_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("RSS_RETRIES", raising=False)
    monkeypatch.setattr(sys, "argv", ["weibo_rss_worker.py"])

    args = parse_args()

    assert float(args.rss_timeout) == 60.0
    assert int(args.rss_retries) == 5


def test_parse_args_reads_rss_env(monkeypatch) -> None:
    monkeypatch.setenv("RSS_TIMEOUT_SECONDS", "91")
    monkeypatch.setenv("RSS_RETRIES", "7")
    monkeypatch.setattr(sys, "argv", ["weibo_rss_worker.py"])

    args = parse_args()

    assert float(args.rss_timeout) == 91.0
    assert int(args.rss_retries) == 7


def test_parse_args_cli_overrides_rss_env(monkeypatch) -> None:
    monkeypatch.setenv("RSS_TIMEOUT_SECONDS", "91")
    monkeypatch.setenv("RSS_RETRIES", "7")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "weibo_rss_worker.py",
            "--rss-timeout",
            "22",
            "--rss-retries",
            "1",
        ],
    )

    args = parse_args()

    assert float(args.rss_timeout) == 22.0
    assert int(args.rss_retries) == 1


def test_parse_args_reads_ai_queue_ack_timeout_env(monkeypatch) -> None:
    monkeypatch.setenv("AI_QUEUE_ACK_TIMEOUT_SEC", "123")
    monkeypatch.setattr(sys, "argv", ["weibo_rss_worker.py"])

    args = parse_args()

    assert int(args.ai_stuck_seconds) == 123


def test_parse_args_invalid_ai_queue_ack_timeout_env_uses_default(monkeypatch) -> None:
    monkeypatch.setenv("AI_QUEUE_ACK_TIMEOUT_SEC", "bad")
    monkeypatch.setattr(sys, "argv", ["weibo_rss_worker.py"])

    args = parse_args()

    assert int(args.ai_stuck_seconds) == 3600


def test_parse_args_cli_overrides_ai_queue_ack_timeout_env(monkeypatch) -> None:
    monkeypatch.setenv("AI_QUEUE_ACK_TIMEOUT_SEC", "123")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "weibo_rss_worker.py",
            "--ai-stuck-seconds",
            "456",
        ],
    )

    args = parse_args()

    assert int(args.ai_stuck_seconds) == 456


def test_fetch_feed_retry_sleep_is_incremental(monkeypatch) -> None:
    request_calls: list[tuple[str, float]] = []
    sleep_calls: list[float] = []

    def _fake_get(url: str, *, headers, timeout: float):  # type: ignore[no-untyped-def]
        del headers
        request_calls.append((url, timeout))
        raise requests.Timeout("timeout")

    monkeypatch.setattr(rss_utils.requests, "get", _fake_get)
    monkeypatch.setattr(rss_utils.time, "sleep", lambda sec: sleep_calls.append(sec))

    with pytest.raises(requests.Timeout):
        rss_utils.fetch_feed("https://example.com/rss", timeout=60.0, retries=3)

    assert len(request_calls) == 4
    assert sleep_calls == [1.0, 2.0, 3.0]


def test_ingest_rss_many_once_passes_timeout_and_retries(monkeypatch, tmp_path) -> None:
    fetch_calls: list[tuple[str, float, int]] = []

    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        fetch_calls.append((url, timeout, retries))
        return SimpleNamespace(entries=[])

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=None,
        spool_dir=tmp_path,
        redis_client=None,
        redis_queue_key="",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
    )

    assert fetch_calls == [("https://example.com/rss", 60.0, 5)]
    assert accepted == 0
    assert enqueue_error is False


def test_ingest_rss_many_once_redis_primary_skips_source_db_write(
    monkeypatch, tmp_path
) -> None:
    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[{"link": "https://example.com/post/1", "title": "标题"}]
        )

    def _fake_push_to_redis_status(
        redis_client, redis_queue_key, *, post_uid: str, payload
    ) -> str:
        del redis_client, redis_queue_key, payload
        if str(post_uid or "").strip() == "weibo:1":
            return ingest.REDIS_PUSH_STATUS_PUSHED
        return ingest.REDIS_PUSH_STATUS_ERROR

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(
        ingest,
        "build_ids",
        lambda entry, link, feed_user_id, platform: ("mid1", "weibo:1", ""),
    )
    monkeypatch.setattr(ingest, "parse_datetime", lambda entry: "2026-03-28 10:00:00")
    monkeypatch.setattr(
        ingest,
        "choose_author",
        lambda entry, feed, author, platform: "测试博主",
    )
    monkeypatch.setattr(ingest, "get_entry_content", lambda entry: "")
    monkeypatch.setattr(ingest, "extract_image_urls_from_html", lambda html: [])
    monkeypatch.setattr(
        ingest,
        "upsert_pending_post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not write pending post inline")
        ),
        raising=False,
    )
    monkeypatch.setattr(ingest, "_try_push_to_redis_status", _fake_push_to_redis_status)

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=object(),  # type: ignore[arg-type]
        spool_dir=tmp_path,
        redis_client=object(),
        redis_queue_key="k",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
    )

    assert accepted == 1
    assert enqueue_error is False
    assert len(list(tmp_path.glob("*.json"))) == 0


def test_ingest_rss_many_once_ignores_enqueue_callback_and_pushes_redis_inline(
    monkeypatch, tmp_path
) -> None:
    pushed_post_uids: list[str] = []

    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[{"link": "https://example.com/post/1", "title": "标题"}]
        )

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(
        ingest,
        "build_ids",
        lambda entry, link, feed_user_id, platform: ("mid1", "weibo:1", ""),
    )
    monkeypatch.setattr(ingest, "parse_datetime", lambda entry: "2026-03-28 10:00:00")
    monkeypatch.setattr(
        ingest,
        "choose_author",
        lambda entry, feed, author, platform: "测试博主",
    )
    monkeypatch.setattr(ingest, "get_entry_content", lambda entry: "")
    monkeypatch.setattr(ingest, "extract_image_urls_from_html", lambda html: [])
    monkeypatch.setattr(
        ingest,
        "upsert_pending_post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not write source db inline")
        ),
        raising=False,
    )

    def _fake_push_status(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        pushed_post_uids.append("weibo:1")
        return ingest.REDIS_PUSH_STATUS_PUSHED

    monkeypatch.setattr(
        ingest,
        "_try_push_to_redis_status",
        _fake_push_status,
    )

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=object(),  # type: ignore[arg-type]
        spool_dir=tmp_path,
        redis_client=object(),
        redis_queue_key="k",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
        enqueue_spooled_payload=lambda _payload: (_ for _ in ()).throw(
            AssertionError("should not use local enqueue callback")
        ),
    )

    assert accepted == 1
    assert enqueue_error is False
    assert pushed_post_uids == ["weibo:1"]
    assert len(list(tmp_path.glob("*.json"))) == 0


def test_build_rss_accepted_log_line_contains_id_author_and_progress() -> None:
    line = ingest._build_rss_accepted_log_line(
        platform="weibo",
        post_uid="weibo:5281206301104087",
        author="博主A",
        entry_index=1,
        entry_total=25,
        feed_index=1,
        feed_total=3,
        accepted_total=7,
    )

    assert "[rss] accepted" in line
    assert "post_uid=weibo:5281206301104087" in line
    assert "author=博主A" in line
    assert "progress=1/25" in line
    assert "feed_progress=1/3" in line
    assert "accepted_total=7" in line


def test_ingest_rss_many_once_prints_accepted_log_with_progress(
    monkeypatch, tmp_path, caplog
) -> None:
    def _fake_push_to_redis_status(*_args, **_kwargs) -> str:
        return ingest.REDIS_PUSH_STATUS_PUSHED

    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[{"link": "https://example.com/post/1", "title": "标题"}]
        )

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(
        ingest,
        "build_ids",
        lambda entry, link, feed_user_id, platform: ("mid1", "weibo:1", ""),
    )
    monkeypatch.setattr(ingest, "parse_datetime", lambda entry: "2026-03-28 10:00:00")
    monkeypatch.setattr(
        ingest,
        "choose_author",
        lambda entry, feed, author, platform: "测试博主",
    )
    monkeypatch.setattr(ingest, "get_entry_content", lambda entry: "")
    monkeypatch.setattr(ingest, "extract_image_urls_from_html", lambda html: [])
    monkeypatch.setattr(
        ingest,
        "_try_push_to_redis_status",
        _fake_push_to_redis_status,
    )
    with caplog.at_level(logging.DEBUG):
        accepted, enqueue_error = ingest.ingest_rss_many_once(
            rss_urls=["https://example.com/rss"],
            engine=None,
            spool_dir=tmp_path,
            redis_client=object(),
            redis_queue_key="k",
            platform="weibo",
            author="",
            user_id=None,
            limit=None,
            rss_timeout=60.0,
            rss_retries=5,
        )

    assert accepted == 1
    assert enqueue_error is False
    assert "[rss] accepted" in caplog.text
    assert "post_uid=weibo:1" in caplog.text
    assert "author=测试博主" in caplog.text
    assert "progress=1/1" in caplog.text


def test_ingest_rss_many_once_accepted_total_is_per_user(
    monkeypatch, tmp_path, caplog
) -> None:
    def _fake_push_to_redis_status(*_args, **_kwargs) -> str:
        return ingest.REDIS_PUSH_STATUS_PUSHED

    feed_entries = {
        "https://example.com/rss/user_a": [
            {"id": "1", "link": "https://example.com/user_a/post/1", "title": "A1"},
            {"id": "2", "link": "https://example.com/user_a/post/2", "title": "A2"},
        ],
        "https://example.com/rss/user_b": [
            {"id": "1", "link": "https://example.com/user_b/post/1", "title": "B1"},
            {"id": "2", "link": "https://example.com/user_b/post/2", "title": "B2"},
        ],
    }
    rss_user_by_url = {
        "https://example.com/rss/user_a": "user_a",
        "https://example.com/rss/user_b": "user_b",
    }

    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del timeout, retries
        return SimpleNamespace(entries=feed_entries[url])

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(
        ingest,
        "infer_user_id_from_rss_url",
        lambda url: rss_user_by_url[url],
    )
    monkeypatch.setattr(
        ingest,
        "build_ids",
        lambda entry, link, feed_user_id, platform: (
            f"mid-{feed_user_id}-{entry['id']}",
            f"weibo:{feed_user_id}:{entry['id']}",
            "",
        ),
    )
    monkeypatch.setattr(ingest, "parse_datetime", lambda entry: "2026-03-28 10:00:00")
    monkeypatch.setattr(
        ingest,
        "choose_author",
        lambda entry, feed, author, platform: "测试博主",
    )
    monkeypatch.setattr(ingest, "get_entry_content", lambda entry: "")
    monkeypatch.setattr(ingest, "extract_image_urls_from_html", lambda html: [])
    monkeypatch.setattr(
        ingest,
        "_try_push_to_redis_status",
        _fake_push_to_redis_status,
    )
    with caplog.at_level(logging.DEBUG):
        accepted, enqueue_error = ingest.ingest_rss_many_once(
            rss_urls=[
                "https://example.com/rss/user_a",
                "https://example.com/rss/user_b",
            ],
            engine=None,
            spool_dir=tmp_path,
            redis_client=object(),
            redis_queue_key="k",
            platform="weibo",
            author="",
            user_id=None,
            limit=None,
            rss_timeout=60.0,
            rss_retries=5,
        )

    out_lines = caplog.messages
    assert accepted == 4
    assert enqueue_error is False
    assert any(
        "post_uid=weibo:user_a:1" in line and "accepted_total=1" in line
        for line in out_lines
    )
    assert any(
        "post_uid=weibo:user_a:2" in line and "accepted_total=2" in line
        for line in out_lines
    )
    assert any(
        "post_uid=weibo:user_b:1" in line and "accepted_total=1" in line
        for line in out_lines
    )
    assert any(
        "post_uid=weibo:user_b:2" in line and "accepted_total=2" in line
        for line in out_lines
    )


def test_ingest_rss_many_once_sleeps_between_feeds(monkeypatch, tmp_path) -> None:
    sleep_calls: list[float] = []

    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(entries=[])

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(ingest.time, "sleep", lambda sec: sleep_calls.append(sec))

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss/a", "https://example.com/rss/b"],
        engine=None,
        spool_dir=tmp_path,
        redis_client=None,
        redis_queue_key="",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
        rss_feed_sleep_seconds=10.0,
    )

    assert accepted == 0
    assert enqueue_error is False
    assert sleep_calls == [10.0]


def test_ingest_rss_many_once_does_not_sleep_when_disabled(
    monkeypatch, tmp_path
) -> None:
    sleep_calls: list[float] = []

    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(entries=[])

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(ingest.time, "sleep", lambda sec: sleep_calls.append(sec))

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss/a", "https://example.com/rss/b"],
        engine=None,
        spool_dir=tmp_path,
        redis_client=None,
        redis_queue_key="",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
        rss_feed_sleep_seconds=0.0,
    )

    assert accepted == 0
    assert enqueue_error is False
    assert sleep_calls == []


def test_ingest_rss_many_once_calls_item_ingested_callback_on_accept(
    monkeypatch, tmp_path
) -> None:
    callback_calls = {"count": 0}

    def _fake_push_to_redis_status(*_args, **_kwargs) -> str:
        return ingest.REDIS_PUSH_STATUS_PUSHED

    def _on_item_ingested() -> None:
        callback_calls["count"] = int(callback_calls["count"]) + 1

    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[{"link": "https://example.com/post/1", "title": "标题"}]
        )

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(
        ingest,
        "build_ids",
        lambda entry, link, feed_user_id, platform: ("mid1", "weibo:1", ""),
    )
    monkeypatch.setattr(ingest, "parse_datetime", lambda entry: "2026-03-28 10:00:00")
    monkeypatch.setattr(
        ingest,
        "choose_author",
        lambda entry, feed, author, platform: "测试博主",
    )
    monkeypatch.setattr(ingest, "get_entry_content", lambda entry: "")
    monkeypatch.setattr(ingest, "extract_image_urls_from_html", lambda html: [])
    monkeypatch.setattr(
        ingest,
        "_try_push_to_redis_status",
        _fake_push_to_redis_status,
    )
    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=None,
        spool_dir=tmp_path,
        redis_client=object(),
        redis_queue_key="k",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
        rss_feed_sleep_seconds=0.0,
        on_item_ingested=_on_item_ingested,
    )

    assert accepted == 1
    assert enqueue_error is False
    assert int(callback_calls["count"]) == 1


def test_ingest_rss_many_once_redis_error_sets_enqueue_error(
    monkeypatch, tmp_path
) -> None:
    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[{"link": "https://example.com/post/1", "title": "标题"}]
        )

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(
        ingest,
        "build_ids",
        lambda entry, link, feed_user_id, platform: ("mid1", "weibo:1", ""),
    )
    monkeypatch.setattr(ingest, "parse_datetime", lambda entry: "2026-03-28 10:00:00")
    monkeypatch.setattr(
        ingest,
        "choose_author",
        lambda entry, feed, author, platform: "测试博主",
    )
    monkeypatch.setattr(ingest, "get_entry_content", lambda entry: "")
    monkeypatch.setattr(ingest, "extract_image_urls_from_html", lambda html: [])
    monkeypatch.setattr(
        ingest,
        "_try_push_to_redis_status",
        lambda *_args, **_kwargs: ingest.REDIS_PUSH_STATUS_ERROR,
    )
    monkeypatch.setattr(
        ingest,
        "upsert_pending_post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not write pending post inline")
        ),
        raising=False,
    )

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=None,
        spool_dir=tmp_path,
        redis_client=object(),
        redis_queue_key="k",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
        rss_feed_sleep_seconds=0.0,
    )

    assert accepted == 0
    assert enqueue_error is True


def test_ingest_rss_many_once_redis_error_does_not_need_second_fallback(
    monkeypatch, tmp_path
) -> None:
    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[{"link": "https://example.com/post/1", "title": "标题"}]
        )

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(
        ingest,
        "build_ids",
        lambda entry, link, feed_user_id, platform: ("mid1", "weibo:1", ""),
    )
    monkeypatch.setattr(ingest, "parse_datetime", lambda entry: "2026-03-28 10:00:00")
    monkeypatch.setattr(
        ingest,
        "choose_author",
        lambda entry, feed, author, platform: "测试博主",
    )
    monkeypatch.setattr(ingest, "get_entry_content", lambda entry: "")
    monkeypatch.setattr(ingest, "extract_image_urls_from_html", lambda html: [])
    monkeypatch.setattr(
        ingest,
        "_try_push_to_redis_status",
        lambda *_args, **_kwargs: ingest.REDIS_PUSH_STATUS_ERROR,
    )
    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=None,
        spool_dir=tmp_path,
        redis_client=object(),
        redis_queue_key="k",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
        rss_feed_sleep_seconds=0.0,
    )

    assert accepted == 0
    assert enqueue_error is True


def test_ingest_rss_many_once_without_redis_sets_enqueue_error(
    monkeypatch, tmp_path
) -> None:
    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[{"link": "https://example.com/post/1", "title": "标题"}]
        )

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(
        ingest,
        "build_ids",
        lambda entry, link, feed_user_id, platform: ("mid1", "weibo:1", ""),
    )
    monkeypatch.setattr(ingest, "parse_datetime", lambda entry: "2026-03-28 10:00:00")
    monkeypatch.setattr(
        ingest,
        "choose_author",
        lambda entry, feed, author, platform: "测试博主",
    )
    monkeypatch.setattr(ingest, "get_entry_content", lambda entry: "")
    monkeypatch.setattr(ingest, "extract_image_urls_from_html", lambda html: [])
    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=None,
        spool_dir=tmp_path,
        redis_client=None,
        redis_queue_key="",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
        rss_feed_sleep_seconds=0.0,
    )

    assert accepted == 0
    assert enqueue_error is True


def test_build_source_runtimes_requires_redis_for_worker(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(worker_loop_runtime, "ensure_spool_dir", lambda: tmp_path)
    monkeypatch.setattr(worker_loop_runtime, "try_get_redis", lambda: (None, ""))
    monkeypatch.setattr(
        worker_loop_runtime,
        "require_postgres_source_from_env",
        lambda name: SimpleNamespace(dsn="postgres://db", schema=str(name)),
    )
    monkeypatch.setattr(
        worker_loop_runtime, "ensure_postgres_engine", lambda *args, **kwargs: object()
    )

    with pytest.raises(RuntimeError, match="REDIS_URL"):
        worker_loop_runtime.build_source_runtimes(
            source_configs=[
                RSSSourceConfig(
                    name="weibo",
                    platform="weibo",
                    rss_urls=["https://example.com/rss"],
                    database_url="postgres://db",
                    author="",
                    user_id=None,
                )
            ]
        )


def test_build_source_runtimes_always_uses_source_redis_key(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setattr(worker_loop_runtime, "ensure_spool_dir", lambda: tmp_path)
    monkeypatch.setattr(
        worker_loop_runtime, "try_get_redis", lambda: (object(), "queue")
    )
    monkeypatch.setattr(
        worker_loop_runtime,
        "require_postgres_source_from_env",
        lambda name: SimpleNamespace(dsn="postgres://db", schema=str(name)),
    )
    monkeypatch.setattr(
        worker_loop_runtime, "ensure_postgres_engine", lambda *args, **kwargs: object()
    )

    sources, redis_client, base_key = worker_loop_runtime.build_source_runtimes(
        source_configs=[
            RSSSourceConfig(
                name="weibo",
                platform="weibo",
                rss_urls=["https://example.com/rss"],
                database_url="postgres://db",
                author="",
                user_id=None,
            )
        ]
    )

    assert redis_client is not None
    assert base_key == "queue"
    assert len(sources) == 1
    assert str(sources[0].redis_queue_key) == "queue:weibo"


def test_ingest_rss_many_once_retry_same_item_after_enqueue_failure(
    monkeypatch, tmp_path
) -> None:
    push_calls = {"count": 0}

    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[
                {"id": "1", "link": "https://example.com/post/dup", "title": "标题1"},
                {"id": "1", "link": "https://example.com/post/dup", "title": "标题2"},
            ]
        )

    def _fake_push(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        push_calls["count"] += 1
        if int(push_calls["count"]) == 1:
            return ingest.REDIS_PUSH_STATUS_ERROR
        return ingest.REDIS_PUSH_STATUS_PUSHED

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(
        ingest,
        "build_ids",
        lambda entry, link, feed_user_id, platform: ("mid1", "weibo:1", ""),
    )
    monkeypatch.setattr(ingest, "parse_datetime", lambda entry: "2026-03-28 10:00:00")
    monkeypatch.setattr(
        ingest,
        "choose_author",
        lambda entry, feed, author, platform: "测试博主",
    )
    monkeypatch.setattr(ingest, "get_entry_content", lambda entry: "")
    monkeypatch.setattr(ingest, "extract_image_urls_from_html", lambda html: [])
    monkeypatch.setattr(ingest, "_try_push_to_redis_status", _fake_push)

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=None,
        spool_dir=tmp_path,
        redis_client=object(),
        redis_queue_key="k",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
        rss_feed_sleep_seconds=0.0,
    )

    assert accepted == 1
    assert enqueue_error is True
    assert int(push_calls["count"]) == 2


def test_ingest_rss_many_once_prints_feed_sleep_log(
    monkeypatch, tmp_path, caplog
) -> None:
    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(entries=[])

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(ingest.time, "sleep", lambda sec: None)

    with caplog.at_level(logging.INFO):
        accepted, enqueue_error = ingest.ingest_rss_many_once(
            rss_urls=["https://example.com/rss/a", "https://example.com/rss/b"],
            engine=None,
            spool_dir=tmp_path,
            redis_client=None,
            redis_queue_key="",
            platform="weibo",
            author="",
            user_id=None,
            limit=None,
            rss_timeout=60.0,
            rss_retries=5,
            rss_feed_sleep_seconds=10.0,
        )

    assert accepted == 0
    assert enqueue_error is False
    assert "[rss] feed_start" in caplog.text
    assert "[rss] feed_done" in caplog.text
    assert "[rss] feed_sleep" in caplog.text
    assert "[rss] cycle_done" in caplog.text


def test_ingest_rss_many_once_without_redis_skips_items(monkeypatch, tmp_path) -> None:
    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[
                {"id": "1", "link": "https://example.com/post/1", "title": "标题1"},
                {"id": "2", "link": "https://example.com/post/2", "title": "标题2"},
            ]
        )

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(
        ingest,
        "build_ids",
        lambda entry, link, feed_user_id, platform: (
            f"mid-{entry['id']}",
            f"weibo:{entry['id']}",
            "",
        ),
    )
    monkeypatch.setattr(ingest, "parse_datetime", lambda entry: "2026-03-28 10:00:00")
    monkeypatch.setattr(
        ingest,
        "choose_author",
        lambda entry, feed, author, platform: "测试博主",
    )
    monkeypatch.setattr(ingest, "get_entry_content", lambda entry: "")
    monkeypatch.setattr(ingest, "extract_image_urls_from_html", lambda html: [])
    monkeypatch.setattr(
        ingest,
        "upsert_pending_post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not write pending post inline")
        ),
        raising=False,
    )

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=cast(PostgresEngine, object()),
        spool_dir=tmp_path,
        redis_client=None,
        redis_queue_key="",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
    )

    assert accepted == 0
    assert enqueue_error is True


def test_ingest_rss_many_once_redis_error_does_not_fall_back_to_source_db_inline(
    monkeypatch, tmp_path
) -> None:
    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[
                {"id": "1", "link": "https://example.com/post/1", "title": "标题1"},
                {"id": "2", "link": "https://example.com/post/2", "title": "标题2"},
            ]
        )

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(
        ingest,
        "build_ids",
        lambda entry, link, feed_user_id, platform: (
            f"mid-{entry['id']}",
            f"weibo:{entry['id']}",
            "",
        ),
    )
    monkeypatch.setattr(ingest, "parse_datetime", lambda entry: "2026-03-28 10:00:00")
    monkeypatch.setattr(
        ingest,
        "choose_author",
        lambda entry, feed, author, platform: "测试博主",
    )
    monkeypatch.setattr(ingest, "get_entry_content", lambda entry: "")
    monkeypatch.setattr(ingest, "extract_image_urls_from_html", lambda html: [])
    monkeypatch.setattr(
        ingest,
        "upsert_pending_post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not write pending post inline")
        ),
        raising=False,
    )
    monkeypatch.setattr(
        ingest,
        "_try_push_to_redis_status",
        lambda *_args, **_kwargs: ingest.REDIS_PUSH_STATUS_ERROR,
    )

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=cast(PostgresEngine, object()),
        spool_dir=tmp_path,
        redis_client=object(),
        redis_queue_key="queue",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
    )

    assert accepted == 0
    assert enqueue_error is True


def test_ingest_rss_many_once_redis_duplicate_is_skipped_without_error(
    monkeypatch, tmp_path
) -> None:
    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[
                {"id": "1", "link": "https://example.com/post/1", "title": "标题1"}
            ]
        )

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(
        ingest,
        "build_ids",
        lambda entry, link, feed_user_id, platform: (
            f"mid-{entry['id']}",
            f"weibo:{entry['id']}",
            "",
        ),
    )
    monkeypatch.setattr(ingest, "parse_datetime", lambda entry: "2026-03-28 10:00:00")
    monkeypatch.setattr(
        ingest,
        "choose_author",
        lambda entry, feed, author, platform: "测试博主",
    )
    monkeypatch.setattr(ingest, "get_entry_content", lambda entry: "")
    monkeypatch.setattr(ingest, "extract_image_urls_from_html", lambda html: [])
    monkeypatch.setattr(
        ingest,
        "upsert_pending_post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not write pending post inline")
        ),
        raising=False,
    )
    monkeypatch.setattr(
        ingest,
        "_try_push_to_redis_status",
        lambda *_args, **_kwargs: ingest.REDIS_PUSH_STATUS_DUPLICATE,
    )

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=cast(PostgresEngine, object()),
        spool_dir=tmp_path,
        redis_client=object(),
        redis_queue_key="queue",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
    )

    assert accepted == 0
    assert enqueue_error is False


def test_ingest_rss_many_once_redis_duplicate_keeps_enqueue_error_false(
    monkeypatch, tmp_path
) -> None:
    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[
                {"id": "1", "link": "https://example.com/post/1", "title": "标题1"}
            ]
        )

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(
        ingest,
        "build_ids",
        lambda entry, link, feed_user_id, platform: (
            f"mid-{entry['id']}",
            f"weibo:{entry['id']}",
            "",
        ),
    )
    monkeypatch.setattr(ingest, "parse_datetime", lambda entry: "2026-03-28 10:00:00")
    monkeypatch.setattr(
        ingest,
        "choose_author",
        lambda entry, feed, author, platform: "测试博主",
    )
    monkeypatch.setattr(ingest, "get_entry_content", lambda entry: "")
    monkeypatch.setattr(ingest, "extract_image_urls_from_html", lambda html: [])
    monkeypatch.setattr(
        ingest,
        "_try_push_to_redis_status",
        lambda *_args, **_kwargs: ingest.REDIS_PUSH_STATUS_DUPLICATE,
    )

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=cast(PostgresEngine, object()),
        spool_dir=tmp_path,
        redis_client=object(),
        redis_queue_key="queue",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
    )

    assert accepted == 0
    assert enqueue_error is False
