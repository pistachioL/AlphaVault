from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest
import requests

from alphavault.rss import utils as rss_utils
from alphavault.worker import ingest
from alphavault.worker.cli import parse_args


def test_parse_args_rss_defaults(monkeypatch) -> None:
    monkeypatch.delenv("RSS_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("RSS_RETRIES", raising=False)
    monkeypatch.setattr(sys, "argv", ["weibo_rss_turso_worker.py"])

    args = parse_args()

    assert float(args.rss_timeout) == 60.0
    assert int(args.rss_retries) == 5


def test_parse_args_reads_rss_env(monkeypatch) -> None:
    monkeypatch.setenv("RSS_TIMEOUT_SECONDS", "91")
    monkeypatch.setenv("RSS_RETRIES", "7")
    monkeypatch.setattr(sys, "argv", ["weibo_rss_turso_worker.py"])

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
            "weibo_rss_turso_worker.py",
            "--rss-timeout",
            "22",
            "--rss-retries",
            "1",
        ],
    )

    args = parse_args()

    assert float(args.rss_timeout) == 22.0
    assert int(args.rss_retries) == 1


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
        verbose=False,
    )

    assert fetch_calls == [("https://example.com/rss", 60.0, 5)]
    assert accepted == 0
    assert enqueue_error is False


def test_ingest_rss_many_once_redis_primary_skips_turso_write(
    monkeypatch, tmp_path
) -> None:
    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[{"link": "https://example.com/post/1", "title": "标题"}]
        )

    class _ConnContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    upsert_calls: list[str] = []

    def _fake_push_to_redis_status(
        redis_client, redis_queue_key, *, post_uid: str, payload, verbose: bool
    ) -> str:
        del redis_client, redis_queue_key, payload, verbose
        if str(post_uid or "").strip() == "weibo:1":
            return ingest.REDIS_PUSH_STATUS_PUSHED
        return ingest.REDIS_PUSH_STATUS_ERROR

    author_recent_push_calls: list[str] = []

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
        ingest, "turso_connect_autocommit", lambda _engine: _ConnContext()
    )
    monkeypatch.setattr(
        ingest,
        "upsert_pending_post",
        lambda *_args, **_kwargs: upsert_calls.append("called"),
    )
    monkeypatch.setattr(ingest, "_try_push_to_redis_status", _fake_push_to_redis_status)

    def _fake_author_recent_push(*_args, **_kwargs) -> bool:  # type: ignore[no-untyped-def]
        author_recent_push_calls.append("called")
        return True

    monkeypatch.setattr(
        ingest,
        "redis_author_recent_push",
        _fake_author_recent_push,
        raising=False,
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
        verbose=False,
    )

    assert accepted == 1
    assert enqueue_error is False
    assert upsert_calls == []
    assert author_recent_push_calls == []
    assert len(list(tmp_path.glob("*.json"))) == 1


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
    monkeypatch, tmp_path, capsys
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
        verbose=True,
    )

    out = capsys.readouterr().out
    assert accepted == 1
    assert enqueue_error is False
    assert "[rss] accepted" in out
    assert "post_uid=weibo:1" in out
    assert "author=测试博主" in out
    assert "progress=1/1" in out


def test_ingest_rss_many_once_accepted_total_is_per_user(
    monkeypatch, tmp_path, capsys
) -> None:
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
    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss/user_a", "https://example.com/rss/user_b"],
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
        verbose=True,
    )

    out_lines = capsys.readouterr().out.splitlines()
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
        verbose=False,
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
        verbose=False,
    )

    assert accepted == 0
    assert enqueue_error is False
    assert sleep_calls == []


def test_ingest_rss_many_once_calls_item_ingested_callback_on_accept(
    monkeypatch, tmp_path
) -> None:
    callback_calls = {"count": 0}

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
        on_item_ingested=lambda: callback_calls.__setitem__(
            "count", int(callback_calls["count"]) + 1
        ),
        verbose=False,
    )

    assert accepted == 1
    assert enqueue_error is False
    assert int(callback_calls["count"]) == 1


def test_ingest_rss_many_once_spool_write_fallback_to_redis(
    monkeypatch, tmp_path
) -> None:
    redis_push_calls: list[str] = []

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
        "spool_write",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("disk_full")),
    )

    def _fake_redis_try_push_status(_client, _key, *, post_uid: str, **_kwargs) -> str:
        redis_push_calls.append(str(post_uid))
        return ingest.REDIS_PUSH_STATUS_PUSHED

    monkeypatch.setattr(
        ingest,
        "redis_try_push_ai_dedup_status",
        _fake_redis_try_push_status,
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
        verbose=False,
    )

    assert accepted == 1
    assert enqueue_error is True
    assert redis_push_calls == ["weibo:1"]


def test_ingest_rss_many_once_spool_write_and_redis_both_fail(
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
        "spool_write",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("disk_full")),
    )
    monkeypatch.setattr(
        ingest,
        "redis_try_push_ai_dedup_status",
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
        verbose=False,
    )

    assert accepted == 0
    assert enqueue_error is True


def test_ingest_rss_many_once_retry_same_item_after_enqueue_failure(
    monkeypatch, tmp_path
) -> None:
    spool_write_calls = {"count": 0}

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

    def _fake_spool_write(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        spool_write_calls["count"] += 1
        if int(spool_write_calls["count"]) == 1:
            raise RuntimeError("disk_full")
        return tmp_path / "ok.json"

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
    monkeypatch.setattr(ingest, "spool_write", _fake_spool_write)
    monkeypatch.setattr(
        ingest,
        "redis_try_push_ai_dedup_status",
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
        verbose=False,
    )

    assert accepted == 1
    assert enqueue_error is True
    assert int(spool_write_calls["count"]) == 2


def test_ingest_rss_many_once_prints_feed_sleep_log(
    monkeypatch, tmp_path, capsys
) -> None:
    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(entries=[])

    monkeypatch.setattr(ingest, "fetch_feed", _fake_fetch_feed)
    monkeypatch.setattr(ingest.time, "sleep", lambda sec: None)

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
        verbose=True,
    )

    out = capsys.readouterr().out
    assert accepted == 0
    assert enqueue_error is False
    assert "[rss] feed_start" in out
    assert "[rss] feed_done" in out
    assert "[rss] feed_sleep" in out
    assert "[rss] cycle_done" in out


def test_ingest_rss_many_once_reuses_single_turso_connection(
    monkeypatch, tmp_path
) -> None:
    engine_marker = object()
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

    def _fake_upsert(conn, **kwargs) -> None:
        del kwargs
        assert conn is conn_marker
        upsert_conn_ids.append(id(conn))

    monkeypatch.setattr(
        ingest, "turso_connect_autocommit", _fake_connect, raising=False
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
    monkeypatch.setattr(ingest, "upsert_pending_post", _fake_upsert)

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=engine_marker,
        spool_dir=tmp_path,
        redis_client=None,
        redis_queue_key="",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
        verbose=False,
    )

    assert accepted == 2
    assert enqueue_error is False
    assert connect_calls == [engine_marker]
    assert len(upsert_conn_ids) == 2


def test_ingest_rss_many_once_reconnects_after_write_error(
    monkeypatch, tmp_path
) -> None:
    engine_marker = object()
    first_conn = object()
    second_conn = object()
    connect_calls: list[object] = []
    upsert_calls: list[object] = []

    class _ConnContext:
        def __init__(self, conn_marker: object):
            self._conn_marker = conn_marker

        def __enter__(self):
            return self._conn_marker

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    def _fake_connect(engine):
        connect_calls.append(engine)
        if len(connect_calls) == 1:
            return _ConnContext(first_conn)
        return _ConnContext(second_conn)

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

    def _fake_upsert(conn, **kwargs) -> None:
        del kwargs
        upsert_calls.append(conn)
        if len(upsert_calls) == 1:
            raise RuntimeError("first write failed")

    monkeypatch.setattr(
        ingest, "turso_connect_autocommit", _fake_connect, raising=False
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
    monkeypatch.setattr(ingest, "upsert_pending_post", _fake_upsert)

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=engine_marker,
        spool_dir=tmp_path,
        redis_client=None,
        redis_queue_key="",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
        verbose=False,
    )

    assert accepted == 2
    assert enqueue_error is True
    assert connect_calls == [engine_marker, engine_marker]
    assert upsert_calls == [first_conn, second_conn]


def test_ingest_rss_many_once_closes_failed_connection_before_reconnect(
    monkeypatch, tmp_path
) -> None:
    engine_marker = object()
    connect_calls: list[object] = []
    upsert_calls: list[object] = []

    class _FakeConn:
        def __init__(self, name: str):
            self.name = name
            self.close_calls: list[bool] = []

        def close(self, *, broken: bool = False) -> None:
            self.close_calls.append(bool(broken))

    connections = [_FakeConn("c1"), _FakeConn("c2"), _FakeConn("c3")]

    def _fake_connect(engine):
        connect_calls.append(engine)
        return connections[len(connect_calls) - 1]

    def _fake_fetch_feed(
        url: str, timeout: float, *, retries: int = 0
    ) -> SimpleNamespace:
        del url, timeout, retries
        return SimpleNamespace(
            entries=[
                {"id": "1", "link": "https://example.com/post/1", "title": "标题1"},
                {"id": "2", "link": "https://example.com/post/2", "title": "标题2"},
                {"id": "3", "link": "https://example.com/post/3", "title": "标题3"},
            ]
        )

    def _fake_upsert(conn, **kwargs) -> None:
        del kwargs
        upsert_calls.append(conn)
        if len(upsert_calls) <= 2:
            raise RuntimeError("write failed")

    monkeypatch.setattr(
        ingest, "turso_connect_autocommit", _fake_connect, raising=False
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
    monkeypatch.setattr(ingest, "upsert_pending_post", _fake_upsert)

    accepted, enqueue_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=engine_marker,
        spool_dir=tmp_path,
        redis_client=None,
        redis_queue_key="",
        platform="weibo",
        author="",
        user_id=None,
        limit=None,
        rss_timeout=60.0,
        rss_retries=5,
        verbose=False,
    )

    assert accepted == 3
    assert enqueue_error is True
    assert connect_calls == [engine_marker, engine_marker, engine_marker]
    assert upsert_calls == [connections[0], connections[1], connections[2]]
    assert connections[0].close_calls == [False]
    assert connections[1].close_calls == [False]
    assert connections[2].close_calls == [False]
