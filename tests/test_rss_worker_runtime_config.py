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

    inserted, turso_error = ingest.ingest_rss_many_once(
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
    assert inserted == 0
    assert turso_error is False


def test_build_rss_inserted_log_line_contains_id_author_and_progress() -> None:
    line = ingest._build_rss_inserted_log_line(
        platform="weibo",
        post_uid="weibo:5281206301104087",
        author="博主A",
        entry_index=1,
        entry_total=25,
        feed_index=1,
        feed_total=3,
        inserted_total=7,
    )

    assert "[rss] inserted" in line
    assert "post_uid=weibo:5281206301104087" in line
    assert "author=博主A" in line
    assert "progress=1/25" in line
    assert "feed_progress=1/3" in line
    assert "inserted_total=7" in line


def test_ingest_rss_many_once_prints_inserted_log_with_progress(
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
    monkeypatch.setattr(ingest, "upsert_pending_post", lambda *args, **kwargs: None)

    inserted, turso_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss"],
        engine=object(),
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
    assert inserted == 1
    assert turso_error is False
    assert "[rss] inserted" in out
    assert "post_uid=weibo:1" in out
    assert "author=测试博主" in out
    assert "progress=1/1" in out


def test_ingest_rss_many_once_inserted_total_is_per_user(
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
    monkeypatch.setattr(ingest, "upsert_pending_post", lambda *args, **kwargs: None)

    inserted, turso_error = ingest.ingest_rss_many_once(
        rss_urls=["https://example.com/rss/user_a", "https://example.com/rss/user_b"],
        engine=object(),
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
    assert inserted == 4
    assert turso_error is False
    assert any(
        "post_uid=weibo:user_a:1" in line and "inserted_total=1" in line
        for line in out_lines
    )
    assert any(
        "post_uid=weibo:user_a:2" in line and "inserted_total=2" in line
        for line in out_lines
    )
    assert any(
        "post_uid=weibo:user_b:1" in line and "inserted_total=1" in line
        for line in out_lines
    )
    assert any(
        "post_uid=weibo:user_b:2" in line and "inserted_total=2" in line
        for line in out_lines
    )
