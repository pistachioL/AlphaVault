from __future__ import annotations

from types import SimpleNamespace

from alphavault_reflex.services import stock_hot_read


def _setup_single_source(monkeypatch) -> None:
    monkeypatch.setattr(stock_hot_read, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        stock_hot_read,
        "load_configured_postgres_sources_from_env",
        lambda: [SimpleNamespace(name="weibo", url="u1", token="t1")],
    )
    monkeypatch.setattr(
        stock_hot_read, "ensure_postgres_engine", lambda *_args: object()
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_stock_alias_relations_from_env",
        lambda: ([], ""),
    )
    stock_hot_read.clear_stock_hot_read_caches()


def _signal_row(
    *,
    post_uid: str = "weibo:1",
    summary: str = "现查分页",
    created_at: str = "2099-01-01 00:00",
    created_at_line: str = "2099-01-01 00:00 · 0分钟前",
) -> dict[str, str]:
    return {
        "post_uid": post_uid,
        "summary": summary,
        "action": "trade.buy",
        "action_strength": "high",
        "author": "alice",
        "created_at": created_at,
        "created_at_line": created_at_line,
        "url": "https://example.com/post",
    }


def test_load_stock_cached_view_queries_rows_directly_without_snapshot_lookup(
    monkeypatch,
) -> None:
    _setup_single_source(monkeypatch)
    seen_calls: list[dict[str, object]] = []

    def _fake_load_stock_page_rows_from_env(
        stock_key: str,
        *,
        signal_page: int,
        signal_page_size: int,
        author: str = "",
    ) -> dict[str, object]:
        seen_calls.append(
            {
                "stock_key": stock_key,
                "signal_page": signal_page,
                "signal_page_size": signal_page_size,
                "author": author,
            }
        )
        return {
            "entity_key": stock_key,
            "page_title": stock_key.removeprefix("stock:"),
            "signals": [{"post_uid": "weibo:1", "summary": "现查分页"}],
            "signal_total": 1,
            "signal_page": signal_page,
            "signal_page_size": signal_page_size,
            "load_error": "",
        }

    monkeypatch.setattr(
        stock_hot_read,
        "load_entity_page_signal_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should_not_read_snapshot")
        ),
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_stock_page_rows_from_env",
        _fake_load_stock_page_rows_from_env,
        raising=False,
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_worker_job_cursor",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should_not_read_worker_progress")
        ),
    )

    payload = stock_hot_read.load_stock_cached_view_from_env(
        "600519.SH",
        signal_page=2,
        signal_page_size=20,
        author="alice",
    )

    assert seen_calls == [
        {
            "stock_key": "stock:600519.SH",
            "signal_page": 2,
            "signal_page_size": 20,
            "author": "alice",
        }
    ]
    assert payload["signals"] == [{"post_uid": "weibo:1", "summary": "现查分页"}]
    assert payload["signal_total"] == 1


def test_load_stock_cached_view_without_running_worker_has_no_processing_warning(
    monkeypatch,
) -> None:
    _setup_single_source(monkeypatch)
    monkeypatch.setattr(
        stock_hot_read,
        "load_entity_page_signal_snapshot",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_worker_job_cursor",
        lambda *_args, **_kwargs: "",
    )

    payload = stock_hot_read.load_stock_cached_view_from_env(
        "600519.SH",
        signal_page=1,
        signal_page_size=5,
    )

    assert str(payload.get("load_warning") or "").strip() == ""
    assert bool(payload.get("worker_running", True)) is False


def test_load_stock_cached_view_normalizes_prefixed_cn_slug_before_snapshot_lookup(
    monkeypatch,
) -> None:
    _setup_single_source(monkeypatch)
    seen_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        stock_hot_read,
        "load_entity_page_signal_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should_not_read_snapshot")
        ),
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_worker_job_cursor",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should_not_read_worker_progress")
        ),
    )

    def _fake_load_source_signal_page(
        _source,
        *,
        stock_keys: list[str],
        author: str,
        signal_page: int,
        signal_page_size: int,
        signal_window_days: int,
    ) -> tuple[list[dict[str, str]], int]:
        seen_calls.append(
            {
                "stock_keys": list(stock_keys),
                "author": author,
                "signal_page": signal_page,
                "signal_page_size": signal_page_size,
                "signal_window_days": signal_window_days,
            }
        )
        return ([_signal_row(post_uid="xueqiu:1", summary="规范 key 先命中")], 1)

    monkeypatch.setattr(
        stock_hot_read,
        "_load_source_signal_page",
        _fake_load_source_signal_page,
    )

    payload = stock_hot_read.load_stock_page_rows_from_env(
        "SZ000725.US",
        signal_page=1,
        signal_page_size=5,
    )

    assert seen_calls == [
        {
            "stock_keys": ["stock:000725.SZ"],
            "author": "",
            "signal_page": 1,
            "signal_page_size": 5,
            "signal_window_days": 30,
        }
    ]
    assert payload["entity_key"] == "stock:000725.SZ"
    assert payload["page_title"] == "000725.SZ"
    assert payload["signal_total"] == 1


def test_load_stock_cached_view_passes_author_to_snapshot_lookup(
    monkeypatch,
) -> None:
    _setup_single_source(monkeypatch)
    seen_calls: list[dict[str, str]] = []

    def _fake_load_source_signal_page(
        _source,
        *,
        stock_keys: list[str],
        author: str,
        signal_page: int,
        signal_page_size: int,
        signal_window_days: int,
    ) -> tuple[list[dict[str, str]], int]:
        del signal_page, signal_page_size, signal_window_days
        seen_calls.append({"stock_key": stock_keys[0], "author": author})
        return ([_signal_row(summary="只看 alice")], 1)

    monkeypatch.setattr(
        stock_hot_read,
        "_load_source_signal_page",
        _fake_load_source_signal_page,
    )

    payload = stock_hot_read.load_stock_page_rows_from_env(
        "600519.SH",
        signal_page=1,
        signal_page_size=5,
        author="alice",
    )

    assert seen_calls == [{"stock_key": "stock:600519.SH", "author": "alice"}]
    assert payload["signal_total"] == 1


def test_load_stock_cached_view_falls_back_to_legacy_prefixed_cn_snapshot(
    monkeypatch,
) -> None:
    _setup_single_source(monkeypatch)
    seen_stock_keys: list[str] = []

    def _fake_load_source_signal_page(
        _source,
        *,
        stock_keys: list[str],
        author: str,
        signal_page: int,
        signal_page_size: int,
        signal_window_days: int,
    ) -> tuple[list[dict[str, str]], int]:
        del author, signal_page, signal_page_size, signal_window_days
        stock_key = stock_keys[0]
        seen_stock_keys.append(stock_key)
        if stock_key == "stock:000725.SZ":
            return ([], 0)
        return ([_signal_row(post_uid="xueqiu:1", summary="旧 key 回退命中")], 1)

    monkeypatch.setattr(
        stock_hot_read,
        "_load_source_signal_page",
        _fake_load_source_signal_page,
    )

    payload = stock_hot_read.load_stock_page_rows_from_env(
        "SZ000725.US",
        signal_page=1,
        signal_page_size=5,
    )

    assert seen_stock_keys == ["stock:000725.SZ", "stock:SZ000725.US"]
    assert payload["entity_key"] == "stock:000725.SZ"
    assert payload["page_title"] == "000725.SZ"
    assert payload["signal_total"] == 1


def test_resolve_stock_key_candidates_appends_alias_keys_for_canonical_stock(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        stock_hot_read,
        "load_stock_alias_relations_from_env",
        lambda: (
            [
                {
                    "relation_type": "stock_alias",
                    "left_key": "stock:000725.SZ",
                    "right_key": "stock:京东方A",
                    "relation_label": "alias_of",
                }
            ],
            "",
        ),
    )

    stock_keys, err = stock_hot_read._resolve_stock_key_candidates("stock:000725.SZ")

    assert err == ""
    assert stock_keys == ["stock:000725.SZ", "stock:SZ000725.US", "stock:京东方A"]


def test_load_stock_cached_view_keeps_canonical_entity_key_when_alias_snapshot_hits(
    monkeypatch,
) -> None:
    _setup_single_source(monkeypatch)
    monkeypatch.setattr(
        stock_hot_read,
        "load_stock_alias_relations_from_env",
        lambda: (
            [
                {
                    "relation_type": "stock_alias",
                    "left_key": "stock:000725.SZ",
                    "right_key": "stock:京东方A",
                    "relation_label": "alias_of",
                }
            ],
            "",
        ),
    )
    seen_stock_keys: list[str] = []

    def _fake_load_source_signal_page(
        _source,
        *,
        stock_keys: list[str],
        author: str,
        signal_page: int,
        signal_page_size: int,
        signal_window_days: int,
    ) -> tuple[list[dict[str, str]], int]:
        del author, signal_page, signal_page_size, signal_window_days
        stock_key = stock_keys[0]
        seen_stock_keys.append(stock_key)
        if stock_key != "stock:京东方A":
            return ([], 0)
        return ([_signal_row(post_uid="xueqiu:2", summary="alias 行情命中")], 1)

    monkeypatch.setattr(
        stock_hot_read,
        "_load_source_signal_page",
        _fake_load_source_signal_page,
    )

    payload = stock_hot_read.load_stock_page_rows_from_env(
        "000725.SZ",
        signal_page=1,
        signal_page_size=5,
    )

    assert seen_stock_keys == ["stock:000725.SZ", "stock:SZ000725.US", "stock:京东方A"]
    assert payload["entity_key"] == "stock:000725.SZ"
    assert payload["page_title"] == "000725.SZ"
    assert payload["signal_total"] == 1


def test_load_stock_cached_view_ignores_worker_state_reads(
    monkeypatch,
) -> None:
    _setup_single_source(monkeypatch)
    monkeypatch.setattr(
        stock_hot_read,
        "load_entity_page_signal_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should_not_read_snapshot")
        ),
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_worker_job_cursor",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should_not_read_worker_progress")
        ),
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_stock_page_rows_from_env",
        lambda stock_key, *, signal_page, signal_page_size, author="": {
            "entity_key": stock_key,
            "page_title": stock_key.removeprefix("stock:"),
            "signals": [_signal_row()],
            "signal_total": 1,
            "signal_page": signal_page,
            "signal_page_size": signal_page_size,
            "related_sectors": [],
            "load_error": "",
            "load_warning": "",
            "worker_status_text": "",
            "worker_next_run_at": "",
            "worker_cycle_updated_at": "",
            "worker_running": False,
        },
    )

    payload = stock_hot_read.load_stock_cached_view_from_env(
        "600519.SH",
        signal_page=1,
        signal_page_size=5,
    )

    assert str(payload.get("load_warning") or "") == ""
    assert bool(payload.get("worker_running", True)) is False
    assert str(payload.get("worker_status_text") or "") == ""


def test_load_stock_cached_view_does_not_emit_nat_created_at(monkeypatch) -> None:
    _setup_single_source(monkeypatch)
    monkeypatch.setattr(
        stock_hot_read,
        "_load_source_signal_page",
        lambda _source, **_kwargs: (
            [
                _signal_row(
                    post_uid="p1",
                    summary="s",
                    created_at="",
                    created_at_line="",
                )
            ],
            1,
        ),
    )

    payload = stock_hot_read.load_stock_page_rows_from_env(
        "600519.SH",
        signal_page=1,
        signal_page_size=5,
    )

    signals = payload.get("signals") or []
    assert isinstance(signals, list)
    assert signals
    first = signals[0]
    assert isinstance(first, dict)
    assert first.get("created_at") == ""


def test_load_stock_cached_view_has_no_backfill_posts(monkeypatch) -> None:
    _setup_single_source(monkeypatch)
    monkeypatch.setattr(
        stock_hot_read,
        "load_entity_page_signal_snapshot",
        lambda *_args, **_kwargs: {
            "entity_key": "stock:600519.SH",
            "entity_type": "stock",
            "header": {"title": "600519.SH"},
            "signal_top": [],
            "related": [
                {
                    "entity_key": "cluster:white_liquor",
                    "entity_type": "sector",
                    "mention_count": "1",
                }
            ],
            "counters": {"signal_total": 0},
        },
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_worker_job_cursor",
        lambda *_args, **_kwargs: "",
    )

    payload = stock_hot_read.load_stock_cached_view_from_env(
        "600519.SH",
        signal_page=1,
        signal_page_size=5,
    )

    assert "pending_candidates" not in payload
    assert "backfill_posts" not in payload


def test_load_stock_cached_view_returns_relation_error_when_standard_alias_fails(
    monkeypatch,
) -> None:
    monkeypatch.setattr(stock_hot_read, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        stock_hot_read,
        "load_configured_postgres_sources_from_env",
        lambda: [SimpleNamespace(name="weibo", url="u1", token="t1")],
    )
    monkeypatch.setattr(
        stock_hot_read, "ensure_postgres_engine", lambda *_args: object()
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_stock_alias_relations_from_env",
        lambda: ([], "postgres_connect_error:standard:RuntimeError"),
    )
    monkeypatch.setattr(
        stock_hot_read,
        "load_entity_page_signal_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should_not_load_source_payload")
        ),
    )
    stock_hot_read.clear_stock_hot_read_caches()

    payload = stock_hot_read.load_stock_cached_view_from_env(
        "600519.SH",
        signal_page=1,
        signal_page_size=5,
    )

    assert payload["load_error"] == "postgres_connect_error:standard:RuntimeError"
    assert payload["signals"] == []
