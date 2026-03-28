from __future__ import annotations

from alphavault.worker import spool


def test_flush_spool_to_turso_reuses_single_connection(monkeypatch, tmp_path) -> None:
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

    def _fake_upsert(conn, **kwargs) -> None:
        del kwargs
        assert conn is conn_marker
        upsert_conn_ids.append(id(conn))

    spool.spool_write(
        tmp_path,
        "weibo:1",
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
    )
    spool.spool_write(
        tmp_path,
        "weibo:2",
        {
            "post_uid": "weibo:2",
            "platform": "weibo",
            "platform_post_id": "2",
            "author": "作者B",
            "created_at": "2026-03-28 10:01:00",
            "url": "https://example.com/post/2",
            "raw_text": "文本2",
            "display_md": "",
            "ingested_at": 101,
        },
    )

    monkeypatch.setattr(spool, "turso_connect_autocommit", _fake_connect, raising=False)
    monkeypatch.setattr(spool, "upsert_pending_post", _fake_upsert)

    processed, turso_error = spool.flush_spool_to_turso(
        spool_dir=tmp_path,
        engine=engine_marker,
        max_items=10,
        verbose=False,
    )

    assert processed == 2
    assert turso_error is False
    assert connect_calls == [engine_marker]
    assert len(upsert_conn_ids) == 2
    assert list(tmp_path.glob("*.json")) == []
