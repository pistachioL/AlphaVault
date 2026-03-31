from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from alphavault.worker import spool


def test_flush_spool_to_turso_drops_xueqiu_display_md(monkeypatch, tmp_path) -> None:
    captured: dict[str, Any] = {}

    @contextmanager
    def _fake_connect(_engine) -> Iterator[object]:  # type: ignore[no-untyped-def]
        yield object()

    def _fake_upsert_pending_post(  # type: ignore[no-untyped-def]
        _conn,
        *,
        post_uid: str,
        platform: str,
        platform_post_id: str,
        author: str,
        created_at: str,
        url: str,
        raw_text: str,
        display_md: str,
        archived_at: str,
        ingested_at: int,
    ) -> None:
        captured.update(
            {
                "post_uid": post_uid,
                "platform": platform,
                "platform_post_id": platform_post_id,
                "author": author,
                "created_at": created_at,
                "url": url,
                "raw_text": raw_text,
                "display_md": display_md,
                "archived_at": archived_at,
                "ingested_at": ingested_at,
            }
        )

    monkeypatch.setattr(spool, "turso_connect_autocommit", _fake_connect)
    monkeypatch.setattr(spool, "upsert_pending_post", _fake_upsert_pending_post)

    post_uid = "xueqiu:comment:401390302"
    payload = {
        "post_uid": post_uid,
        "platform": "xueqiu",
        "platform_post_id": post_uid,
        "author": "泽元投资",
        "created_at": "2026-03-31 17:39:52+08:00",
        "url": "https://xueqiu.com/5992135535/381907747",
        "raw_text": "泽元投资：[献花花][献花花]",
        "display_md": "SHOULD_BE_DROPPED",
        "ingested_at": 1,
    }
    spool.spool_write(tmp_path, post_uid, payload)

    processed, had_error = spool.flush_spool_to_turso(
        spool_dir=tmp_path,
        engine=object(),  # type: ignore[arg-type]
        max_items=10,
        verbose=False,
    )

    assert processed == 1
    assert had_error is False
    assert captured["platform"] == "xueqiu"
    assert captured["raw_text"] == "泽元投资：[献花花][献花花]"
    assert captured["display_md"] == ""
