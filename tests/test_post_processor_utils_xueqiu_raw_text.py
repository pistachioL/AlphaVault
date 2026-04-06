from __future__ import annotations

from typing import Any

from alphavault.db.turso_queue import CloudPost
from alphavault.worker.post_processor_utils import ensure_prefetched_post_persisted


def test_ensure_prefetched_post_persisted_keeps_xueqiu_raw_text(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_upsert_pending_post(  # type: ignore[no-untyped-def]
        _engine,
        *,
        post_uid: str,
        platform: str,
        platform_post_id: str,
        author: str,
        created_at: str,
        url: str,
        raw_text: str,
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
                "archived_at": archived_at,
                "ingested_at": ingested_at,
            }
        )

    monkeypatch.setattr(
        "alphavault.worker.post_processor_utils.upsert_pending_post",
        _fake_upsert_pending_post,
    )

    post = CloudPost(
        post_uid="xueqiu:comment:401390302",
        platform="xueqiu",
        platform_post_id="xueqiu:comment:401390302",
        author="泽元投资",
        created_at="2026-03-31 17:39:52+08:00",
        url="https://xueqiu.com/5992135535/381907747",
        raw_text="泽元投资：[献花花][献花花]",
        ai_retry_count=0,
    )

    ensure_prefetched_post_persisted(
        engine=object(),  # type: ignore[arg-type]
        post=post,
        archived_at="2026-03-31 17:40:00+08:00",
        ingested_at=1,
    )

    assert captured["platform"] == "xueqiu"
    assert captured["raw_text"] == "泽元投资：[献花花][献花花]"
