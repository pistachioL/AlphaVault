from __future__ import annotations

from pathlib import Path
from typing import cast

from alphavault.db.turso_db import TursoEngine
from alphavault.worker import assertion_outbox_consumer
from alphavault.worker.local_cache import (
    CACHE_ASSERTIONS_TABLE,
    ENV_LOCAL_CACHE_DB_PATH,
    apply_outbox_event_payload,
    open_local_cache,
    resolve_local_cache_db_path,
)


def test_rebuild_local_cache_from_outbox_replays_ai_done_and_clears_old_rows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(ENV_LOCAL_CACHE_DB_PATH, str(tmp_path / "cache.sqlite3"))
    db_path = resolve_local_cache_db_path(source_name="weibo")
    with open_local_cache(db_path=db_path) as cache_conn:
        apply_outbox_event_payload(
            cache_conn,
            payload={
                "event_type": "ai_done",
                "post_uid": "old:1",
                "author": "旧作者",
                "created_at": "2026-04-04 09:00:00",
                "assertions": [
                    {
                        "topic_key": "stock:old",
                        "action": "trade.buy",
                        "stock_codes": ["000001.SZ"],
                    }
                ],
            },
        )

    events = [
        assertion_outbox_consumer.AssertionOutboxEvent(
            id=1,
            source="weibo",
            post_uid="weibo:1",
            author="作者A",
            event_json=(
                '{"event_type":"ai_done","post_uid":"weibo:1","author":"作者A",'
                '"created_at":"2026-04-04 10:00:00","assertions":['
                '{"topic_key":"stock:601899.SH","action":"trade.buy",'
                '"stock_codes":["601899.SH"],"stock_names":["紫金矿业"]},'
                '{"topic_key":"macro:gold","action":"macro.watch"}'
                "]}"
            ),
            created_at="2026-04-04 10:00:00",
        ),
        assertion_outbox_consumer.AssertionOutboxEvent(
            id=2,
            source="weibo",
            post_uid="weibo:2",
            author="作者B",
            event_json='{"event_type":"rss_ingested","post_uid":"weibo:2"}',
            created_at="2026-04-04 10:01:00",
        ),
    ]
    calls = {"count": 0}

    def _fake_load(*_args, **_kwargs):
        calls["count"] += 1
        return events if calls["count"] == 1 else []

    monkeypatch.setattr(
        assertion_outbox_consumer,
        "_load_outbox_events",
        _fake_load,
    )

    stats = assertion_outbox_consumer.rebuild_local_cache_from_outbox(
        cast(TursoEngine, object()),
        source_name="weibo",
        verbose=False,
    )

    assert stats == {
        "processed": 2,
        "inserted": 1,
        "cursor": 2,
        "has_error": False,
    }
    with open_local_cache(db_path=db_path) as cache_conn:
        rows = cache_conn.execute(
            f"""
SELECT post_uid, topic_key, action
FROM {CACHE_ASSERTIONS_TABLE}
ORDER BY post_uid ASC, idx ASC
"""
        ).fetchall()
    assert rows == [("weibo:1", "stock:601899.SH", "trade.buy")]
