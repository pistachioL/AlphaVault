from __future__ import annotations

import libsql

from alphavault.db.turso_db import TursoConnection
from alphavault.worker.stock_hot_payload_builder import build_stock_hot_payload


def test_build_stock_hot_payload_includes_url_from_posts() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        conn.execute(
            """
            CREATE TABLE posts(
              post_uid TEXT PRIMARY KEY,
              author TEXT NOT NULL,
              created_at TEXT NOT NULL,
              url TEXT NOT NULL,
              raw_text TEXT NOT NULL,
              display_md TEXT NOT NULL,
              processed_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE assertions(
              post_uid TEXT NOT NULL,
              topic_key TEXT NOT NULL,
              action TEXT NOT NULL,
              action_strength INTEGER NOT NULL,
              summary TEXT NOT NULL,
              author TEXT NOT NULL,
              created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO posts(post_uid, author, created_at, url, raw_text, display_md, processed_at)
            VALUES (:post_uid, :author, :created_at, :url, :raw_text, :display_md, :processed_at)
            """,
            {
                "post_uid": "weibo:1",
                "author": "alice",
                "created_at": "2099-01-01 00:00:00",
                "url": "https://example.com/weibo/1",
                "raw_text": "原文",
                "display_md": "原文",
                "processed_at": "2099-01-01 00:00:01",
            },
        )
        conn.execute(
            """
            INSERT INTO assertions(post_uid, topic_key, action, action_strength, summary, author, created_at)
            VALUES (:post_uid, :topic_key, :action, :action_strength, :summary, :author, :created_at)
            """,
            {
                "post_uid": "weibo:1",
                "topic_key": "stock:601899.SH",
                "action": "trade.buy",
                "action_strength": 3,
                "summary": "看多",
                "author": "alice",
                "created_at": "2099-01-01 00:00:00",
            },
        )

        payload = build_stock_hot_payload(
            conn,
            stock_key="stock:601899.SH",
            signal_window_days=30,
            signal_cap=10,
        )

        signals = payload.get("signals") or []
        assert isinstance(signals, list)
        assert signals
        first = signals[0]
        assert isinstance(first, dict)
        assert first.get("post_uid") == "weibo:1"
        assert first.get("url") == "https://example.com/weibo/1"
    finally:
        conn.close()
