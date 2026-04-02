from __future__ import annotations

import libsql

from alphavault.db.turso_db import TursoConnection
from alphavault.worker.stock_hot_payload_builder import build_stock_hot_payload


CREATE_POSTS_TABLE_SQL = """
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
CREATE_ASSERTIONS_TABLE_SQL = """
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
INSERT_POST_SQL = """
INSERT INTO posts(post_uid, author, created_at, url, raw_text, display_md, processed_at)
VALUES (:post_uid, :author, :created_at, :url, :raw_text, :display_md, :processed_at)
"""
INSERT_ASSERTION_SQL = """
INSERT INTO assertions(post_uid, topic_key, action, action_strength, summary, author, created_at)
VALUES (:post_uid, :topic_key, :action, :action_strength, :summary, :author, :created_at)
"""


def _setup_tables(conn: TursoConnection) -> None:
    conn.execute(CREATE_POSTS_TABLE_SQL)
    conn.execute(CREATE_ASSERTIONS_TABLE_SQL)


def test_build_stock_hot_payload_includes_url_from_posts() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        _setup_tables(conn)
        conn.execute(
            INSERT_POST_SQL,
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
            INSERT_ASSERTION_SQL,
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


def test_build_stock_hot_payload_fills_missing_created_at_from_posts() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        _setup_tables(conn)
        conn.execute(
            INSERT_POST_SQL,
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
            INSERT_ASSERTION_SQL,
            {
                "post_uid": "weibo:1",
                "topic_key": "stock:601899.SH",
                "action": "trade.buy",
                "action_strength": 3,
                "summary": "看多",
                "author": "alice",
                "created_at": "2099-01-01 00:00:00 INVALID",
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
        assert first.get("created_at") == "2099-01-01 00:00"
        assert first.get("created_at_line") == "2099-01-01 00:00 · 0分钟前"
    finally:
        conn.close()
