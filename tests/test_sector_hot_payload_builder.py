from __future__ import annotations

import libsql
from typing import cast

from alphavault.db.turso_db import TursoConnection
from alphavault.worker.sector_hot_payload_builder import build_sector_hot_payload


CREATE_POSTS_TABLE_SQL = """
CREATE TABLE posts(
  post_uid TEXT PRIMARY KEY,
  platform_post_id TEXT NOT NULL,
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
  idx INTEGER NOT NULL,
  topic_key TEXT NOT NULL,
  action TEXT NOT NULL,
  action_strength INTEGER NOT NULL,
  summary TEXT NOT NULL,
  author TEXT NOT NULL,
  created_at TEXT NOT NULL,
  cluster_keys_json TEXT NOT NULL DEFAULT '[]'
)
"""
INSERT_POST_SQL = """
INSERT INTO posts(
  post_uid, platform_post_id, author, created_at, url, raw_text, display_md, processed_at
)
VALUES (
  :post_uid, :platform_post_id, :author, :created_at, :url, :raw_text, :display_md, :processed_at
)
"""
INSERT_ASSERTION_SQL = """
INSERT INTO assertions(
  post_uid, idx, topic_key, action, action_strength, summary, author, created_at, cluster_keys_json
)
VALUES (
  :post_uid, :idx, :topic_key, :action, :action_strength, :summary, :author, :created_at, :cluster_keys_json
)
"""


def _setup_tables(conn: TursoConnection) -> None:
    conn.execute(CREATE_POSTS_TABLE_SQL)
    conn.execute(CREATE_ASSERTIONS_TABLE_SQL)


def test_build_sector_hot_payload_groups_signals_and_related_stocks() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        _setup_tables(conn)
        conn.execute(
            INSERT_POST_SQL,
            {
                "post_uid": "weibo:1",
                "platform_post_id": "1",
                "author": "alice",
                "created_at": "2099-01-01 00:00:00",
                "url": "https://example.com/weibo/1",
                "raw_text": "白酒继续走强",
                "display_md": "白酒继续走强",
                "processed_at": "2099-01-01 00:00:01",
            },
        )
        conn.execute(
            INSERT_ASSERTION_SQL,
            {
                "post_uid": "weibo:1",
                "idx": 1,
                "topic_key": "stock:600519.SH",
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "板块继续走强",
                "author": "alice",
                "created_at": "2099-01-01 00:00:00",
                "cluster_keys_json": '["white_liquor"]',
            },
        )

        payload = build_sector_hot_payload(
            conn,
            sector_key="cluster:white_liquor",
            signal_cap=10,
        )
        signals = cast(list[dict[str, str]], payload["signals"])
        related_stocks = cast(list[dict[str, str]], payload["related_stocks"])

        assert payload["entity_key"] == "cluster:white_liquor"
        assert payload["header_title"] == "white_liquor"
        assert payload["signal_total"] == 1
        assert signals[0]["summary"] == "板块继续走强"
        assert related_stocks == [
            {"stock_key": "stock:600519.SH", "mention_count": "1"}
        ]
    finally:
        conn.close()
