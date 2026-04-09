from __future__ import annotations

import libsql
from typing import cast

from alphavault.db.libsql_db import LibsqlConnection as TursoConnection
from alphavault.db.postgres_db import PostgresConnection
from alphavault.worker.sector_hot_payload_builder import build_sector_hot_payload


CREATE_POSTS_TABLE_SQL = """
CREATE TABLE posts(
  post_uid TEXT PRIMARY KEY,
  platform_post_id TEXT NOT NULL,
  author TEXT NOT NULL,
  created_at TEXT NOT NULL,
  url TEXT NOT NULL,
  raw_text TEXT NOT NULL,
  processed_at TEXT NOT NULL
)
"""
CREATE_ASSERTIONS_TABLE_SQL = """
CREATE TABLE assertions(
  assertion_id TEXT PRIMARY KEY,
  post_uid TEXT NOT NULL,
  idx INTEGER NOT NULL,
  action TEXT NOT NULL,
  action_strength INTEGER NOT NULL,
  summary TEXT NOT NULL,
  evidence TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(post_uid, idx)
)
"""
CREATE_ASSERTION_ENTITIES_TABLE_SQL = """
CREATE TABLE assertion_entities(
  assertion_id TEXT NOT NULL,
  entity_key TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  match_source TEXT NOT NULL,
  is_primary INTEGER NOT NULL DEFAULT 0
)
"""
CREATE_TOPIC_CLUSTER_TOPICS_TABLE_SQL = """
CREATE TABLE topic_cluster_topics(
  topic_key TEXT NOT NULL,
  cluster_key TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'manual',
  confidence REAL NOT NULL DEFAULT 1.0,
  created_at TEXT NOT NULL,
  PRIMARY KEY (topic_key, cluster_key)
)
"""
INSERT_POST_SQL = """
INSERT INTO posts(
  post_uid, platform_post_id, author, created_at, url, raw_text, processed_at
)
VALUES (
  :post_uid, :platform_post_id, :author, :created_at, :url, :raw_text, :processed_at
)
"""
INSERT_ASSERTION_SQL = """
INSERT INTO assertions(
  assertion_id, post_uid, idx, action, action_strength, summary, evidence, created_at
)
VALUES (
  :assertion_id, :post_uid, :idx, :action, :action_strength, :summary, :evidence, :created_at
)
"""
INSERT_ASSERTION_ENTITY_SQL = """
INSERT INTO assertion_entities(assertion_id, entity_key, entity_type, match_source, is_primary)
VALUES (:assertion_id, :entity_key, :entity_type, :match_source, :is_primary)
"""
INSERT_TOPIC_CLUSTER_TOPIC_SQL = """
INSERT INTO topic_cluster_topics(topic_key, cluster_key, source, confidence, created_at)
VALUES (:topic_key, :cluster_key, :source, :confidence, :created_at)
"""


def _memory_conn() -> PostgresConnection:
    return cast(
        PostgresConnection,
        TursoConnection(libsql.connect(":memory:", isolation_level=None)),
    )


def _setup_tables(conn: PostgresConnection) -> None:
    conn.execute(CREATE_POSTS_TABLE_SQL)
    conn.execute(CREATE_ASSERTIONS_TABLE_SQL)
    conn.execute(CREATE_ASSERTION_ENTITIES_TABLE_SQL)
    conn.execute(CREATE_TOPIC_CLUSTER_TOPICS_TABLE_SQL)


def test_build_sector_hot_payload_groups_signals_and_related_stocks() -> None:
    conn = _memory_conn()
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
                "processed_at": "2099-01-01 00:00:01",
            },
        )
        conn.execute(
            INSERT_ASSERTION_SQL,
            {
                "assertion_id": "weibo:1#1",
                "post_uid": "weibo:1",
                "idx": 1,
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "板块继续走强",
                "evidence": "白酒继续走强",
                "created_at": "2099-01-01 00:00:00",
            },
        )
        conn.execute(
            INSERT_ASSERTION_ENTITY_SQL,
            {
                "assertion_id": "weibo:1#1",
                "entity_key": "stock:600519.SH",
                "entity_type": "stock",
                "match_source": "stock_code",
                "is_primary": 1,
            },
        )
        conn.execute(
            INSERT_ASSERTION_ENTITY_SQL,
            {
                "assertion_id": "weibo:1#1",
                "entity_key": "industry:白酒",
                "entity_type": "industry",
                "match_source": "industry_name",
                "is_primary": 0,
            },
        )
        conn.execute(
            INSERT_TOPIC_CLUSTER_TOPIC_SQL,
            {
                "topic_key": "industry:白酒",
                "cluster_key": "white_liquor",
                "source": "manual",
                "confidence": 1.0,
                "created_at": "2099-01-01 00:00:00",
            },
        )

        payload = build_sector_hot_payload(
            conn,
            sector_key="cluster:white_liquor",
            signal_cap=10,
        )
        signal_top = cast(list[dict[str, str]], payload["signal_top"])
        related = cast(list[dict[str, str]], payload["related"])
        header = cast(dict[str, str], payload["header"])
        counters = cast(dict[str, int], payload["counters"])

        assert payload["entity_key"] == "cluster:white_liquor"
        assert payload["entity_type"] == "sector"
        assert header["title"] == "white_liquor"
        assert counters["signal_total"] == 1
        assert signal_top[0]["summary"] == "板块继续走强"
        assert related == [
            {
                "entity_key": "stock:600519.SH",
                "entity_type": "stock",
                "mention_count": "1",
            }
        ]
    finally:
        conn.close()
