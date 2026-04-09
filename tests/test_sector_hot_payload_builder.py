from __future__ import annotations

import libsql
from types import SimpleNamespace
from typing import Any, cast

import pandas as pd

from alphavault.constants import SCHEMA_XUEQIU
from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.libsql_db import LibsqlConnection as TursoConnection
from alphavault.db.postgres_db import PostgresConnection
from alphavault.worker import sector_hot_payload_builder
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


def _source_pg_conn(pg_conn, *, schema_name: str) -> PostgresConnection:
    apply_cloud_schema(pg_conn, target="source", schema_name=schema_name)
    return PostgresConnection(pg_conn, schema_name=schema_name)


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


def test_build_sector_hot_payload_uses_xueqiu_schema_tables(monkeypatch) -> None:
    seen_sql: list[str] = []

    def _fake_read_sql_df(conn, sql: str, params=None):  # type: ignore[no-untyped-def]
        del conn, params
        seen_sql.append(str(sql))
        if "topic_cluster_topics" in str(sql):
            return pd.DataFrame(
                [
                    {
                        "assertion_id": "xueqiu:sector_hot:1#1",
                        "post_uid": "xueqiu:sector_hot:1",
                        "stock_key": "stock:600519.SH",
                        "action": "trade.buy",
                        "action_strength": 2,
                        "summary": "雪球板块页也要能读到",
                        "created_at": "2099-01-04 00:00:00",
                    }
                ]
            )
        return pd.DataFrame(
            [
                {
                    "post_uid": "xueqiu:sector_hot:1",
                    "platform_post_id": "1",
                    "author": "alice",
                    "created_at": "2099-01-04 00:00:00",
                    "url": "https://example.com/xueqiu/sector-hot-1",
                    "raw_text": "白酒继续走强",
                }
            ]
        )

    monkeypatch.setattr(
        sector_hot_payload_builder,
        "turso_read_sql_df",
        _fake_read_sql_df,
    )

    conn = PostgresConnection(
        cast(Any, SimpleNamespace()),
        schema_name=SCHEMA_XUEQIU,
    )

    payload = build_sector_hot_payload(
        conn,
        sector_key="cluster:white_liquor",
        signal_cap=10,
    )

    assert any("FROM xueqiu.assertions a" in sql for sql in seen_sql)
    assert any(
        "JOIN xueqiu.assertion_entities sector_entities" in sql for sql in seen_sql
    )
    assert any("JOIN xueqiu.topic_cluster_topics tct" in sql for sql in seen_sql)
    assert any(
        "LEFT JOIN xueqiu.assertion_entities stock_entities" in sql for sql in seen_sql
    )
    assert any("FROM xueqiu.posts" in sql for sql in seen_sql)
    assert payload["entity_key"] == "cluster:white_liquor"


def test_build_sector_hot_payload_requires_schema_name_for_postgres() -> None:
    conn = PostgresConnection(cast(Any, SimpleNamespace()), schema_name="")

    try:
        build_sector_hot_payload(
            conn,
            sector_key="cluster:white_liquor",
            signal_cap=10,
        )
    except RuntimeError as err:
        assert str(err) == "missing_postgres_schema_name"
    else:
        raise AssertionError("expected missing_postgres_schema_name")


def test_build_sector_hot_payload_reads_xueqiu_source_schema_tables(pg_conn) -> None:
    conn = _source_pg_conn(pg_conn, schema_name=SCHEMA_XUEQIU)
    conn.execute(
        """
        INSERT INTO xueqiu.posts(
          post_uid, platform, platform_post_id, author, created_at, url, raw_text,
          final_status, processed_at, model, prompt_version, archived_at, ingested_at
        )
        VALUES (
          'xueqiu:sector_hot:1', 'xueqiu', '1', 'alice', '2099-01-04 00:00:00',
          'https://example.com/xueqiu/sector-hot-1', '白酒继续走强', 'relevant',
          '2099-01-04 00:00:01', '', '', '', 1
        )
        """
    )
    conn.execute(
        """
        INSERT INTO xueqiu.assertions(
          assertion_id, post_uid, idx, action, action_strength, summary, evidence, created_at
        )
        VALUES (
          'xueqiu:sector_hot:1#1', 'xueqiu:sector_hot:1', 1, 'trade.buy', 2,
          '雪球板块页也要能读到', '雪球板块页也要能读到', '2099-01-04 00:00:00'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO xueqiu.assertion_entities(
          assertion_id, entity_key, entity_type, match_source, is_primary
        )
        VALUES
          ('xueqiu:sector_hot:1#1', 'stock:600519.SH', 'stock', 'stock_code', 1),
          ('xueqiu:sector_hot:1#1', 'industry:白酒', 'industry', 'industry_name', 0)
        """
    )
    conn.execute(
        """
        INSERT INTO xueqiu.topic_cluster_topics(
          topic_key, cluster_key, source, confidence, created_at
        )
        VALUES (
          'industry:白酒', 'white_liquor', 'manual', 1.0, '2099-01-04 00:00:00'
        )
        """
    )

    payload = build_sector_hot_payload(
        conn,
        sector_key="cluster:white_liquor",
        signal_cap=10,
    )

    signal_top = cast(list[dict[str, str]], payload["signal_top"])
    related = cast(list[dict[str, str]], payload["related"])

    assert payload["entity_key"] == "cluster:white_liquor"
    assert signal_top
    assert signal_top[0]["post_uid"] == "xueqiu:sector_hot:1"
    assert related == [
        {
            "entity_key": "stock:600519.SH",
            "entity_type": "stock",
            "mention_count": "1",
        }
    ]
