from __future__ import annotations

import libsql

from alphavault.db.turso_db import TursoConnection
from alphavault.worker.stock_hot_payload_builder import build_stock_hot_payload


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


def _setup_tables(conn: TursoConnection) -> None:
    conn.execute(CREATE_POSTS_TABLE_SQL)
    conn.execute(CREATE_ASSERTIONS_TABLE_SQL)
    conn.execute(CREATE_ASSERTION_ENTITIES_TABLE_SQL)


def test_build_stock_hot_payload_includes_url_from_posts() -> None:
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
                "raw_text": "原文",
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
                "action_strength": 3,
                "summary": "看多",
                "evidence": "看多",
                "created_at": "2099-01-01 00:00:00",
            },
        )
        conn.execute(
            INSERT_ASSERTION_ENTITY_SQL,
            {
                "assertion_id": "weibo:1#1",
                "entity_key": "stock:601899.SH",
                "entity_type": "stock",
                "match_source": "stock_code",
                "is_primary": 1,
            },
        )

        payload = build_stock_hot_payload(
            conn,
            stock_key="stock:601899.SH",
            signal_window_days=30,
            signal_cap=10,
        )

        signal_top = payload.get("signal_top") or []
        assert isinstance(signal_top, list)
        assert signal_top
        first = signal_top[0]
        assert isinstance(first, dict)
        assert payload.get("entity_type") == "stock"
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
                "platform_post_id": "1",
                "author": "alice",
                "created_at": "2099-01-01 00:00:00",
                "url": "https://example.com/weibo/1",
                "raw_text": "原文",
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
                "action_strength": 3,
                "summary": "看多",
                "evidence": "看多",
                "created_at": "2099-01-01 00:00:00 INVALID",
            },
        )
        conn.execute(
            INSERT_ASSERTION_ENTITY_SQL,
            {
                "assertion_id": "weibo:1#1",
                "entity_key": "stock:601899.SH",
                "entity_type": "stock",
                "match_source": "stock_code",
                "is_primary": 1,
            },
        )

        payload = build_stock_hot_payload(
            conn,
            stock_key="stock:601899.SH",
            signal_window_days=30,
            signal_cap=10,
        )

        signal_top = payload.get("signal_top") or []
        assert isinstance(signal_top, list)
        assert signal_top
        first = signal_top[0]
        assert isinstance(first, dict)
        assert first.get("created_at") == "2099-01-01 00:00"
        assert first.get("created_at_line") == "2099-01-01 00:00 · 0分钟前"
    finally:
        conn.close()


def test_build_stock_hot_payload_reads_stock_entity_key_instead_of_topic_key() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        _setup_tables(conn)
        conn.execute(
            INSERT_POST_SQL,
            {
                "post_uid": "weibo:2",
                "platform_post_id": "2",
                "author": "alice",
                "created_at": "2099-01-02 00:00:00",
                "url": "https://example.com/weibo/2",
                "raw_text": "原文",
                "processed_at": "2099-01-02 00:00:01",
            },
        )
        conn.execute(
            INSERT_ASSERTION_SQL,
            {
                "assertion_id": "weibo:2#1",
                "post_uid": "weibo:2",
                "idx": 1,
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "别名行也要进正式个股页",
                "evidence": "别名行也要进正式个股页",
                "created_at": "2099-01-02 00:00:00",
            },
        )
        conn.execute(
            INSERT_ASSERTION_ENTITY_SQL,
            {
                "assertion_id": "weibo:2#1",
                "entity_key": "stock:601899.SH",
                "entity_type": "stock",
                "match_source": "stock_alias",
                "is_primary": 1,
            },
        )

        payload = build_stock_hot_payload(
            conn,
            stock_key="stock:601899.SH",
            signal_window_days=30,
            signal_cap=10,
        )

        signal_top = payload.get("signal_top") or []
        assert isinstance(signal_top, list)
        assert signal_top
        first = signal_top[0]
        assert isinstance(first, dict)
        header = payload.get("header") or {}
        counters = payload.get("counters") or {}
        assert first.get("post_uid") == "weibo:2"
        assert first.get("summary") == "别名行也要进正式个股页"
        assert header == {"title": "601899.SH"}
        assert counters == {"signal_total": 1}
    finally:
        conn.close()


def test_build_stock_hot_payload_reads_legacy_prefixed_cn_entity_key_for_canonical_stock() -> (
    None
):
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        _setup_tables(conn)
        conn.execute(
            INSERT_POST_SQL,
            {
                "post_uid": "xueqiu:1",
                "platform_post_id": "1",
                "author": "alice",
                "created_at": "2099-01-03 00:00:00",
                "url": "https://example.com/xueqiu/1",
                "raw_text": "原文",
                "processed_at": "2099-01-03 00:00:01",
            },
        )
        conn.execute(
            INSERT_ASSERTION_SQL,
            {
                "assertion_id": "xueqiu:1#1",
                "post_uid": "xueqiu:1",
                "idx": 1,
                "action": "trade.buy",
                "action_strength": 2,
                "summary": "旧坏 key 也要进规范个股页",
                "evidence": "旧坏 key 也要进规范个股页",
                "created_at": "2099-01-03 00:00:00",
            },
        )
        conn.execute(
            INSERT_ASSERTION_ENTITY_SQL,
            {
                "assertion_id": "xueqiu:1#1",
                "entity_key": "stock:SZ000725.US",
                "entity_type": "stock",
                "match_source": "stock_code",
                "is_primary": 1,
            },
        )

        payload = build_stock_hot_payload(
            conn,
            stock_key="stock:000725.SZ",
            signal_window_days=30,
            signal_cap=10,
        )

        signal_top = payload.get("signal_top") or []
        assert isinstance(signal_top, list)
        assert signal_top
        assert payload.get("entity_key") == "stock:000725.SZ"
        assert (payload.get("header") or {}) == {"title": "000725.SZ"}
        assert signal_top[0].get("post_uid") == "xueqiu:1"
    finally:
        conn.close()
