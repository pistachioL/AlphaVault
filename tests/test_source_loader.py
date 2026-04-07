from __future__ import annotations

import libsql

from alphavault.db.turso_db import TursoConnection
from alphavault_reflex.services import source_loader


def test_load_trade_sources_cached_reads_new_assertion_schema(monkeypatch) -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    source_loader.load_trade_sources_cached.cache_clear()
    try:
        conn.execute(
            """
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
        )
        conn.execute(
            """
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
        )
        conn.execute(
            """
CREATE TABLE assertion_entities(
  assertion_id TEXT NOT NULL,
  entity_key TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  match_source TEXT NOT NULL,
  is_primary INTEGER NOT NULL DEFAULT 0
)
"""
        )
        conn.execute(
            """
CREATE TABLE assertion_mentions(
  assertion_id TEXT NOT NULL,
  mention_seq INTEGER NOT NULL,
  mention_text TEXT NOT NULL,
  mention_norm TEXT NOT NULL,
  mention_type TEXT NOT NULL,
  evidence TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0
)
"""
        )
        conn.execute(
            """
CREATE TABLE topic_cluster_topics(
  topic_key TEXT NOT NULL,
  cluster_key TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'manual',
  confidence REAL NOT NULL DEFAULT 1.0,
  created_at TEXT NOT NULL,
  PRIMARY KEY (topic_key, cluster_key)
)
"""
        )

        conn.execute(
            """
INSERT INTO posts(
  post_uid, platform_post_id, author, created_at, url, raw_text, processed_at
)
VALUES (?, ?, ?, ?, ?, ?, ?)
""",
            (
                "weibo:1",
                "1",
                "alice",
                "2099-01-01 00:00:00",
                "https://example.com/weibo/1",
                "原文",
                "2099-01-01 00:00:01",
            ),
        )
        conn.execute(
            """
INSERT INTO assertions(
  assertion_id, post_uid, idx, action, action_strength, summary, evidence, created_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""",
            (
                "weibo:1#1",
                "weibo:1",
                1,
                "trade.buy",
                3,
                "继续看多",
                "证据",
                "2099-01-01 00:00:00",
            ),
        )
        conn.execute(
            """
INSERT INTO assertion_entities(
  assertion_id, entity_key, entity_type, match_source, is_primary
)
VALUES (?, ?, ?, ?, ?)
""",
            (
                "weibo:1#1",
                "stock:601899.SH",
                "stock",
                "stock_code",
                1,
            ),
        )
        conn.execute(
            """
INSERT INTO assertion_entities(
  assertion_id, entity_key, entity_type, match_source, is_primary
)
VALUES (?, ?, ?, ?, ?)
""",
            (
                "weibo:1#1",
                "industry:黄金",
                "industry",
                "industry_name",
                0,
            ),
        )
        conn.execute(
            """
INSERT INTO assertion_mentions(
  assertion_id, mention_seq, mention_text, mention_norm, mention_type, evidence, confidence
)
VALUES (?, ?, ?, ?, ?, ?, ?)
""",
            (
                "weibo:1#1",
                1,
                "紫金矿业",
                "紫金矿业",
                "stock_name",
                "证据",
                0.91,
            ),
        )
        conn.execute(
            """
INSERT INTO topic_cluster_topics(
  topic_key, cluster_key, source, confidence, created_at
)
VALUES (?, ?, ?, ?, ?)
""",
            (
                "industry:黄金",
                "gold",
                "manual",
                1.0,
                "2099-01-01 00:00:00",
            ),
        )

        monkeypatch.setattr(
            source_loader,
            "ensure_turso_engine",
            lambda *_args, **_kwargs: object(),
        )
        monkeypatch.setattr(
            source_loader,
            "turso_connect_autocommit",
            lambda _engine: conn,
        )

        posts, assertions = source_loader.load_trade_sources_cached(
            "libsql://example.turso.io",
            "token",
            "weibo",
        )

        assert list(posts["post_uid"]) == ["weibo:1"]
        assert list(assertions["post_uid"]) == ["weibo:1"]
        row = assertions.iloc[0]
        assert row["entity_key"] == "stock:601899.SH"
        assert row["stock_codes"] == ["601899.SH"]
        assert row["stock_names"] == ["紫金矿业"]
        assert row["industries"] == ["黄金"]
        assert row["confidence"] == 0.91
        assert row["author"] == "alice"
        assert row["url"] == "https://example.com/weibo/1"
        assert row["raw_text"] == "原文"
    finally:
        source_loader.load_trade_sources_cached.cache_clear()
        conn.close()
