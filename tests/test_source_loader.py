from __future__ import annotations

from datetime import datetime

from alphavault.constants import SCHEMA_WEIBO
from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.postgres_db import PostgresConnection
from alphavault.db.sql.ui import build_assertions_query
from alphavault_reflex.services import source_loader

_POST_UID = "weibo:source_loader:1"
_ASSERTION_ID = "weibo:source_loader:1#1"


def test_build_assertions_query_reads_created_at_from_posts() -> None:
    query = build_assertions_query(
        ["post_uid", "created_at"],
        posts_table="weibo.posts",
        assertions_table="weibo.assertions",
        assertion_entities_table="weibo.assertion_entities",
        assertion_mentions_table="weibo.assertion_mentions",
        topic_cluster_topics_table="weibo.topic_cluster_topics",
    )

    assert "JOIN weibo.posts p ON p.post_uid = a.post_uid" in query
    assert "p.created_at AS created_at" in query
    assert "a.created_at AS created_at" not in query


def test_load_trade_sources_cached_reads_new_assertion_schema(
    monkeypatch,
    pg_conn,
) -> None:
    source_loader.load_trade_sources_cached.cache_clear()
    apply_cloud_schema(pg_conn, target="source", schema_name=SCHEMA_WEIBO)

    pg_conn.execute(
        f"""
INSERT INTO weibo.posts(
  post_uid, platform, platform_post_id, author, created_at, url, raw_text,
  final_status, processed_at, model, prompt_version, archived_at, ingested_at
)
VALUES (
  '{_POST_UID}', 'weibo', '1', 'alice', '2099-01-01 00:00:00',
  'https://example.com/weibo/1', '原文', 'relevant',
  '2099-01-01 00:00:01', 'gpt', 'v1', '2099-01-01 00:00:02', 1
)
"""
    )
    pg_conn.execute(
        f"""
INSERT INTO weibo.assertions(
  assertion_id, post_uid, idx, action, action_strength, summary, evidence
)
VALUES (
  '{_ASSERTION_ID}', '{_POST_UID}', 1, 'trade.buy', 3, '继续看多', '证据'
)
"""
    )
    pg_conn.execute(
        f"""
INSERT INTO weibo.assertion_entities(
  assertion_id, entity_key, entity_type, match_source, is_primary
)
VALUES
  ('{_ASSERTION_ID}', 'stock:601899.SH', 'stock', 'stock_code', 1),
  ('{_ASSERTION_ID}', 'industry:黄金', 'industry', 'industry_name', 0)
"""
    )
    pg_conn.execute(
        f"""
INSERT INTO weibo.assertion_mentions(
  assertion_id, mention_seq, mention_text, mention_norm, mention_type, evidence, confidence
)
VALUES (
  '{_ASSERTION_ID}', 1, '紫金矿业', '紫金矿业', 'stock_name', '证据', 0.91
)
"""
    )
    pg_conn.execute(
        """
INSERT INTO weibo.topic_cluster_topics(
  topic_key, cluster_key, source, confidence, created_at
)
VALUES (
  'industry:黄金', 'gold', 'manual', 1.0, '2099-01-01 00:00:00'
)
"""
    )

    conn = PostgresConnection(pg_conn, schema_name=SCHEMA_WEIBO)
    monkeypatch.setattr(
        source_loader,
        "ensure_postgres_engine",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        source_loader,
        "postgres_connect_autocommit",
        lambda _engine: conn,
    )

    posts, assertions = source_loader.load_trade_sources_cached(
        "postgresql://unused",
        "",
        SCHEMA_WEIBO,
    )

    assert [row["post_uid"] for row in posts] == [_POST_UID]
    assert [row["post_uid"] for row in assertions] == [_POST_UID]
    row = assertions[0]
    assert row["entity_key"] == "stock:601899.SH"
    assert row["stock_codes"] == ["601899.SH"]
    assert row["stock_names"] == ["紫金矿业"]
    assert row["industries"] == ["黄金"]
    assert row["confidence"] == 0.91
    assert row["author"] == "alice"
    assert row["url"] == "https://example.com/weibo/1"
    assert row["raw_text"] == "原文"

    source_loader.load_trade_sources_cached.cache_clear()


def test_load_trade_sources_rows_cached_normalizes_row_lists(monkeypatch) -> None:
    source_loader.load_trade_sources_rows_cached.cache_clear()
    monkeypatch.setattr(
        source_loader,
        "ensure_postgres_engine",
        lambda *_args, **_kwargs: object(),
    )

    class _FakeConn:
        pass

    from contextlib import contextmanager

    @contextmanager
    def _fake_connect(_engine):  # type: ignore[no-untyped-def]
        yield _FakeConn()

    monkeypatch.setattr(
        source_loader,
        "postgres_connect_autocommit",
        _fake_connect,
    )

    seen_sql: list[str] = []

    def _fake_read_sql_rows(conn, sql, params=None):  # type: ignore[no-untyped-def]
        del conn, params
        seen_sql.append(str(sql))
        if "FROM weibo.posts" in str(sql):
            return [
                {
                    "post_uid": _POST_UID,
                    "platform_post_id": "",
                    "author": "alice",
                    "created_at": "2099-01-01T00:00:00Z",
                    "url": "https://example.com/weibo/1",
                    "raw_text": "原文",
                }
            ]
        return [
            {
                "post_uid": _POST_UID,
                "idx": 1,
                "entity_key": "stock:601899.SH",
                "action": "trade.buy",
                "action_strength": 3,
                "summary": "继续看多",
                "evidence": "证据",
                "confidence": 0.91,
                "stock_codes": '["601899.SH"]',
                "stock_names": '["紫金矿业"]',
                "industries_json": '["黄金"]',
                "commodities_json": "[]",
                "indices_json": "[]",
                "author": "",
                "created_at": "",
            }
        ]

    monkeypatch.setattr(
        source_loader,
        "read_sql_rows",
        _fake_read_sql_rows,
    )

    posts, assertions = source_loader.load_trade_sources_rows_cached(
        "postgresql://unused",
        "",
        SCHEMA_WEIBO,
    )

    assert list(posts) == [
        {
            "post_uid": _POST_UID,
            "platform_post_id": "1",
            "author": "alice",
            "created_at": datetime(2099, 1, 1, 0, 0),
            "url": "https://example.com/weibo/1",
            "raw_text": "原文",
            "platform": "weibo",
            "status": "",
            "invest_score": 0.0,
            "processed_at": "",
            "source": "weibo",
        }
    ]
    assert list(assertions) == [
        {
            "post_uid": _POST_UID,
            "idx": 1,
            "entity_key": "stock:601899.SH",
            "action": "trade.buy",
            "action_strength": 3,
            "summary": "继续看多",
            "evidence": "证据",
            "confidence": 0.91,
            "stock_codes": ["601899.SH"],
            "stock_names": ["紫金矿业"],
            "industries_json": '["黄金"]',
            "commodities_json": "[]",
            "indices_json": "[]",
            "author": "alice",
            "created_at": datetime(2099, 1, 1, 0, 0),
            "source": "weibo",
            "url": "https://example.com/weibo/1",
            "raw_text": "原文",
            "industries": ["黄金"],
            "commodities": [],
            "indices": [],
        }
    ]
    assert len(seen_sql) == 2

    source_loader.load_trade_sources_rows_cached.cache_clear()
