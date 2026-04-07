from __future__ import annotations

from types import SimpleNamespace

import libsql
import pandas as pd

from alphavault.db.turso_db import TursoConnection
from alphavault_reflex.services import stock_fast_loader


def test_load_stock_sources_fast_from_env_returns_partial_error(monkeypatch) -> None:
    monkeypatch.setattr(stock_fast_loader, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        stock_fast_loader,
        "load_configured_turso_sources_from_env",
        lambda: [
            SimpleNamespace(name="weibo", url="u1", token="t1"),
            SimpleNamespace(name="xueqiu", url="u2", token="t2"),
        ],
    )

    def _fake_fast_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        stock_key: str,
        stock_code: str,
        per_source_limit: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        del db_url, auth_token, stock_key, stock_code, per_source_limit
        if source_name == "xueqiu":
            raise RuntimeError("boom")
        posts = pd.DataFrame([{"post_uid": "weibo:1"}])
        assertions = pd.DataFrame(
            [{"post_uid": "weibo:1", "entity_key": "stock:03316.HK"}]
        )
        return posts, assertions

    posts, assertions, err = stock_fast_loader.load_stock_sources_fast_from_env(
        "03316.HK",
        per_source_limit=16,
        load_cached_fn=_fake_fast_cached,
    )

    assert not posts.empty
    assert not assertions.empty
    assert err.startswith("partial_source_error:")


def test_load_stock_sources_fast_from_env_normalizes_stock_key(monkeypatch) -> None:
    monkeypatch.setattr(stock_fast_loader, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        stock_fast_loader,
        "load_configured_turso_sources_from_env",
        lambda: [SimpleNamespace(name="weibo", url="u1", token="t1")],
    )
    seen_stock_keys: list[str] = []

    def _fake_fast_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        stock_key: str,
        stock_code: str,
        per_source_limit: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        del db_url, auth_token, source_name, stock_code, per_source_limit
        seen_stock_keys.append(stock_key)
        return pd.DataFrame(), pd.DataFrame()

    _posts, _assertions, _err = stock_fast_loader.load_stock_sources_fast_from_env(
        "03316.HK",
        load_cached_fn=_fake_fast_cached,
    )
    assert seen_stock_keys == ["stock:03316.HK"]


def test_load_stock_trade_sources_fast_cached_reads_stock_entity_key(
    monkeypatch,
) -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    stock_fast_loader.load_stock_trade_sources_fast_cached.cache_clear()
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
                "weibo:2",
                "2",
                "alice",
                "2099-01-02 00:00:00",
                "https://example.com/weibo/2",
                "原文",
                "2099-01-02 00:00:01",
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
                "weibo:2#1",
                "weibo:2",
                1,
                "trade.buy",
                2,
                "别名行也要进正式个股页",
                "别名行也要进正式个股页",
                "2099-01-02 00:00:00",
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
                "weibo:2#1",
                "stock:601899.SH",
                "stock",
                "stock_alias",
                1,
            ),
        )

        monkeypatch.setattr(
            stock_fast_loader,
            "ensure_turso_engine",
            lambda *_args, **_kwargs: object(),
        )
        monkeypatch.setattr(
            stock_fast_loader,
            "turso_connect_autocommit",
            lambda _engine: conn,
        )

        posts, assertions = stock_fast_loader.load_stock_trade_sources_fast_cached(
            "libsql://example.turso.io",
            "token",
            "weibo",
            "stock:601899.SH",
            "601899.SH",
            16,
        )

        assert list(posts["post_uid"]) == ["weibo:2"]
        assert list(assertions["post_uid"]) == ["weibo:2"]
        assert assertions.iloc[0]["entity_key"] == "stock:601899.SH"
        assert assertions.iloc[0]["summary"] == "别名行也要进正式个股页"
    finally:
        stock_fast_loader.load_stock_trade_sources_fast_cached.cache_clear()
        conn.close()
