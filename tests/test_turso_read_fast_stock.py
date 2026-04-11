from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from alphavault.constants import SCHEMA_WEIBO, SCHEMA_XUEQIU
from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.postgres_db import PostgresConnection
from alphavault_reflex.services import stock_fast_loader


def _source_pg_conn(pg_conn, *, schema_name: str) -> PostgresConnection:
    apply_cloud_schema(pg_conn, target="source", schema_name=schema_name)
    return PostgresConnection(pg_conn, schema_name=schema_name)


def test_load_stock_alias_keys_cached_uses_standard_relations_table(
    monkeypatch,
) -> None:
    fake_engine = object()
    seen: list[object] = []
    stock_fast_loader.load_stock_alias_keys_cached.cache_clear()

    monkeypatch.setattr(
        stock_fast_loader,
        "get_research_workbench_engine_from_env",
        lambda: fake_engine,
        raising=False,
    )

    class _FakeConn:
        def execute(self, sql: str, params: dict[str, object]):  # type: ignore[no-untyped-def]
            seen.extend([sql, params])

            class _FakeCursor:
                def fetchall(self) -> list[tuple[str]]:
                    return [("stock:茅台",), ("stock:小茅",)]

            return _FakeCursor()

    from contextlib import contextmanager

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        seen.append(engine)
        yield _FakeConn()

    monkeypatch.setattr(
        stock_fast_loader,
        "postgres_connect_autocommit",
        _fake_connect,
    )

    alias_keys = stock_fast_loader.load_stock_alias_keys_cached("stock:600519.SH")

    assert alias_keys == ("stock:茅台", "stock:小茅")
    assert seen[0] is fake_engine
    assert "FROM standard.relations" in str(seen[1])


def test_load_stock_alias_keys_cached_raises_standard_error_on_query_failure(
    monkeypatch,
) -> None:
    stock_fast_loader.load_stock_alias_keys_cached.cache_clear()
    monkeypatch.setattr(
        stock_fast_loader,
        "get_research_workbench_engine_from_env",
        lambda: object(),
        raising=False,
    )

    class _BrokenConn:
        def execute(self, sql: str, params: dict[str, object]):  # type: ignore[no-untyped-def]
            del sql, params
            raise RuntimeError("boom")

    from contextlib import contextmanager

    @contextmanager
    def _fake_connect(_engine):  # type: ignore[no-untyped-def]
        yield _BrokenConn()

    monkeypatch.setattr(
        stock_fast_loader,
        "postgres_connect_autocommit",
        _fake_connect,
    )

    try:
        stock_fast_loader.load_stock_alias_keys_cached("stock:600519.SH")
    except RuntimeError as err:
        assert str(err) == "turso_connect_error:standard:RuntimeError"
    else:
        raise AssertionError("expected standard alias lookup error")


def test_load_stock_sources_fast_from_env_returns_partial_error(monkeypatch) -> None:
    monkeypatch.setattr(stock_fast_loader, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        stock_fast_loader,
        "load_configured_postgres_sources_from_env",
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
        "load_configured_postgres_sources_from_env",
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


def test_load_stock_sources_fast_from_env_normalizes_prefixed_cn_stock_key(
    monkeypatch,
) -> None:
    monkeypatch.setattr(stock_fast_loader, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        stock_fast_loader,
        "load_configured_postgres_sources_from_env",
        lambda: [SimpleNamespace(name="xueqiu", url="u1", token="t1")],
    )
    seen_calls: list[tuple[str, str]] = []

    def _fake_fast_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        stock_key: str,
        stock_code: str,
        per_source_limit: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        del db_url, auth_token, source_name, per_source_limit
        seen_calls.append((stock_key, stock_code))
        return pd.DataFrame(), pd.DataFrame()

    _posts, _assertions, _err = stock_fast_loader.load_stock_sources_fast_from_env(
        "SZ000725.US",
        load_cached_fn=_fake_fast_cached,
    )

    assert seen_calls == [("stock:000725.SZ", "000725.SZ")]


def test_load_stock_sources_fast_from_env_returns_standard_error(
    monkeypatch,
) -> None:
    monkeypatch.setattr(stock_fast_loader, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        stock_fast_loader,
        "load_configured_postgres_sources_from_env",
        lambda: [SimpleNamespace(name="weibo", url="u1", token="t1")],
    )

    def _raise_standard_error(
        db_url: str,
        auth_token: str,
        source_name: str,
        stock_key: str,
        stock_code: str,
        per_source_limit: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        del db_url, auth_token, source_name, stock_key, stock_code, per_source_limit
        raise RuntimeError("turso_connect_error:standard:RuntimeError")

    posts, assertions, err = stock_fast_loader.load_stock_sources_fast_from_env(
        "03316.HK",
        load_cached_fn=_raise_standard_error,
    )

    assert posts.empty
    assert assertions.empty
    assert err == "turso_connect_error:standard:RuntimeError"


def test_load_stock_trade_sources_fast_cached_reads_stock_entity_key(
    monkeypatch,
    pg_conn,
) -> None:
    stock_fast_loader.load_stock_trade_sources_fast_cached.cache_clear()
    stock_fast_loader.load_stock_alias_keys_cached.cache_clear()
    conn = _source_pg_conn(pg_conn, schema_name=SCHEMA_WEIBO)
    pg_conn.execute(
        "INSERT INTO weibo.posts(post_uid, platform, platform_post_id, author, created_at, url, raw_text, final_status, processed_at, model, prompt_version, archived_at, ingested_at) VALUES ('weibo:2', 'weibo', '2', 'alice', '2099-01-02 00:00:00', 'https://example.com/weibo/2', '原文', 'relevant', '2099-01-02 00:00:01', 'gpt', 'v1', '2099-01-02 00:00:02', 1)"
    )
    pg_conn.execute(
        "INSERT INTO weibo.assertions(assertion_id, post_uid, idx, action, action_strength, summary, evidence) VALUES ('weibo:2#1', 'weibo:2', 1, 'trade.buy', 2, '别名行也要进正式个股页', '别名行也要进正式个股页')"
    )
    pg_conn.execute(
        "INSERT INTO weibo.assertion_entities(assertion_id, entity_key, entity_type, match_source, is_primary) VALUES ('weibo:2#1', 'stock:601899.SH', 'stock', 'stock_alias', 1)"
    )

    monkeypatch.setattr(
        stock_fast_loader,
        "ensure_postgres_engine",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        stock_fast_loader,
        "get_research_workbench_engine_from_env",
        lambda: object(),
        raising=False,
    )

    def _fake_load_stock_alias_keys_cached(
        _stock_key: str,
    ) -> tuple[str, ...]:
        return ()

    _fake_load_stock_alias_keys_cached.cache_clear = lambda: None  # type: ignore[attr-defined]
    monkeypatch.setattr(
        stock_fast_loader,
        "load_stock_alias_keys_cached",
        _fake_load_stock_alias_keys_cached,
    )
    monkeypatch.setattr(
        stock_fast_loader,
        "postgres_connect_autocommit",
        lambda _engine: conn,
    )

    posts, assertions = stock_fast_loader.load_stock_trade_sources_fast_cached(
        "postgresql://unused",
        "",
        SCHEMA_WEIBO,
        "stock:601899.SH",
        "601899.SH",
        16,
    )

    assert list(posts["post_uid"]) == ["weibo:2"]
    assert list(assertions["post_uid"]) == ["weibo:2"]
    assert assertions.iloc[0]["entity_key"] == "stock:601899.SH"
    assert assertions.iloc[0]["summary"] == "别名行也要进正式个股页"

    stock_fast_loader.load_stock_trade_sources_fast_cached.cache_clear()
    stock_fast_loader.load_stock_alias_keys_cached.cache_clear()


def test_load_stock_trade_sources_fast_cached_orders_by_post_created_at(
    monkeypatch,
) -> None:
    stock_fast_loader.load_stock_trade_sources_fast_cached.cache_clear()
    stock_fast_loader.load_stock_alias_keys_cached.cache_clear()
    seen_sql: list[str] = []

    monkeypatch.setattr(
        stock_fast_loader,
        "ensure_postgres_engine",
        lambda *_args, **_kwargs: object(),
    )

    def _fake_load_stock_alias_keys_cached(
        _stock_key: str,
    ) -> tuple[str, ...]:
        return ()

    _fake_load_stock_alias_keys_cached.cache_clear = lambda: None  # type: ignore[attr-defined]
    monkeypatch.setattr(
        stock_fast_loader,
        "load_stock_alias_keys_cached",
        _fake_load_stock_alias_keys_cached,
    )

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

    monkeypatch.setattr(
        stock_fast_loader,
        "postgres_connect_autocommit",
        lambda _engine: _FakeConn(),
    )

    def _fake_read_sql_df(conn, sql: str, params=None):  # type: ignore[no-untyped-def]
        del conn, params
        seen_sql.append(str(sql))
        return pd.DataFrame()

    monkeypatch.setattr(
        stock_fast_loader,
        "turso_read_sql_df",
        _fake_read_sql_df,
    )

    posts, assertions = stock_fast_loader.load_stock_trade_sources_fast_cached(
        "postgresql://unused",
        "",
        SCHEMA_WEIBO,
        "stock:601899.SH",
        "601899.SH",
        16,
    )

    assert posts.empty
    assert assertions.empty
    assert seen_sql
    assert "JOIN weibo.posts p" in seen_sql[0]
    assert "ORDER BY p.created_at DESC" in seen_sql[0]
    assert "a.created_at" not in seen_sql[0]

    stock_fast_loader.load_stock_trade_sources_fast_cached.cache_clear()
    stock_fast_loader.load_stock_alias_keys_cached.cache_clear()


def test_load_stock_trade_sources_fast_cached_reads_legacy_prefixed_cn_entity_key(
    monkeypatch,
    pg_conn,
) -> None:
    stock_fast_loader.load_stock_trade_sources_fast_cached.cache_clear()
    stock_fast_loader.load_stock_alias_keys_cached.cache_clear()
    conn = _source_pg_conn(pg_conn, schema_name=SCHEMA_XUEQIU)
    pg_conn.execute(
        "INSERT INTO xueqiu.posts(post_uid, platform, platform_post_id, author, created_at, url, raw_text, final_status, processed_at, model, prompt_version, archived_at, ingested_at) VALUES ('xueqiu:1', 'xueqiu', '1', 'alice', '2099-01-03 00:00:00', 'https://example.com/xueqiu/1', '原文', 'relevant', '2099-01-03 00:00:01', 'gpt', 'v1', '2099-01-03 00:00:02', 1)"
    )
    pg_conn.execute(
        "INSERT INTO xueqiu.assertions(assertion_id, post_uid, idx, action, action_strength, summary, evidence) VALUES ('xueqiu:1#1', 'xueqiu:1', 1, 'trade.buy', 2, '旧坏 key 也要被快读链路命中', '旧坏 key 也要被快读链路命中')"
    )
    pg_conn.execute(
        "INSERT INTO xueqiu.assertion_entities(assertion_id, entity_key, entity_type, match_source, is_primary) VALUES ('xueqiu:1#1', 'stock:SZ000725.US', 'stock', 'stock_code', 1)"
    )

    monkeypatch.setattr(
        stock_fast_loader,
        "ensure_postgres_engine",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        stock_fast_loader,
        "get_research_workbench_engine_from_env",
        lambda: object(),
        raising=False,
    )

    def _fake_load_stock_alias_keys_cached(
        _stock_key: str,
    ) -> tuple[str, ...]:
        return ()

    _fake_load_stock_alias_keys_cached.cache_clear = lambda: None  # type: ignore[attr-defined]
    monkeypatch.setattr(
        stock_fast_loader,
        "load_stock_alias_keys_cached",
        _fake_load_stock_alias_keys_cached,
    )
    monkeypatch.setattr(
        stock_fast_loader,
        "postgres_connect_autocommit",
        lambda _engine: conn,
    )

    posts, assertions = stock_fast_loader.load_stock_trade_sources_fast_cached(
        "postgresql://unused",
        "",
        SCHEMA_XUEQIU,
        "stock:000725.SZ",
        "000725.SZ",
        16,
    )

    assert list(posts["post_uid"]) == ["xueqiu:1"]
    assert list(assertions["post_uid"]) == ["xueqiu:1"]

    stock_fast_loader.load_stock_trade_sources_fast_cached.cache_clear()
    stock_fast_loader.load_stock_alias_keys_cached.cache_clear()
