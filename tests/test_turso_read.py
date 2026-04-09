from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from alphavault.homework_trade_feed import HOMEWORK_DEFAULT_VIEW_KEY
from alphavault_reflex.services import trade_board_loader
from alphavault_reflex.services import turso_read


def TursoSource(*, name: str, url: str, token: str) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        url=url,
        token=token,
        dsn=url,
        schema=name,
    )


def test_query_stock_alias_relations_uses_formal_relations_table(monkeypatch) -> None:
    captured_sql: list[str] = []

    def _fake_turso_read_sql_df(conn, sql, params=None):  # type: ignore[no-untyped-def]
        del conn, params
        captured_sql.append(str(sql))
        return pd.DataFrame()

    monkeypatch.setattr(
        trade_board_loader,
        "turso_read_sql_df",
        _fake_turso_read_sql_df,
    )

    df = trade_board_loader.query_stock_alias_relations(conn=object(), source_name="")

    assert df.empty
    assert captured_sql
    assert "FROM standard.relations" in captured_sql[0]


def test_resolve_homework_source_workers_uses_default_when_env_missing(
    monkeypatch,
) -> None:
    monkeypatch.delenv(
        trade_board_loader.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS,
        raising=False,
    )
    assert trade_board_loader.resolve_homework_source_workers(source_count=5) == 2


def test_resolve_homework_source_workers_falls_back_on_invalid_env(
    monkeypatch,
) -> None:
    monkeypatch.setenv(trade_board_loader.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "bad")
    assert trade_board_loader.resolve_homework_source_workers(source_count=5) == 2


def test_resolve_homework_source_workers_clamps_to_valid_bounds(
    monkeypatch,
) -> None:
    monkeypatch.setenv(trade_board_loader.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "0")
    assert trade_board_loader.resolve_homework_source_workers(source_count=5) == 1

    monkeypatch.setenv(trade_board_loader.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "9")
    assert trade_board_loader.resolve_homework_source_workers(source_count=2) == 2


def test_load_homework_board_payload_from_env_merges_source_rows(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trade_board_loader,
        "load_configured_postgres_sources_from_env",
        lambda: [
            TursoSource(name="weibo", url="u1", token="t1"),
            TursoSource(name="xueqiu", url="u2", token="t2"),
        ],
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        lookback_days: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        del db_url, auth_token
        assert lookback_days == 3
        assertions = pd.DataFrame(
            [
                {
                    "post_uid": f"{source_name}:p1",
                    "entity_key": "stock:600519.SH",
                    "action": "trade.buy",
                    "action_strength": 2,
                    "summary": "小仓试错",
                    "author": "alice",
                    "created_at": pd.Timestamp("2026-03-25 10:00:00"),
                    "url": "https://example.com",
                    "stock_codes": ["600519.SH"],
                    "stock_names": ["贵州茅台"],
                }
            ]
        )
        relations = pd.DataFrame(
            [
                {
                    "relation_type": "stock_alias",
                    "left_key": "stock:600519.SH",
                    "right_key": "stock:茅台",
                    "relation_label": "alias_of",
                    "source": source_name,
                    "updated_at": "2026-03-25 10:00:00",
                }
            ]
        )
        return assertions, relations

    assertions, relations, err = (
        trade_board_loader.load_homework_board_payload_from_env(
            3,
            load_cached_fn=_fake_cached,
        )
    )

    assert err == ""
    assert len(assertions) == 2
    assert len(relations) == 2
    assert {
        str(item).strip()
        for item in assertions.get("post_uid", pd.Series(dtype=str)).tolist()
    } == {"weibo:p1", "xueqiu:p1"}
    assert {
        str(item).strip()
        for item in relations.get("source", pd.Series(dtype=str)).tolist()
    } == {"weibo", "xueqiu"}


def test_load_homework_board_payload_from_env_returns_turso_error(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trade_board_loader,
        "load_configured_postgres_sources_from_env",
        lambda: [TursoSource(name="weibo", url="u1", token="t1")],
    )

    def _raise_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        lookback_days: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        del db_url, auth_token, source_name, lookback_days
        raise RuntimeError("boom")

    assertions, relations, err = (
        trade_board_loader.load_homework_board_payload_from_env(
            3,
            load_cached_fn=_raise_cached,
        )
    )

    assert assertions.empty
    assert relations.empty
    assert err == "turso_connect_error:weibo:RuntimeError"


def test_load_homework_board_payload_from_env_keeps_partial_success(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trade_board_loader,
        "load_configured_postgres_sources_from_env",
        lambda: [
            TursoSource(name="weibo", url="u1", token="t1"),
            TursoSource(name="xueqiu", url="u2", token="t2"),
        ],
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        lookback_days: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        del db_url, auth_token
        assert lookback_days == 3
        if source_name == "xueqiu":
            raise ValueError("bad token")
        return (
            pd.DataFrame([{"post_uid": "weibo:p1"}]),
            pd.DataFrame([{"source": "weibo"}]),
        )

    monkeypatch.setenv(trade_board_loader.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "2")
    assertions, relations, err = (
        trade_board_loader.load_homework_board_payload_from_env(
            3,
            load_cached_fn=_fake_cached,
        )
    )

    assert err == ""
    assert {
        str(item).strip()
        for item in assertions.get("post_uid", pd.Series(dtype=str)).tolist()
    } == {"weibo:p1"}
    assert {
        str(item).strip()
        for item in relations.get("source", pd.Series(dtype=str)).tolist()
    } == {"weibo"}


def test_load_homework_board_payload_from_env_fails_on_standard_error(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trade_board_loader,
        "load_configured_postgres_sources_from_env",
        lambda: [TursoSource(name="weibo", url="u1", token="t1")],
    )

    def _raise_standard_error(
        db_url: str,
        auth_token: str,
        source_name: str,
        lookback_days: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        del db_url, auth_token, source_name, lookback_days
        raise RuntimeError("turso_connect_error:standard:RuntimeError")

    assertions, relations, err = (
        trade_board_loader.load_homework_board_payload_from_env(
            3,
            load_cached_fn=_raise_standard_error,
        )
    )

    assert assertions.empty
    assert relations.empty
    assert err == "turso_connect_error:standard:RuntimeError"


def test_load_homework_board_payload_from_env_serial_parallel_same(
    monkeypatch,
) -> None:
    sources = [
        TursoSource(name="weibo", url="u1", token="t1"),
        TursoSource(name="xueqiu", url="u2", token="t2"),
    ]
    monkeypatch.setattr(
        trade_board_loader,
        "load_configured_postgres_sources_from_env",
        lambda: sources,
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        lookback_days: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        del db_url, auth_token
        assert lookback_days == 3
        return (
            pd.DataFrame([{"post_uid": f"{source_name}:p1"}]),
            pd.DataFrame([{"source": source_name}]),
        )

    monkeypatch.setenv(trade_board_loader.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "1")
    serial_assertions, serial_relations, serial_err = (
        trade_board_loader.load_homework_board_payload_from_env(
            3,
            load_cached_fn=_fake_cached,
        )
    )

    monkeypatch.setenv(trade_board_loader.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "2")
    parallel_assertions, parallel_relations, parallel_err = (
        trade_board_loader.load_homework_board_payload_from_env(
            3,
            load_cached_fn=_fake_cached,
        )
    )

    assert serial_err == ""
    assert parallel_err == ""
    assert {
        str(item).strip()
        for item in serial_assertions.get("post_uid", pd.Series(dtype=str)).tolist()
    } == {
        str(item).strip()
        for item in parallel_assertions.get("post_uid", pd.Series(dtype=str)).tolist()
    }
    assert {
        str(item).strip()
        for item in serial_relations.get("source", pd.Series(dtype=str)).tolist()
    } == {
        str(item).strip()
        for item in parallel_relations.get("source", pd.Series(dtype=str)).tolist()
    }


def test_load_homework_board_payload_from_env_dedupes_global_relations(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trade_board_loader,
        "load_configured_postgres_sources_from_env",
        lambda: [
            TursoSource(name="weibo", url="u1", token="t1"),
            TursoSource(name="xueqiu", url="u2", token="t2"),
        ],
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        lookback_days: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        del db_url, auth_token
        assert lookback_days == 3
        return (
            pd.DataFrame([{"post_uid": f"{source_name}:p1"}]),
            pd.DataFrame(
                [
                    {
                        "relation_type": "stock_alias",
                        "left_key": "stock:600519.SH",
                        "right_key": "stock:茅台",
                        "relation_label": "alias_of",
                        "source": "standard",
                    }
                ]
            ),
        )

    assertions, relations, err = (
        trade_board_loader.load_homework_board_payload_from_env(
            3,
            load_cached_fn=_fake_cached,
        )
    )

    assert err == ""
    assert len(assertions) == 2
    assert len(relations) == 1


def test_load_stock_alias_relations_from_env_uses_workbench_engine(monkeypatch) -> None:
    fake_engine = object()
    seen: list[object] = []

    monkeypatch.setattr(
        trade_board_loader,
        "load_configured_postgres_sources_from_env",
        lambda: (_ for _ in ()).throw(AssertionError("should_not_read_source_env")),
    )
    monkeypatch.setattr(
        trade_board_loader,
        "get_research_workbench_engine_from_env",
        lambda: fake_engine,
        raising=False,
    )

    class _FakeConn:
        pass

    from contextlib import contextmanager

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        seen.append(engine)
        yield _FakeConn()

    def _fake_query_stock_alias_relations(*, conn, source_name):  # type: ignore[no-untyped-def]
        seen.extend([conn, source_name])
        return pd.DataFrame([{"left_key": "stock:600519.SH"}])

    trade_board_loader.load_stock_alias_relations_cached.cache_clear()
    monkeypatch.setattr(
        trade_board_loader,
        "postgres_connect_autocommit",
        _fake_connect,
    )
    monkeypatch.setattr(
        trade_board_loader,
        "query_stock_alias_relations",
        _fake_query_stock_alias_relations,
    )

    relations, err = trade_board_loader.load_stock_alias_relations_from_env()

    assert err == ""
    assert len(relations) == 1
    assert seen[0] is fake_engine
    assert seen[2] == "standard"


def test_load_homework_trade_feed_from_env_uses_workbench_engine(monkeypatch) -> None:
    fake_engine = object()
    seen: list[object] = []

    def _fake_load_homework_trade_feed(engine, *, view_key):  # type: ignore[no-untyped-def]
        seen.extend([engine, view_key])
        return {"rows": [{"topic": "stock:600519.SH"}]}

    monkeypatch.setattr(
        turso_read,
        "get_research_workbench_engine_from_env",
        lambda: fake_engine,
        raising=False,
    )
    monkeypatch.setattr(
        turso_read,
        "load_homework_trade_feed",
        _fake_load_homework_trade_feed,
        raising=False,
    )

    payload = turso_read.load_homework_trade_feed_from_env()

    assert payload == {"rows": [{"topic": "stock:600519.SH"}]}
    assert seen == [fake_engine, HOMEWORK_DEFAULT_VIEW_KEY]


def test_save_homework_trade_feed_from_env_uses_workbench_engine(monkeypatch) -> None:
    fake_engine = object()
    seen: list[object] = []

    monkeypatch.setattr(
        turso_read,
        "get_research_workbench_engine_from_env",
        lambda: fake_engine,
        raising=False,
    )
    monkeypatch.setattr(
        turso_read,
        "save_homework_trade_feed",
        lambda engine, **kwargs: seen.extend([engine, kwargs]),
        raising=False,
    )

    turso_read.save_homework_trade_feed_from_env(
        caption="窗口：最近 3 天",
        used_window_days=3,
        rows=[{"topic": "stock:600519.SH"}],
    )

    assert seen == [
        fake_engine,
        {
            "view_key": HOMEWORK_DEFAULT_VIEW_KEY,
            "caption": "窗口：最近 3 天",
            "used_window_days": 3,
            "rows": [{"topic": "stock:600519.SH"}],
        },
    ]
