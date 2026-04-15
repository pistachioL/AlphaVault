from __future__ import annotations

from types import SimpleNamespace

from alphavault.domains.relation.ids import make_candidate_id
from alphavault.homework_trade_feed import HOMEWORK_DEFAULT_VIEW_KEY
from alphavault_reflex.services import source_loader
from alphavault_reflex.services import trade_board_loader
from alphavault_reflex.services import source_read


def _rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return rows


def SourceConfigStub(*, name: str, url: str, token: str) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        url=url,
        token=token,
        dsn=url,
        schema=name,
    )


def test_query_stock_alias_relation_rows_uses_formal_relations_table(
    monkeypatch,
) -> None:
    captured_sql: list[str] = []

    def _fake_read_sql_rows(conn, sql, params=None):  # type: ignore[no-untyped-def]
        del conn, params
        captured_sql.append(str(sql))
        return []

    monkeypatch.setattr(
        trade_board_loader,
        "read_sql_rows",
        _fake_read_sql_rows,
    )

    rows = trade_board_loader.query_stock_alias_relation_rows(
        conn=object(),
        source_name="",
    )

    assert rows == []
    assert captured_sql
    assert "FROM standard.relations" in captured_sql[0]


def test_clear_reflex_source_caches_clears_row_and_wrapper_caches(
    monkeypatch,
) -> None:
    captured_cache_clears: list[object] = []

    def _fake_clear_registered_caches(*cache_clear_fns) -> None:  # type: ignore[no-untyped-def]
        captured_cache_clears.extend(cache_clear_fns)

    monkeypatch.setattr(
        source_read,
        "clear_registered_caches",
        _fake_clear_registered_caches,
    )

    source_read.clear_reflex_source_caches()

    cache_owners = {
        getattr(cache_clear_fn, "__self__", None)
        for cache_clear_fn in captured_cache_clears
    }

    assert source_loader.load_trade_sources_cached in cache_owners
    assert source_loader.load_trade_sources_rows_cached in cache_owners
    assert source_loader.load_trade_assertion_rows_cached in cache_owners
    assert source_loader.load_stock_alias_candidate_rows_cached in cache_owners
    assert trade_board_loader.load_trade_board_assertions_cached in cache_owners
    assert trade_board_loader.load_trade_board_assertion_rows_cached in cache_owners
    assert trade_board_loader.load_homework_board_payload_cached in cache_owners
    assert trade_board_loader.load_homework_board_payload_rows_cached in cache_owners
    assert trade_board_loader.load_stock_alias_relations_cached in cache_owners
    assert trade_board_loader.load_stock_alias_relation_rows_cached in cache_owners


def test_load_source_engines_from_env_builds_all_source_schema_engines(
    monkeypatch,
) -> None:
    seen_steps: list[str] = []

    monkeypatch.setattr(
        source_read,
        "load_dotenv_if_present",
        lambda: seen_steps.append("dotenv"),
        raising=False,
    )
    monkeypatch.setattr(
        source_read,
        "load_configured_source_schemas_from_env",
        lambda: seen_steps.append("sources")
        or [
            SourceConfigStub(name="weibo", url="u1", token=""),
            SourceConfigStub(name="xueqiu", url="u2", token=""),
        ],
        raising=False,
    )

    captured: list[tuple[str, str]] = []

    def _fake_ensure_postgres_engine(dsn: str, *, schema_name: str = ""):  # type: ignore[no-untyped-def]
        captured.append((dsn, schema_name))
        return f"engine:{schema_name}"

    monkeypatch.setattr(
        source_read,
        "ensure_postgres_engine",
        _fake_ensure_postgres_engine,
        raising=False,
    )

    engines = source_read.load_source_engines_from_env()

    assert engines == ["engine:weibo", "engine:xueqiu"]
    assert captured == [("u1", "weibo"), ("u2", "xueqiu")]
    assert seen_steps[:2] == ["dotenv", "sources"]


def test_load_stock_alias_candidates_from_env_merges_source_scores(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        source_loader,
        "load_configured_source_schemas_from_env",
        lambda: [
            SourceConfigStub(name="weibo", url="u1", token="t1"),
            SourceConfigStub(name="xueqiu", url="u2", token="t2"),
        ],
    )
    monkeypatch.setattr(
        source_loader,
        "get_research_workbench_engine_from_env",
        lambda: object(),
        raising=False,
    )
    monkeypatch.setattr(
        source_loader,
        "get_official_names_by_stock_keys",
        lambda _engine, _stock_keys: {},
        raising=False,
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        window_days: int,
    ) -> tuple[dict[str, object], ...]:
        del db_url, auth_token
        assert window_days == 30
        if source_name == "weibo":
            return (
                {
                    "left_key": "stock:600519.SH",
                    "right_key": "stock:茅台",
                    "score": 3,
                },
                {
                    "left_key": "stock:601899.SH",
                    "right_key": "stock:紫金矿业",
                    "score": 4,
                },
            )
        return (
            {
                "left_key": "stock:600519.SH",
                "right_key": "stock:茅台",
                "score": 5,
            },
        )

    rows, err = source_loader.load_stock_alias_candidates_from_env(
        load_cached_fn=_fake_cached,
    )

    assert err == ""
    assert rows == [
        {
            "relation_type": "stock_alias",
            "left_key": "stock:600519.SH",
            "right_key": "stock:茅台",
            "relation_label": "alias_of",
            "candidate_id": make_candidate_id(
                relation_type="stock_alias",
                left_key="stock:600519.SH",
                right_key="stock:茅台",
                relation_label="alias_of",
            ),
            "candidate_key": "stock:茅台",
            "score": "8",
            "suggestion_reason": "近30天同票提及 8 次",
            "evidence_summary": "近30天同票提及 8 次",
        },
        {
            "relation_type": "stock_alias",
            "left_key": "stock:601899.SH",
            "right_key": "stock:紫金矿业",
            "relation_label": "alias_of",
            "candidate_id": make_candidate_id(
                relation_type="stock_alias",
                left_key="stock:601899.SH",
                right_key="stock:紫金矿业",
                relation_label="alias_of",
            ),
            "candidate_key": "stock:紫金矿业",
            "score": "4",
            "suggestion_reason": "近30天同票提及 4 次",
            "evidence_summary": "近30天同票提及 4 次",
        },
    ]


def test_load_stock_alias_candidates_from_env_filters_same_stock_official_name(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        source_loader,
        "load_configured_source_schemas_from_env",
        lambda: [SourceConfigStub(name="weibo", url="u1", token="t1")],
    )
    engine = object()
    seen_stock_keys: list[str] = []
    monkeypatch.setattr(
        source_loader,
        "get_research_workbench_engine_from_env",
        lambda: engine,
        raising=False,
    )

    def _fake_get_official_names_by_stock_keys(received_engine, stock_keys):  # type: ignore[no-untyped-def]
        assert received_engine is engine
        seen_stock_keys.extend(str(stock_key) for stock_key in stock_keys)
        return {
            "stock:601899.SH": "紫金矿业",
            "stock:02899.HK": "紫金矿业",
        }

    monkeypatch.setattr(
        source_loader,
        "get_official_names_by_stock_keys",
        _fake_get_official_names_by_stock_keys,
        raising=False,
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        window_days: int,
    ) -> tuple[dict[str, object], ...]:
        del db_url, auth_token, source_name
        assert window_days == 30
        return (
            {
                "left_key": "stock:601899.SH",
                "right_key": "stock:紫金矿业",
                "score": 4,
            },
            {
                "left_key": "stock:02899.HK",
                "right_key": "stock:紫金矿业",
                "score": 3,
            },
            {
                "left_key": "stock:601899.SH",
                "right_key": "stock:阿紫",
                "score": 2,
            },
        )

    rows, err = source_loader.load_stock_alias_candidates_from_env(
        load_cached_fn=_fake_cached,
    )

    assert err == ""
    assert set(seen_stock_keys) == {"stock:601899.SH", "stock:02899.HK"}
    assert rows == [
        {
            "relation_type": "stock_alias",
            "left_key": "stock:601899.SH",
            "right_key": "stock:阿紫",
            "relation_label": "alias_of",
            "candidate_id": make_candidate_id(
                relation_type="stock_alias",
                left_key="stock:601899.SH",
                right_key="stock:阿紫",
                relation_label="alias_of",
            ),
            "candidate_key": "stock:阿紫",
            "score": "2",
            "suggestion_reason": "近30天同票提及 2 次",
            "evidence_summary": "近30天同票提及 2 次",
        }
    ]


def test_load_stock_alias_candidates_from_env_keeps_unknown_name_candidate(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        source_loader,
        "load_configured_source_schemas_from_env",
        lambda: [SourceConfigStub(name="weibo", url="u1", token="t1")],
    )
    monkeypatch.setattr(
        source_loader,
        "get_research_workbench_engine_from_env",
        lambda: object(),
        raising=False,
    )
    monkeypatch.setattr(
        source_loader,
        "get_official_names_by_stock_keys",
        lambda _engine, _stock_keys: {"stock:601899.SH": "紫金矿业"},
        raising=False,
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        window_days: int,
    ) -> tuple[dict[str, object], ...]:
        del db_url, auth_token, source_name
        assert window_days == 30
        return (
            {
                "left_key": "stock:601899.SH",
                "right_key": "stock:紫金",
                "score": 4,
            },
        )

    rows, err = source_loader.load_stock_alias_candidates_from_env(
        load_cached_fn=_fake_cached,
    )

    assert err == ""
    assert rows == [
        {
            "relation_type": "stock_alias",
            "left_key": "stock:601899.SH",
            "right_key": "stock:紫金",
            "relation_label": "alias_of",
            "candidate_id": make_candidate_id(
                relation_type="stock_alias",
                left_key="stock:601899.SH",
                right_key="stock:紫金",
                relation_label="alias_of",
            ),
            "candidate_key": "stock:紫金",
            "score": "4",
            "suggestion_reason": "近30天同票提及 4 次",
            "evidence_summary": "近30天同票提及 4 次",
        }
    ]


def test_load_stock_alias_candidates_from_env_returns_empty_without_error_after_filter(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        source_loader,
        "load_configured_source_schemas_from_env",
        lambda: [SourceConfigStub(name="weibo", url="u1", token="t1")],
    )
    monkeypatch.setattr(
        source_loader,
        "get_research_workbench_engine_from_env",
        lambda: object(),
        raising=False,
    )
    monkeypatch.setattr(
        source_loader,
        "get_official_names_by_stock_keys",
        lambda _engine, _stock_keys: {"stock:601899.SH": "紫金矿业"},
        raising=False,
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        window_days: int,
    ) -> tuple[dict[str, object], ...]:
        del db_url, auth_token, source_name
        assert window_days == 30
        return (
            {
                "left_key": "stock:601899.SH",
                "right_key": "stock:紫金矿业",
                "score": 4,
            },
        )

    rows, err = source_loader.load_stock_alias_candidates_from_env(
        load_cached_fn=_fake_cached,
    )

    assert err == ""
    assert rows == []


def test_load_stock_alias_candidates_from_env_filters_hyphen_width_variant(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        source_loader,
        "load_configured_source_schemas_from_env",
        lambda: [SourceConfigStub(name="weibo", url="u1", token="t1")],
    )
    monkeypatch.setattr(
        source_loader,
        "get_research_workbench_engine_from_env",
        lambda: object(),
        raising=False,
    )
    monkeypatch.setattr(
        source_loader,
        "get_official_names_by_stock_keys",
        lambda _engine, _stock_keys: {"stock:09988.HK": "阿里巴巴－W"},
        raising=False,
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        window_days: int,
    ) -> tuple[dict[str, object], ...]:
        del db_url, auth_token, source_name
        assert window_days == 30
        return (
            {
                "left_key": "stock:09988.HK",
                "right_key": "stock:阿里巴巴-W",
                "score": 11,
            },
        )

    rows, err = source_loader.load_stock_alias_candidates_from_env(
        load_cached_fn=_fake_cached,
    )

    assert err == ""
    assert rows == []


def test_load_stock_alias_candidates_from_env_filters_confirmed_alias_relation(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        source_loader,
        "load_configured_source_schemas_from_env",
        lambda: [SourceConfigStub(name="weibo", url="u1", token="t1")],
    )
    fake_engine = object()
    monkeypatch.setattr(
        source_loader,
        "get_research_workbench_engine_from_env",
        lambda: fake_engine,
        raising=False,
    )
    monkeypatch.setattr(
        source_loader,
        "get_official_names_by_stock_keys",
        lambda _engine, _stock_keys: {"stock:600741.SH": "狮头股份"},
        raising=False,
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        window_days: int,
    ) -> tuple[dict[str, object], ...]:
        del db_url, auth_token, source_name
        assert window_days == 30
        return (
            {
                "left_key": "stock:600741.SH",
                "right_key": "stock:中国平安",
                "score": 4,
            },
        )

    seen_sql: list[str] = []
    seen_params: list[dict[str, object]] = []

    class _FakeConn:
        def execute(self, sql: str, params: dict[str, object]):  # type: ignore[no-untyped-def]
            seen_sql.append(sql)
            seen_params.append(dict(params))

            class _FakeCursor:
                def fetchall(self) -> list[tuple[str]]:
                    return [("stock:中国平安",)]

            return _FakeCursor()

    from contextlib import contextmanager

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        assert engine is fake_engine
        yield _FakeConn()

    monkeypatch.setattr(
        source_loader,
        "postgres_connect_autocommit",
        _fake_connect,
    )

    rows, err = source_loader.load_stock_alias_candidates_from_env(
        load_cached_fn=_fake_cached,
    )

    assert err == ""
    assert rows == []
    assert seen_sql
    assert "FROM standard.relations" in seen_sql[0]
    assert "stock:中国平安" in set(seen_params[0].values())


def test_load_stock_alias_candidates_from_env_keeps_unconfirmed_alias_after_confirmed_filter(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        source_loader,
        "load_configured_source_schemas_from_env",
        lambda: [SourceConfigStub(name="weibo", url="u1", token="t1")],
    )
    fake_engine = object()
    monkeypatch.setattr(
        source_loader,
        "get_research_workbench_engine_from_env",
        lambda: fake_engine,
        raising=False,
    )
    monkeypatch.setattr(
        source_loader,
        "get_official_names_by_stock_keys",
        lambda _engine, _stock_keys: {"stock:600741.SH": "狮头股份"},
        raising=False,
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        window_days: int,
    ) -> tuple[dict[str, object], ...]:
        del db_url, auth_token, source_name
        assert window_days == 30
        return (
            {
                "left_key": "stock:600741.SH",
                "right_key": "stock:中国平安",
                "score": 4,
            },
        )

    class _FakeConn:
        def execute(self, sql: str, params: dict[str, object]):  # type: ignore[no-untyped-def]
            del sql, params

            class _FakeCursor:
                def fetchall(self) -> list[tuple[str]]:
                    return []

            return _FakeCursor()

    from contextlib import contextmanager

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        assert engine is fake_engine
        yield _FakeConn()

    monkeypatch.setattr(
        source_loader,
        "postgres_connect_autocommit",
        _fake_connect,
    )

    rows, err = source_loader.load_stock_alias_candidates_from_env(
        load_cached_fn=_fake_cached,
    )

    assert err == ""
    assert rows == [
        {
            "relation_type": "stock_alias",
            "left_key": "stock:600741.SH",
            "right_key": "stock:中国平安",
            "relation_label": "alias_of",
            "candidate_id": make_candidate_id(
                relation_type="stock_alias",
                left_key="stock:600741.SH",
                right_key="stock:中国平安",
                relation_label="alias_of",
            ),
            "candidate_key": "stock:中国平安",
            "score": "4",
            "suggestion_reason": "近30天同票提及 4 次",
            "evidence_summary": "近30天同票提及 4 次",
        }
    ]


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
            SourceConfigStub(name="weibo", url="u1", token="t1"),
            SourceConfigStub(name="xueqiu", url="u2", token="t2"),
        ],
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        lookback_days: int,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        del db_url, auth_token
        assert lookback_days == 3
        assertions = _rows(
            [
                {
                    "post_uid": f"{source_name}:p1",
                    "entity_key": "stock:600519.SH",
                    "action": "trade.buy",
                    "action_strength": 2,
                    "summary": "小仓试错",
                    "author": "alice",
                    "created_at": "2026-03-25 10:00:00",
                    "url": "https://example.com",
                    "stock_codes": ["600519.SH"],
                    "stock_names": ["贵州茅台"],
                }
            ]
        )
        relations = _rows(
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
    assert {str(row.get("post_uid") or "").strip() for row in assertions} == {
        "weibo:p1",
        "xueqiu:p1",
    }
    assert {str(row.get("source") or "").strip() for row in relations} == {
        "weibo",
        "xueqiu",
    }


def test_load_homework_board_payload_rows_from_env_merges_source_rows(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trade_board_loader,
        "load_configured_postgres_sources_from_env",
        lambda: [
            SourceConfigStub(name="weibo", url="u1", token="t1"),
            SourceConfigStub(name="xueqiu", url="u2", token="t2"),
        ],
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        lookback_days: int,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        del db_url, auth_token
        assert lookback_days == 3
        return (
            [
                {
                    "post_uid": f"{source_name}:p1",
                    "entity_key": "stock:600519.SH",
                    "action": "trade.buy",
                    "action_strength": 2,
                    "summary": "小仓试错",
                    "author": "alice",
                    "created_at": "2026-03-25 10:00:00",
                    "url": "https://example.com",
                }
            ],
            [
                {
                    "relation_type": "stock_alias",
                    "left_key": "stock:600519.SH",
                    "right_key": "stock:茅台",
                    "relation_label": "alias_of",
                    "source": source_name,
                    "updated_at": "2026-03-25 10:00:00",
                }
            ],
        )

    assertions, relations, err = (
        trade_board_loader.load_homework_board_payload_rows_from_env(
            3,
            load_cached_fn=_fake_cached,
        )
    )

    assert err == ""
    assert assertions == [
        {
            "post_uid": "weibo:p1",
            "entity_key": "stock:600519.SH",
            "action": "trade.buy",
            "action_strength": 2,
            "summary": "小仓试错",
            "author": "alice",
            "created_at": "2026-03-25 10:00:00",
            "url": "https://example.com",
        },
        {
            "post_uid": "xueqiu:p1",
            "entity_key": "stock:600519.SH",
            "action": "trade.buy",
            "action_strength": 2,
            "summary": "小仓试错",
            "author": "alice",
            "created_at": "2026-03-25 10:00:00",
            "url": "https://example.com",
        },
    ]
    assert relations == [
        {
            "relation_type": "stock_alias",
            "left_key": "stock:600519.SH",
            "right_key": "stock:茅台",
            "relation_label": "alias_of",
            "source": "weibo",
            "updated_at": "2026-03-25 10:00:00",
        },
        {
            "relation_type": "stock_alias",
            "left_key": "stock:600519.SH",
            "right_key": "stock:茅台",
            "relation_label": "alias_of",
            "source": "xueqiu",
            "updated_at": "2026-03-25 10:00:00",
        },
    ]


def test_load_homework_board_payload_from_env_returns_postgres_error(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trade_board_loader,
        "load_configured_postgres_sources_from_env",
        lambda: [SourceConfigStub(name="weibo", url="u1", token="t1")],
    )

    def _raise_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        lookback_days: int,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        del db_url, auth_token, source_name, lookback_days
        raise RuntimeError("boom")

    assertions, relations, err = (
        trade_board_loader.load_homework_board_payload_from_env(
            3,
            load_cached_fn=_raise_cached,
        )
    )

    assert assertions == []
    assert relations == []
    assert err == "postgres_connect_error:weibo:RuntimeError"


def test_load_homework_board_payload_from_env_keeps_partial_success(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trade_board_loader,
        "load_configured_postgres_sources_from_env",
        lambda: [
            SourceConfigStub(name="weibo", url="u1", token="t1"),
            SourceConfigStub(name="xueqiu", url="u2", token="t2"),
        ],
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        lookback_days: int,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        del db_url, auth_token
        assert lookback_days == 3
        if source_name == "xueqiu":
            raise ValueError("bad token")
        return (_rows([{"post_uid": "weibo:p1"}]), _rows([{"source": "weibo"}]))

    monkeypatch.setenv(trade_board_loader.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "2")
    assertions, relations, err = (
        trade_board_loader.load_homework_board_payload_from_env(
            3,
            load_cached_fn=_fake_cached,
        )
    )

    assert err == ""
    assert {str(row.get("post_uid") or "").strip() for row in assertions} == {
        "weibo:p1"
    }
    assert {str(row.get("source") or "").strip() for row in relations} == {"weibo"}


def test_load_homework_board_payload_from_env_fails_on_standard_error(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trade_board_loader,
        "load_configured_postgres_sources_from_env",
        lambda: [SourceConfigStub(name="weibo", url="u1", token="t1")],
    )

    def _raise_standard_error(
        db_url: str,
        auth_token: str,
        source_name: str,
        lookback_days: int,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        del db_url, auth_token, source_name, lookback_days
        raise RuntimeError("postgres_connect_error:standard:RuntimeError")

    assertions, relations, err = (
        trade_board_loader.load_homework_board_payload_from_env(
            3,
            load_cached_fn=_raise_standard_error,
        )
    )

    assert assertions == []
    assert relations == []
    assert err == "postgres_connect_error:standard:RuntimeError"


def test_load_homework_board_payload_from_env_serial_parallel_same(
    monkeypatch,
) -> None:
    sources = [
        SourceConfigStub(name="weibo", url="u1", token="t1"),
        SourceConfigStub(name="xueqiu", url="u2", token="t2"),
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
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        del db_url, auth_token
        assert lookback_days == 3
        return ([{"post_uid": f"{source_name}:p1"}], [{"source": source_name}])

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
    assert {str(row.get("post_uid") or "").strip() for row in serial_assertions} == {
        str(row.get("post_uid") or "").strip() for row in parallel_assertions
    }
    assert {str(row.get("source") or "").strip() for row in serial_relations} == {
        str(row.get("source") or "").strip() for row in parallel_relations
    }


def test_load_homework_board_payload_from_env_dedupes_global_relations(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trade_board_loader,
        "load_configured_postgres_sources_from_env",
        lambda: [
            SourceConfigStub(name="weibo", url="u1", token="t1"),
            SourceConfigStub(name="xueqiu", url="u2", token="t2"),
        ],
    )

    def _fake_cached(
        db_url: str,
        auth_token: str,
        source_name: str,
        lookback_days: int,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        del db_url, auth_token
        assert lookback_days == 3
        return (
            _rows([{"post_uid": f"{source_name}:p1"}]),
            _rows(
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
        return _rows([{"left_key": "stock:600519.SH"}])

    trade_board_loader.load_stock_alias_relations_cached.cache_clear()
    monkeypatch.setattr(
        trade_board_loader,
        "postgres_connect_autocommit",
        _fake_connect,
    )
    monkeypatch.setattr(
        trade_board_loader,
        "query_stock_alias_relation_rows",
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
        source_read,
        "get_research_workbench_engine_from_env",
        lambda: fake_engine,
        raising=False,
    )
    monkeypatch.setattr(
        source_read,
        "load_homework_trade_feed",
        _fake_load_homework_trade_feed,
        raising=False,
    )

    payload = source_read.load_homework_trade_feed_from_env()

    assert payload == {"rows": [{"topic": "stock:600519.SH"}]}
    assert seen == [fake_engine, HOMEWORK_DEFAULT_VIEW_KEY]


def test_load_sources_rows_from_env_returns_row_lists() -> None:
    posts, assertions, err = source_loader.load_sources_rows_from_env(
        load_posts_for_tree_from_env_fn=lambda: (
            [{"post_uid": "weibo:p1"}],
            "",
        ),
        load_trade_assertions_from_env_fn=lambda: (
            [{"post_uid": "weibo:p1", "action": "trade.buy"}],
            "",
        ),
    )

    assert err == ""
    assert posts == [{"post_uid": "weibo:p1"}]
    assert assertions == [{"post_uid": "weibo:p1", "action": "trade.buy"}]


def test_save_homework_trade_feed_from_env_uses_workbench_engine(monkeypatch) -> None:
    fake_engine = object()
    seen: list[object] = []

    monkeypatch.setattr(
        source_read,
        "get_research_workbench_engine_from_env",
        lambda: fake_engine,
        raising=False,
    )
    monkeypatch.setattr(
        source_read,
        "save_homework_trade_feed",
        lambda engine, **kwargs: seen.extend([engine, kwargs]),
        raising=False,
    )

    source_read.save_homework_trade_feed_from_env(
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
