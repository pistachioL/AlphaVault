from __future__ import annotations

import pandas as pd

from alphavault.db.turso_env import TursoSource
from alphavault_reflex.services import turso_read


def test_resolve_homework_source_workers_uses_default_when_env_missing(
    monkeypatch,
) -> None:
    monkeypatch.delenv(turso_read.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, raising=False)
    assert turso_read._resolve_homework_source_workers(source_count=5) == 2


def test_resolve_homework_source_workers_falls_back_on_invalid_env(
    monkeypatch,
) -> None:
    monkeypatch.setenv(turso_read.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "bad")
    assert turso_read._resolve_homework_source_workers(source_count=5) == 2


def test_resolve_homework_source_workers_clamps_to_valid_bounds(
    monkeypatch,
) -> None:
    monkeypatch.setenv(turso_read.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "0")
    assert turso_read._resolve_homework_source_workers(source_count=5) == 1

    monkeypatch.setenv(turso_read.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "9")
    assert turso_read._resolve_homework_source_workers(source_count=2) == 2


def test_load_homework_board_payload_from_env_merges_source_rows(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        turso_read,
        "load_configured_turso_sources_from_env",
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
                    "topic_key": "stock:600519.SH",
                    "action": "trade.buy",
                    "action_strength": 2,
                    "summary": "小仓试错",
                    "author": "alice",
                    "created_at": pd.Timestamp("2026-03-25 10:00:00"),
                    "url": "https://example.com",
                    "stock_codes_json": '["600519.SH"]',
                    "stock_names_json": '["贵州茅台"]',
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

    monkeypatch.setattr(turso_read, "_load_homework_board_payload_cached", _fake_cached)

    assertions, relations, err = turso_read.load_homework_board_payload_from_env(3)

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
        turso_read,
        "load_configured_turso_sources_from_env",
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

    monkeypatch.setattr(
        turso_read, "_load_homework_board_payload_cached", _raise_cached
    )

    assertions, relations, err = turso_read.load_homework_board_payload_from_env(3)

    assert assertions.empty
    assert relations.empty
    assert err == "turso_connect_error:weibo:RuntimeError"


def test_load_homework_board_payload_from_env_serial_parallel_same(
    monkeypatch,
) -> None:
    sources = [
        TursoSource(name="weibo", url="u1", token="t1"),
        TursoSource(name="xueqiu", url="u2", token="t2"),
    ]
    monkeypatch.setattr(
        turso_read,
        "load_configured_turso_sources_from_env",
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

    monkeypatch.setattr(turso_read, "_load_homework_board_payload_cached", _fake_cached)

    monkeypatch.setenv(turso_read.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "1")
    serial_assertions, serial_relations, serial_err = (
        turso_read.load_homework_board_payload_from_env(3)
    )

    monkeypatch.setenv(turso_read.ENV_REFLEX_HOMEWORK_SOURCE_MAX_WORKERS, "2")
    parallel_assertions, parallel_relations, parallel_err = (
        turso_read.load_homework_board_payload_from_env(3)
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
