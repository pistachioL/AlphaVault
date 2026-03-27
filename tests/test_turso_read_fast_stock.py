from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from alphavault_reflex.services import turso_read


def test_load_stock_sources_fast_from_env_returns_partial_error(monkeypatch) -> None:
    monkeypatch.setattr(turso_read, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        turso_read,
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
            [{"post_uid": "weibo:1", "topic_key": "stock:03316.HK"}]
        )
        return posts, assertions

    monkeypatch.setattr(
        turso_read,
        "_load_stock_trade_sources_fast_cached",
        _fake_fast_cached,
    )

    posts, assertions, err = turso_read.load_stock_sources_fast_from_env(
        "03316.HK",
        per_source_limit=16,
    )

    assert not posts.empty
    assert not assertions.empty
    assert err.startswith("partial_source_error:")


def test_load_stock_sources_fast_from_env_normalizes_stock_key(monkeypatch) -> None:
    monkeypatch.setattr(turso_read, "load_dotenv_if_present", lambda: None)
    monkeypatch.setattr(
        turso_read,
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

    monkeypatch.setattr(
        turso_read,
        "_load_stock_trade_sources_fast_cached",
        _fake_fast_cached,
    )

    _posts, _assertions, _err = turso_read.load_stock_sources_fast_from_env("03316.HK")
    assert seen_stock_keys == ["stock:03316.HK"]
