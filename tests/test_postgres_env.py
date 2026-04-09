import pytest

from alphavault.constants import ENV_POSTGRES_DSN
from alphavault.db.postgres_env import (
    load_configured_postgres_sources_from_env,
    require_postgres_source_platform,
)


def test_load_configured_postgres_sources_from_single_dsn(monkeypatch) -> None:
    dsn = "postgresql://postgres:postgres@127.0.0.1:55432/postgres"
    monkeypatch.setenv(ENV_POSTGRES_DSN, dsn)
    sources = load_configured_postgres_sources_from_env()
    assert [(source.name, source.dsn, source.schema) for source in sources] == [
        ("weibo", dsn, "weibo"),
        ("xueqiu", dsn, "xueqiu"),
        ("standard", dsn, "standard"),
    ]


def test_load_configured_postgres_sources_from_empty_dsn(monkeypatch) -> None:
    monkeypatch.setenv(ENV_POSTGRES_DSN, "")
    assert load_configured_postgres_sources_from_env() == []


def test_require_postgres_source_platform_accepts_platform_name_or_post_uid() -> None:
    assert require_postgres_source_platform("weibo") == "weibo"
    assert require_postgres_source_platform("xueqiu") == "xueqiu"
    assert require_postgres_source_platform("weibo:123") == "weibo"
    assert (
        require_postgres_source_platform("xueqiu:https://xueqiu.com/test") == "xueqiu"
    )


def test_require_postgres_source_platform_rejects_unknown_value() -> None:
    with pytest.raises(RuntimeError, match="unknown_source_platform:bad:1"):
        require_postgres_source_platform("bad:1")
