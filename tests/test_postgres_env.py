from alphavault.constants import ENV_POSTGRES_DSN
from alphavault.db.postgres_env import load_configured_postgres_sources_from_env


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
