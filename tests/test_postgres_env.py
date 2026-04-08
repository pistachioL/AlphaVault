from alphavault.db.postgres_env import load_configured_postgres_sources_from_env


def test_load_configured_postgres_sources_from_single_dsn(monkeypatch) -> None:
    monkeypatch.setenv(
        "POSTGRES_DSN",
        "postgresql://postgres:postgres@127.0.0.1:55432/postgres",
    )
    sources = load_configured_postgres_sources_from_env()
    assert [source.name for source in sources] == ["weibo", "xueqiu", "standard"]
