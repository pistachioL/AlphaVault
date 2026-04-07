from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import startup_healthcheck


def test_check_turso_requires_standard_database_url(monkeypatch) -> None:
    monkeypatch.setenv(
        startup_healthcheck.ENV_WEIBO_TURSO_DATABASE_URL, "libsql://weibo.turso.io"
    )
    monkeypatch.setenv(startup_healthcheck.ENV_WEIBO_TURSO_AUTH_TOKEN, "weibo-token")
    monkeypatch.delenv(startup_healthcheck.ENV_XUEQIU_TURSO_DATABASE_URL, raising=False)
    monkeypatch.delenv(startup_healthcheck.ENV_XUEQIU_TURSO_AUTH_TOKEN, raising=False)
    monkeypatch.delenv("STANDARD_TURSO_DATABASE_URL", raising=False)
    monkeypatch.delenv("STANDARD_TURSO_AUTH_TOKEN", raising=False)

    def _fake_ensure_turso_engine(url: str, token: str):  # type: ignore[no-untyped-def]
        return SimpleNamespace(remote_url=url, auth_token=token)

    @contextmanager
    def _fake_connect(_engine):  # type: ignore[no-untyped-def]
        class _FakeConn:
            def execute(self, _query: str) -> "_FakeConn":
                return self

            def fetchone(self) -> tuple[int]:
                return (1,)

        yield _FakeConn()

    monkeypatch.setattr(
        startup_healthcheck, "ensure_turso_engine", _fake_ensure_turso_engine
    )
    monkeypatch.setattr(startup_healthcheck, "turso_connect_autocommit", _fake_connect)

    try:
        startup_healthcheck._check_turso()
    except RuntimeError as err:
        assert str(err) == "missing STANDARD_TURSO_DATABASE_URL"
    else:
        raise AssertionError("expected missing standard database url error")


def test_check_turso_checks_source_and_standard_targets(monkeypatch) -> None:
    monkeypatch.setenv(
        startup_healthcheck.ENV_WEIBO_TURSO_DATABASE_URL, "libsql://weibo.turso.io"
    )
    monkeypatch.setenv(startup_healthcheck.ENV_WEIBO_TURSO_AUTH_TOKEN, "weibo-token")
    monkeypatch.delenv(startup_healthcheck.ENV_XUEQIU_TURSO_DATABASE_URL, raising=False)
    monkeypatch.delenv(startup_healthcheck.ENV_XUEQIU_TURSO_AUTH_TOKEN, raising=False)
    monkeypatch.setenv("STANDARD_TURSO_DATABASE_URL", "libsql://standard.turso.io")
    monkeypatch.setenv("STANDARD_TURSO_AUTH_TOKEN", "standard-token")

    checked_urls: list[str] = []

    def _fake_ensure_turso_engine(url: str, token: str):  # type: ignore[no-untyped-def]
        return SimpleNamespace(remote_url=url, auth_token=token)

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        checked_urls.append(str(engine.remote_url))

        class _FakeConn:
            def execute(self, _query: str) -> "_FakeConn":
                return self

            def fetchone(self) -> tuple[int]:
                return (1,)

        yield _FakeConn()

    monkeypatch.setattr(
        startup_healthcheck, "ensure_turso_engine", _fake_ensure_turso_engine
    )
    monkeypatch.setattr(startup_healthcheck, "turso_connect_autocommit", _fake_connect)

    startup_healthcheck._check_turso()

    assert checked_urls == [
        "libsql://weibo.turso.io",
        "libsql://standard.turso.io",
    ]


def test_check_turso_checks_standard_required_tables(monkeypatch) -> None:
    monkeypatch.setenv(
        startup_healthcheck.ENV_WEIBO_TURSO_DATABASE_URL, "libsql://weibo.turso.io"
    )
    monkeypatch.setenv(startup_healthcheck.ENV_WEIBO_TURSO_AUTH_TOKEN, "weibo-token")
    monkeypatch.delenv(startup_healthcheck.ENV_XUEQIU_TURSO_DATABASE_URL, raising=False)
    monkeypatch.delenv(startup_healthcheck.ENV_XUEQIU_TURSO_AUTH_TOKEN, raising=False)
    monkeypatch.setenv("STANDARD_TURSO_DATABASE_URL", "libsql://standard.turso.io")
    monkeypatch.setenv("STANDARD_TURSO_AUTH_TOKEN", "standard-token")

    queries_by_url: dict[str, list[str]] = {}

    def _fake_ensure_turso_engine(url: str, token: str):  # type: ignore[no-untyped-def]
        return SimpleNamespace(remote_url=url, auth_token=token)

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        url = str(engine.remote_url)
        queries_by_url[url] = []

        class _FakeConn:
            def execute(self, query: str) -> "_FakeConn":
                queries_by_url[url].append(query.strip())
                return self

            def fetchone(self) -> tuple[int]:
                return (1,)

        yield _FakeConn()

    monkeypatch.setattr(
        startup_healthcheck, "ensure_turso_engine", _fake_ensure_turso_engine
    )
    monkeypatch.setattr(startup_healthcheck, "turso_connect_autocommit", _fake_connect)

    startup_healthcheck._check_turso()

    assert queries_by_url["libsql://weibo.turso.io"] == [startup_healthcheck.SELECT_ONE]
    assert queries_by_url["libsql://standard.turso.io"] == [
        startup_healthcheck.SELECT_ONE,
        "SELECT 1 FROM security_master LIMIT 1",
        "SELECT 1 FROM relations LIMIT 1",
        "SELECT 1 FROM relation_candidates LIMIT 1",
        "SELECT 1 FROM alias_resolve_tasks LIMIT 1",
    ]


def test_check_turso_fails_when_standard_required_table_missing(monkeypatch) -> None:
    monkeypatch.setenv(
        startup_healthcheck.ENV_WEIBO_TURSO_DATABASE_URL, "libsql://weibo.turso.io"
    )
    monkeypatch.setenv(startup_healthcheck.ENV_WEIBO_TURSO_AUTH_TOKEN, "weibo-token")
    monkeypatch.delenv(startup_healthcheck.ENV_XUEQIU_TURSO_DATABASE_URL, raising=False)
    monkeypatch.delenv(startup_healthcheck.ENV_XUEQIU_TURSO_AUTH_TOKEN, raising=False)
    monkeypatch.setenv("STANDARD_TURSO_DATABASE_URL", "libsql://standard.turso.io")
    monkeypatch.setenv("STANDARD_TURSO_AUTH_TOKEN", "standard-token")

    def _fake_ensure_turso_engine(url: str, token: str):  # type: ignore[no-untyped-def]
        return SimpleNamespace(remote_url=url, auth_token=token)

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        url = str(engine.remote_url)

        class _FakeConn:
            def execute(self, query: str) -> "_FakeConn":
                if (
                    url == "libsql://standard.turso.io"
                    and "FROM security_master" in query
                ):
                    raise RuntimeError("no such table: security_master")
                return self

            def fetchone(self) -> tuple[int]:
                return (1,)

        yield _FakeConn()

    monkeypatch.setattr(
        startup_healthcheck, "ensure_turso_engine", _fake_ensure_turso_engine
    )
    monkeypatch.setattr(startup_healthcheck, "turso_connect_autocommit", _fake_connect)

    try:
        startup_healthcheck._check_turso()
    except RuntimeError as err:
        assert str(err) == (
            "turso[standard] schema check failed: "
            "RuntimeError: no such table: security_master"
        )
    else:
        raise AssertionError("expected standard schema check error")
