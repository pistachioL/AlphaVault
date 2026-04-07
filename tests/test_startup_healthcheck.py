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
