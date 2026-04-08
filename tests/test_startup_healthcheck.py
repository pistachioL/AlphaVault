from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import startup_healthcheck


HEALTHCHECK_TARGET_ENV = "STARTUP_HEALTHCHECK_TURSO_TARGET"
WEIBO_URL = "libsql://weibo.turso.io"
WEIBO_TOKEN = "weibo-token"
XUEQIU_URL = "libsql://xueqiu.turso.io"
XUEQIU_TOKEN = "xueqiu-token"
STANDARD_URL = "libsql://standard.turso.io"
STANDARD_TOKEN = "standard-token"


def _set_turso_envs(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv(startup_healthcheck.ENV_WEIBO_TURSO_DATABASE_URL, WEIBO_URL)
    monkeypatch.setenv(startup_healthcheck.ENV_WEIBO_TURSO_AUTH_TOKEN, WEIBO_TOKEN)
    monkeypatch.setenv(startup_healthcheck.ENV_XUEQIU_TURSO_DATABASE_URL, XUEQIU_URL)
    monkeypatch.setenv(startup_healthcheck.ENV_XUEQIU_TURSO_AUTH_TOKEN, XUEQIU_TOKEN)
    monkeypatch.setenv(
        startup_healthcheck.ENV_STANDARD_TURSO_DATABASE_URL, STANDARD_URL
    )
    monkeypatch.setenv(
        startup_healthcheck.ENV_STANDARD_TURSO_AUTH_TOKEN, STANDARD_TOKEN
    )


def _install_fake_turso(
    monkeypatch,
    *,
    queries_by_url: dict[str, list[str]] | None = None,
) -> list[str]:  # type: ignore[no-untyped-def]
    checked_urls: list[str] = []

    def _fake_ensure_turso_engine(url: str, token: str):  # type: ignore[no-untyped-def]
        return SimpleNamespace(remote_url=url, auth_token=token)

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        url = str(engine.remote_url)
        checked_urls.append(url)
        if queries_by_url is not None:
            queries_by_url[url] = []

        class _FakeConn:
            def execute(self, query: str) -> "_FakeConn":
                if queries_by_url is not None:
                    queries_by_url[url].append(query.strip())
                return self

            def fetchone(self) -> tuple[int]:
                return (1,)

        yield _FakeConn()

    monkeypatch.setattr(
        startup_healthcheck, "ensure_turso_engine", _fake_ensure_turso_engine
    )
    monkeypatch.setattr(startup_healthcheck, "turso_connect_autocommit", _fake_connect)
    return checked_urls


def test_check_turso_defaults_to_standard_only(monkeypatch) -> None:
    monkeypatch.delenv(startup_healthcheck.ENV_WEIBO_TURSO_DATABASE_URL, raising=False)
    monkeypatch.delenv(startup_healthcheck.ENV_WEIBO_TURSO_AUTH_TOKEN, raising=False)
    monkeypatch.delenv(startup_healthcheck.ENV_XUEQIU_TURSO_DATABASE_URL, raising=False)
    monkeypatch.delenv(startup_healthcheck.ENV_XUEQIU_TURSO_AUTH_TOKEN, raising=False)
    monkeypatch.setenv(
        startup_healthcheck.ENV_STANDARD_TURSO_DATABASE_URL, STANDARD_URL
    )
    monkeypatch.setenv(
        startup_healthcheck.ENV_STANDARD_TURSO_AUTH_TOKEN, STANDARD_TOKEN
    )
    monkeypatch.delenv(HEALTHCHECK_TARGET_ENV, raising=False)

    queries_by_url: dict[str, list[str]] = {}
    checked_urls = _install_fake_turso(monkeypatch, queries_by_url=queries_by_url)

    startup_healthcheck._check_turso()

    assert checked_urls == [STANDARD_URL]
    assert queries_by_url[STANDARD_URL] == [
        startup_healthcheck.SELECT_ONE,
        "SELECT 1 FROM security_master LIMIT 1",
        "SELECT 1 FROM relations LIMIT 1",
        "SELECT 1 FROM relation_candidates LIMIT 1",
        "SELECT 1 FROM alias_resolve_tasks LIMIT 1",
    ]


def test_check_turso_can_target_xueqiu_only(monkeypatch) -> None:
    _set_turso_envs(monkeypatch)
    monkeypatch.setenv(HEALTHCHECK_TARGET_ENV, "xueqiu")

    checked_urls = _install_fake_turso(monkeypatch)

    startup_healthcheck._check_turso()

    assert checked_urls == [XUEQIU_URL]


def test_check_turso_can_target_weibo_only(monkeypatch) -> None:
    _set_turso_envs(monkeypatch)
    monkeypatch.setenv(HEALTHCHECK_TARGET_ENV, "weibo")

    checked_urls = _install_fake_turso(monkeypatch)

    startup_healthcheck._check_turso()

    assert checked_urls == [WEIBO_URL]


def test_check_turso_rejects_unknown_target(monkeypatch) -> None:
    _set_turso_envs(monkeypatch)
    monkeypatch.setenv(HEALTHCHECK_TARGET_ENV, "bad-target")
    _install_fake_turso(monkeypatch)

    try:
        startup_healthcheck._check_turso()
    except RuntimeError as err:
        assert str(err) == (
            "invalid STARTUP_HEALTHCHECK_TURSO_TARGET: bad-target "
            "(expected weibo, xueqiu, or standard)"
        )
    else:
        raise AssertionError("expected invalid target error")


def test_check_turso_requires_selected_target_database_url(monkeypatch) -> None:
    monkeypatch.delenv(
        startup_healthcheck.ENV_STANDARD_TURSO_DATABASE_URL, raising=False
    )
    monkeypatch.delenv(startup_healthcheck.ENV_STANDARD_TURSO_AUTH_TOKEN, raising=False)
    monkeypatch.setenv(HEALTHCHECK_TARGET_ENV, "standard")
    _install_fake_turso(monkeypatch)

    try:
        startup_healthcheck._check_turso()
    except RuntimeError as err:
        assert (
            str(err) == f"missing {startup_healthcheck.ENV_STANDARD_TURSO_DATABASE_URL}"
        )
    else:
        raise AssertionError("expected missing selected target env error")


def test_check_turso_weibo_target_skips_standard_schema_queries(monkeypatch) -> None:
    _set_turso_envs(monkeypatch)
    monkeypatch.setenv(HEALTHCHECK_TARGET_ENV, "weibo")

    queries_by_url: dict[str, list[str]] = {}
    checked_urls = _install_fake_turso(monkeypatch, queries_by_url=queries_by_url)

    startup_healthcheck._check_turso()

    assert checked_urls == [WEIBO_URL]
    assert queries_by_url[WEIBO_URL] == [startup_healthcheck.SELECT_ONE]
    assert STANDARD_URL not in queries_by_url


def test_check_turso_fails_when_standard_required_table_missing(monkeypatch) -> None:
    monkeypatch.delenv(startup_healthcheck.ENV_WEIBO_TURSO_DATABASE_URL, raising=False)
    monkeypatch.delenv(startup_healthcheck.ENV_WEIBO_TURSO_AUTH_TOKEN, raising=False)
    monkeypatch.delenv(startup_healthcheck.ENV_XUEQIU_TURSO_DATABASE_URL, raising=False)
    monkeypatch.delenv(startup_healthcheck.ENV_XUEQIU_TURSO_AUTH_TOKEN, raising=False)
    monkeypatch.setenv(
        startup_healthcheck.ENV_STANDARD_TURSO_DATABASE_URL, STANDARD_URL
    )
    monkeypatch.setenv(
        startup_healthcheck.ENV_STANDARD_TURSO_AUTH_TOKEN, STANDARD_TOKEN
    )
    monkeypatch.delenv(HEALTHCHECK_TARGET_ENV, raising=False)

    def _fake_ensure_turso_engine(url: str, token: str):  # type: ignore[no-untyped-def]
        return SimpleNamespace(remote_url=url, auth_token=token)

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        url = str(engine.remote_url)

        class _FakeConn:
            def execute(self, query: str) -> "_FakeConn":
                if url == STANDARD_URL and "FROM security_master" in query:
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
