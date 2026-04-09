from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import startup_healthcheck


HEALTHCHECK_TARGET_ENV = "STARTUP_HEALTHCHECK_TURSO_TARGET"
POSTGRES_DSN = "postgresql://postgres@127.0.0.1:5432/postgres"


def _set_postgres_env(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv(startup_healthcheck.ENV_POSTGRES_DSN, POSTGRES_DSN)


def _install_fake_postgres(
    monkeypatch,
    *,
    queries_by_target: dict[str, list[str]] | None = None,
) -> list[str]:  # type: ignore[no-untyped-def]
    checked_targets: list[str] = []

    def _fake_ensure_postgres_engine(dsn: str, *, schema_name: str = ""):  # type: ignore[no-untyped-def]
        return SimpleNamespace(dsn=dsn, schema_name=schema_name)

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        target = str(engine.schema_name or "").strip()
        checked_targets.append(target)
        if queries_by_target is not None:
            queries_by_target[target] = []

        class _FakeConn:
            def execute(self, query: str) -> "_FakeConn":
                if queries_by_target is not None:
                    queries_by_target[target].append(query.strip())
                return self

            def fetchone(self) -> tuple[int]:
                return (1,)

        yield _FakeConn()

    monkeypatch.setattr(
        startup_healthcheck,
        "ensure_postgres_engine",
        _fake_ensure_postgres_engine,
    )
    monkeypatch.setattr(
        startup_healthcheck,
        "postgres_connect_autocommit",
        _fake_connect,
    )
    return checked_targets


def test_check_turso_defaults_to_standard_only(monkeypatch) -> None:
    _set_postgres_env(monkeypatch)
    monkeypatch.delenv(HEALTHCHECK_TARGET_ENV, raising=False)

    queries_by_target: dict[str, list[str]] = {}
    checked_targets = _install_fake_postgres(
        monkeypatch,
        queries_by_target=queries_by_target,
    )

    startup_healthcheck._check_turso()

    assert checked_targets == ["standard"]
    assert queries_by_target["standard"] == [
        startup_healthcheck.SELECT_ONE,
        "SELECT 1 FROM standard.security_master LIMIT 1",
        "SELECT 1 FROM standard.relations LIMIT 1",
        "SELECT 1 FROM standard.relation_candidates LIMIT 1",
        "SELECT 1 FROM standard.alias_resolve_tasks LIMIT 1",
    ]


def test_check_turso_can_target_xueqiu_only(monkeypatch) -> None:
    _set_postgres_env(monkeypatch)
    monkeypatch.setenv(HEALTHCHECK_TARGET_ENV, "xueqiu")

    checked_targets = _install_fake_postgres(monkeypatch)

    startup_healthcheck._check_turso()

    assert checked_targets == ["xueqiu"]


def test_check_turso_can_target_weibo_only(monkeypatch) -> None:
    _set_postgres_env(monkeypatch)
    monkeypatch.setenv(HEALTHCHECK_TARGET_ENV, "weibo")

    checked_targets = _install_fake_postgres(monkeypatch)

    startup_healthcheck._check_turso()

    assert checked_targets == ["weibo"]


def test_check_turso_rejects_unknown_target(monkeypatch) -> None:
    _set_postgres_env(monkeypatch)
    monkeypatch.setenv(HEALTHCHECK_TARGET_ENV, "bad-target")
    _install_fake_postgres(monkeypatch)

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
    monkeypatch.delenv(startup_healthcheck.ENV_POSTGRES_DSN, raising=False)
    monkeypatch.setenv(HEALTHCHECK_TARGET_ENV, "standard")
    _install_fake_postgres(monkeypatch)

    try:
        startup_healthcheck._check_turso()
    except RuntimeError as err:
        assert str(err) == f"missing {startup_healthcheck.ENV_POSTGRES_DSN}"
    else:
        raise AssertionError("expected missing selected target env error")


def test_check_turso_weibo_target_skips_standard_schema_queries(monkeypatch) -> None:
    _set_postgres_env(monkeypatch)
    monkeypatch.setenv(HEALTHCHECK_TARGET_ENV, "weibo")

    queries_by_target: dict[str, list[str]] = {}
    checked_targets = _install_fake_postgres(
        monkeypatch,
        queries_by_target=queries_by_target,
    )

    startup_healthcheck._check_turso()

    assert checked_targets == ["weibo"]
    assert queries_by_target["weibo"] == [startup_healthcheck.SELECT_ONE]
    assert "standard" not in queries_by_target


def test_check_turso_fails_when_standard_required_table_missing(monkeypatch) -> None:
    _set_postgres_env(monkeypatch)
    monkeypatch.delenv(HEALTHCHECK_TARGET_ENV, raising=False)

    def _fake_ensure_postgres_engine(dsn: str, *, schema_name: str = ""):  # type: ignore[no-untyped-def]
        return SimpleNamespace(dsn=dsn, schema_name=schema_name)

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        target = str(engine.schema_name or "").strip()

        class _FakeConn:
            def execute(self, query: str) -> "_FakeConn":
                if target == "standard" and "FROM standard.security_master" in query:
                    raise RuntimeError("no such table: standard.security_master")
                return self

            def fetchone(self) -> tuple[int]:
                return (1,)

        yield _FakeConn()

    monkeypatch.setattr(
        startup_healthcheck,
        "ensure_postgres_engine",
        _fake_ensure_postgres_engine,
    )
    monkeypatch.setattr(
        startup_healthcheck,
        "postgres_connect_autocommit",
        _fake_connect,
    )

    try:
        startup_healthcheck._check_turso()
    except RuntimeError as err:
        assert str(err) == (
            "postgres[standard] schema check failed: "
            "RuntimeError: no such table: standard.security_master"
        )
    else:
        raise AssertionError("expected standard schema check error")
