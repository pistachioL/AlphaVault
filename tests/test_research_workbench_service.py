from __future__ import annotations

from alphavault.constants import (
    ENV_STANDARD_TURSO_AUTH_TOKEN,
    ENV_STANDARD_TURSO_DATABASE_URL,
)
from alphavault.research_workbench import service


def test_get_research_workbench_engine_from_env_uses_standard_turso_env(
    monkeypatch,
) -> None:
    monkeypatch.setenv(ENV_STANDARD_TURSO_DATABASE_URL, "libsql://standard.turso.io")
    monkeypatch.setenv(ENV_STANDARD_TURSO_AUTH_TOKEN, "standard-token")

    captured: list[tuple[str, str]] = []

    def _fake_ensure_turso_engine(url: str, token: str):  # type: ignore[no-untyped-def]
        captured.append((url, token))
        return object()

    monkeypatch.setattr(service, "ensure_turso_engine", _fake_ensure_turso_engine)

    engine = service.get_research_workbench_engine_from_env()

    assert engine is not None
    assert captured == [("libsql://standard.turso.io", "standard-token")]


def test_get_research_workbench_engine_from_env_requires_standard_database_url(
    monkeypatch,
) -> None:
    monkeypatch.delenv(ENV_STANDARD_TURSO_DATABASE_URL, raising=False)
    monkeypatch.setenv(ENV_STANDARD_TURSO_AUTH_TOKEN, "standard-token")

    try:
        service.get_research_workbench_engine_from_env()
    except RuntimeError as err:
        assert str(err) == f"missing {ENV_STANDARD_TURSO_DATABASE_URL}"
    else:
        raise AssertionError("expected missing standard database url error")
