from __future__ import annotations

from alphavault.constants import ENV_POSTGRES_DSN
from alphavault.research_workbench import service


def test_get_research_workbench_engine_from_env_uses_postgres_dsn(
    monkeypatch,
) -> None:
    service._get_cached_research_workbench_engine.cache_clear()
    monkeypatch.setattr(service, "load_dotenv_if_present", lambda: None)
    monkeypatch.setenv(
        ENV_POSTGRES_DSN, "postgresql://postgres@127.0.0.1:5432/postgres"
    )

    captured: list[str] = []

    def _fake_ensure_postgres_engine(dsn: str):  # type: ignore[no-untyped-def]
        captured.append(dsn)
        return object()

    monkeypatch.setattr(service, "ensure_postgres_engine", _fake_ensure_postgres_engine)

    engine = service.get_research_workbench_engine_from_env()

    assert engine is not None
    assert captured == ["postgresql://postgres@127.0.0.1:5432/postgres"]


def test_get_research_workbench_engine_from_env_reuses_cached_engine(
    monkeypatch,
) -> None:
    service._get_cached_research_workbench_engine.cache_clear()
    monkeypatch.setattr(service, "load_dotenv_if_present", lambda: None)
    monkeypatch.setenv(
        ENV_POSTGRES_DSN, "postgresql://postgres@127.0.0.1:5432/postgres"
    )

    captured: list[str] = []
    cached_engine = object()

    def _fake_ensure_postgres_engine(dsn: str):  # type: ignore[no-untyped-def]
        captured.append(dsn)
        return cached_engine

    monkeypatch.setattr(service, "ensure_postgres_engine", _fake_ensure_postgres_engine)

    first = service.get_research_workbench_engine_from_env()
    second = service.get_research_workbench_engine_from_env()

    assert first is cached_engine
    assert second is cached_engine
    assert captured == ["postgresql://postgres@127.0.0.1:5432/postgres"]


def test_get_research_workbench_engine_from_env_requires_postgres_dsn(
    monkeypatch,
) -> None:
    service._get_cached_research_workbench_engine.cache_clear()
    monkeypatch.setattr(service, "load_dotenv_if_present", lambda: None)
    monkeypatch.delenv(ENV_POSTGRES_DSN, raising=False)

    try:
        service.get_research_workbench_engine_from_env()
    except RuntimeError as err:
        assert str(err) == f"missing {ENV_POSTGRES_DSN}"
    else:
        raise AssertionError("expected missing postgres dsn error")
