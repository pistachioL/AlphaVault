from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import cast

from alphavault.db.postgres_db import PostgresEngine
from alphavault.worker import turso_runtime


def test_ensure_turso_ready_only_checks_connectivity(monkeypatch) -> None:
    calls: list[str] = []

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        calls.append(str(engine.dsn))
        yield object()

    monkeypatch.setattr(turso_runtime, "postgres_connect_autocommit", _fake_connect)

    engine = cast(
        PostgresEngine,
        SimpleNamespace(dsn="postgresql://unit.test/postgres"),
    )

    assert (
        turso_runtime.ensure_turso_ready(
            engine=engine,
            verbose=False,
            turso_ready=False,
            source_name="unit",
        )
        is True
    )
    assert calls == ["postgresql://unit.test/postgres"]
