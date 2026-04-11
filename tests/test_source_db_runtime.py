from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import cast

from alphavault.db.postgres_db import PostgresEngine
from alphavault.worker import source_db_runtime


def test_ensure_source_db_ready_only_checks_connectivity(monkeypatch) -> None:
    calls: list[str] = []

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        calls.append(str(engine.dsn))
        yield object()

    monkeypatch.setattr(source_db_runtime, "postgres_connect_autocommit", _fake_connect)

    engine = cast(
        PostgresEngine,
        SimpleNamespace(dsn="postgresql://unit.test/postgres"),
    )

    assert (
        source_db_runtime.ensure_source_db_ready(
            engine=engine,
            source_db_ready=False,
            source_name="unit",
        )
        is True
    )
    assert calls == ["postgresql://unit.test/postgres"]
