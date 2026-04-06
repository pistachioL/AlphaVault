from __future__ import annotations

from contextlib import contextmanager

from alphavault.db.turso_db import TursoEngine
from alphavault.worker import turso_runtime


def test_ensure_turso_ready_only_checks_connectivity(monkeypatch) -> None:
    calls: list[str] = []

    @contextmanager
    def _fake_connect(engine):  # type: ignore[no-untyped-def]
        calls.append(str(engine.remote_url))
        yield object()

    monkeypatch.setattr(turso_runtime, "turso_connect_autocommit", _fake_connect)

    engine = TursoEngine(remote_url="libsql://unit.test", auth_token="token")

    assert (
        turso_runtime.ensure_turso_ready(
            engine=engine,
            verbose=False,
            turso_ready=False,
            source_name="unit",
        )
        is True
    )
    assert calls == ["libsql://unit.test"]
