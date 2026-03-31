from __future__ import annotations

from alphavault.db.turso_db import TursoEngine
from alphavault.worker.turso_runtime import ensure_turso_ready
from alphavault.worker.worker_constants import TURSO_READY_RETRY_SECONDS

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def ensure_source_turso_ready(
    *,
    source,
    source_name: str,
    now: float,
    verbose: bool,
) -> TursoEngine | None:
    if bool(getattr(source, "turso_ready", False)):
        return source.engine
    next_check = float(getattr(source, "turso_next_ready_check_at", 0.0) or 0.0)
    if now < next_check:
        return None
    ready = ensure_turso_ready(
        engine=source.engine,
        verbose=verbose,
        turso_ready=bool(getattr(source, "turso_ready", False)),
        source_name=source_name,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
    )
    source.turso_ready = bool(ready)
    if ready:
        source.turso_next_ready_check_at = 0.0
        return source.engine
    source.turso_next_ready_check_at = now + float(TURSO_READY_RETRY_SECONDS)
    return None


__all__ = ["ensure_source_turso_ready"]
