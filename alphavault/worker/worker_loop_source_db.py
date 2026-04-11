from __future__ import annotations

from alphavault.db.postgres_db import PostgresEngine
from alphavault.worker.source_db_runtime import ensure_source_db_ready
from alphavault.worker.worker_constants import SOURCE_DB_READY_RETRY_SECONDS

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def ensure_source_db_ready_for_tick(
    *,
    source,
    source_name: str,
    now: float,
) -> PostgresEngine | None:
    if bool(getattr(source, "source_db_ready", False)):
        return source.engine
    next_check = float(getattr(source, "source_db_next_ready_check_at", 0.0) or 0.0)
    if now < next_check:
        return None
    ready = ensure_source_db_ready(
        engine=source.engine,
        source_db_ready=bool(getattr(source, "source_db_ready", False)),
        source_name=source_name,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
    )
    source.source_db_ready = bool(ready)
    if ready:
        source.source_db_next_ready_check_at = 0.0
        return source.engine
    source.source_db_next_ready_check_at = now + float(SOURCE_DB_READY_RETRY_SECONDS)
    return None


__all__ = ["ensure_source_db_ready_for_tick"]
