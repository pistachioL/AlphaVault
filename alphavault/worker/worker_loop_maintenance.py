from __future__ import annotations

from alphavault.db.postgres_db import PostgresEngine
from alphavault.worker.worker_loop_models import SourceTickContext


def run_maintenance_if_due(
    *,
    source,
    active_engine: PostgresEngine | None,
    source_name: str,
    platform: str,
    ctx: SourceTickContext,
    source_has_running_jobs: bool,
) -> bool:
    del source
    del active_engine
    del source_name
    del platform
    del ctx
    del source_has_running_jobs
    return False


__all__ = ["run_maintenance_if_due"]
