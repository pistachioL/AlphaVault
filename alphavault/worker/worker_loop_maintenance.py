from __future__ import annotations

import time

from alphavault.db.turso_db import TursoEngine
from alphavault.db.turso_queue import (
    recover_done_without_processed_at,
    recover_stuck_ai_tasks,
)
from alphavault.worker import maintenance
from alphavault.worker.assertion_outbox import pump_assertion_outbox_to_redis
from alphavault.worker.redis_queue import redis_ai_requeue_processing
from alphavault.worker.turso_runtime import (
    maybe_dispose_turso_engine_on_transient_error,
)
from alphavault.worker.worker_constants import (
    MAINTENANCE_RECOVERY_INTERVAL_CYCLES,
    REDIS_AI_REQUEUE_MAX_ITEMS,
)
from alphavault.worker.worker_loop_models import SourceTickContext

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def run_maintenance_if_due(
    *,
    source,
    active_engine: TursoEngine | None,
    source_name: str,
    platform: str,
    ctx: SourceTickContext,
) -> bool:
    if not (ctx.do_maintenance and active_engine is not None):
        return False
    do_recovery = maintenance.should_run_maintenance_recovery(
        source=source,
        force_maintenance=False,
        maintenance_recovery_interval_cycles=int(MAINTENANCE_RECOVERY_INTERVAL_CYCLES),
    )
    recovered, flushed_redis, turso_error = maintenance.run_turso_maintenance(
        engine=active_engine,
        platform=platform,
        spool_dir=source.spool_dir,
        redis_client=ctx.redis_client,
        redis_queue_key=str(source.redis_queue_key or ""),
        stuck_seconds=int(ctx.stuck_seconds),
        verbose=ctx.verbose,
        do_recovery=bool(do_recovery),
        now_fn=time.time,
        recover_stuck_ai_tasks_fn=recover_stuck_ai_tasks,
        recover_done_without_processed_at_fn=recover_done_without_processed_at,
        maybe_dispose_turso_engine_on_transient_error_fn=maybe_dispose_turso_engine_on_transient_error,
        redis_ai_requeue_processing_fn=redis_ai_requeue_processing,
        pump_assertion_outbox_to_redis_fn=pump_assertion_outbox_to_redis,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        redis_ai_requeue_max_items=int(REDIS_AI_REQUEUE_MAX_ITEMS),
    )
    maintenance_error = bool(turso_error)
    maintenance.update_maintenance_recovery_state(
        source=source,
        recovered=int(recovered),
        maintenance_error=maintenance_error,
    )
    if ctx.verbose and (recovered > 0 or flushed_redis > 0 or maintenance_error):
        print(
            f"[maintenance:{source_name}] recovered={recovered} flushed_redis={flushed_redis} "
            f"error={1 if maintenance_error else 0}",
            flush=True,
        )
    return maintenance_error


__all__ = ["run_maintenance_if_due"]
