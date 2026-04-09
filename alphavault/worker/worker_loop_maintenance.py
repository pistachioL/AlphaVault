from __future__ import annotations

import time

from alphavault.db.postgres_db import PostgresEngine
from alphavault.db.turso_queue import (
    load_unprocessed_post_queue_rows,
)
from alphavault.worker import maintenance
from alphavault.worker.redis_queue import (
    redis_ai_pending_count,
    redis_ai_requeue_processing_without_lease,
    redis_try_push_ai_dedup_status,
    resolve_redis_dedup_ttl_seconds,
)
from alphavault.worker.spool import recover_spool_to_turso_and_redis
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
    active_engine: PostgresEngine | None,
    source_name: str,
    platform: str,
    ctx: SourceTickContext,
    source_has_running_jobs: bool,
) -> bool:
    if not (ctx.do_maintenance and active_engine is not None):
        return False
    do_recovery = maintenance.should_run_maintenance_recovery(
        source=source,
        maintenance_recovery_interval_cycles=int(MAINTENANCE_RECOVERY_INTERVAL_CYCLES),
    )
    redis_queue_has_pending_work = maintenance.has_pending_ai_queue_work(
        redis_client=ctx.redis_client,
        redis_queue_key=str(source.redis_queue_key or ""),
        verbose=ctx.verbose,
        pending_count_fn=redis_ai_pending_count,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
    )
    do_db_requeue = maintenance.should_requeue_unprocessed_posts_from_db(
        source=source,
        do_recovery=bool(do_recovery),
        redis_queue_has_pending_work=bool(redis_queue_has_pending_work),
        source_has_running_jobs=bool(source_has_running_jobs),
    )
    recovered, flushed_redis, turso_error = maintenance.run_turso_maintenance(
        engine=active_engine,
        platform=platform,
        spool_dir=source.spool_dir,
        redis_client=ctx.redis_client,
        redis_queue_key=str(source.redis_queue_key or ""),
        verbose=ctx.verbose,
        do_recovery=bool(do_recovery),
        do_db_requeue=bool(do_db_requeue),
        now_fn=time.time,
        recover_spool_to_turso_and_redis_fn=recover_spool_to_turso_and_redis,
        load_unprocessed_posts_for_requeue_fn=load_unprocessed_post_queue_rows,
        redis_try_push_ai_dedup_status_fn=redis_try_push_ai_dedup_status,
        resolve_redis_dedup_ttl_seconds_fn=resolve_redis_dedup_ttl_seconds,
        maybe_dispose_turso_engine_on_transient_error_fn=maybe_dispose_turso_engine_on_transient_error,
        redis_ai_requeue_processing_without_lease_fn=redis_ai_requeue_processing_without_lease,
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
            f"ok={0 if maintenance_error else 1}",
            flush=True,
        )
    return maintenance_error


__all__ = ["run_maintenance_if_due"]
