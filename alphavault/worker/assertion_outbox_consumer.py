from __future__ import annotations

import json

from alphavault.db.sql.turso_queue import SELECT_ASSERTION_OUTBOX_AFTER_ID
from alphavault.db.turso_db import TursoConnection, TursoEngine, is_fatal_base_exception
from alphavault.db.turso_queue import AssertionOutboxEvent, load_assertion_outbox_events
from alphavault.worker.local_cache import (
    CACHE_ASSERTIONS_TABLE,
    CACHE_STOCK_CODES_TABLE,
    CACHE_STOCK_NAMES_TABLE,
    apply_outbox_event_payload,
    open_local_cache,
    resolve_local_cache_db_path,
)


LOCAL_CACHE_REBUILD_BATCH_SIZE = 200
LOCAL_CACHE_REBUILD_LOG_EVERY = 2000


def _log_cache_rebuild(*, verbose: bool, message: str) -> None:
    if not verbose:
        return
    print(f"[local_cache] {message}", flush=True)


def _load_outbox_events(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    after_id: int,
    limit: int,
) -> list[AssertionOutboxEvent]:
    if isinstance(engine_or_conn, TursoEngine):
        return load_assertion_outbox_events(
            engine_or_conn,
            after_id=max(0, int(after_id)),
            limit=max(1, int(limit)),
        )
    conn = engine_or_conn
    rows = (
        conn.execute(
            SELECT_ASSERTION_OUTBOX_AFTER_ID,
            {"after_id": max(0, int(after_id)), "limit": max(1, int(limit))},
        )
        .mappings()
        .fetchall()
    )
    out: list[AssertionOutboxEvent] = []
    for row in rows:
        if not row:
            continue
        out.append(
            AssertionOutboxEvent(
                id=int(row.get("id") or 0),
                source=str(row.get("source") or "").strip(),
                post_uid=str(row.get("post_uid") or "").strip(),
                author=str(row.get("author") or "").strip(),
                event_json=str(row.get("event_json") or ""),
                created_at=str(row.get("created_at") or "").strip(),
            )
        )
    return out


def _clear_local_cache_tables(cache_conn) -> None:
    with cache_conn:
        cache_conn.execute(f"DELETE FROM {CACHE_STOCK_CODES_TABLE}")
        cache_conn.execute(f"DELETE FROM {CACHE_STOCK_NAMES_TABLE}")
        cache_conn.execute(f"DELETE FROM {CACHE_ASSERTIONS_TABLE}")


def rebuild_local_cache_from_outbox(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    source_name: str,
    batch_size: int = LOCAL_CACHE_REBUILD_BATCH_SIZE,
    verbose: bool = False,
) -> dict[str, int | bool]:
    """
    Rebuild local sqlite cache from Turso outbox (full replay).

    This is intentionally "memory-like":
    - Restart -> treat cache as gone.
    - On startup -> replay all outbox events and rebuild local sqlite.
    - Runtime updates are handled by write-through on AI done.
    """
    limit = max(1, int(batch_size))
    db_path = resolve_local_cache_db_path(source_name=str(source_name or "").strip())
    processed_events = 0
    inserted_rows = 0
    last_id = 0

    try:
        with open_local_cache(db_path=db_path) as cache_conn:
            _clear_local_cache_tables(cache_conn)
            _log_cache_rebuild(verbose=verbose, message="rebuild_start cleared=1")

            after_id = 0
            while True:
                events = _load_outbox_events(
                    engine_or_conn,
                    after_id=int(after_id),
                    limit=int(limit),
                )
                if not events:
                    break
                for event in events:
                    processed_events += 1
                    last_id = max(int(last_id), int(event.id))
                    after_id = int(last_id)
                    try:
                        payload = json.loads(str(event.event_json or ""))
                    except Exception:
                        continue
                    if not isinstance(payload, dict):
                        continue
                    if str(payload.get("event_type") or "").strip() != "ai_done":
                        continue
                    inserted_rows += int(
                        apply_outbox_event_payload(cache_conn, payload=payload)
                    )
                    if (
                        verbose
                        and processed_events % int(LOCAL_CACHE_REBUILD_LOG_EVERY) == 0
                    ):
                        _log_cache_rebuild(
                            verbose=verbose,
                            message=(
                                f"rebuild_progress processed={int(processed_events)} "
                                f"inserted={int(inserted_rows)} "
                                f"cursor={int(last_id)}"
                            ),
                        )
                if len(events) < int(limit):
                    break
    except BaseException as err:
        if is_fatal_base_exception(err):
            raise
        _log_cache_rebuild(
            verbose=verbose,
            message=f"rebuild_error {type(err).__name__}: {err}",
        )
        return {
            "processed": int(processed_events),
            "inserted": int(inserted_rows),
            "cursor": int(last_id),
            "has_error": True,
        }

    _log_cache_rebuild(
        verbose=verbose,
        message=(
            f"rebuild_done processed={int(processed_events)} inserted={int(inserted_rows)} "
            f"cursor={int(last_id)}"
        ),
    )
    return {
        "processed": int(processed_events),
        "inserted": int(inserted_rows),
        "cursor": int(last_id),
        "has_error": False,
    }


__all__ = [
    "rebuild_local_cache_from_outbox",
    "LOCAL_CACHE_REBUILD_BATCH_SIZE",
]
