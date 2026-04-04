from __future__ import annotations

from datetime import UTC, datetime
from typing import Callable

from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    turso_connect_autocommit,
)
from alphavault.research_backfill_cache import list_stock_backfill_posts
from alphavault.research_stock_cache import (
    ENTITY_PAGE_SNAPSHOT_TABLE,
    ensure_research_stock_cache_schema,
    list_entity_page_dirty_entries,
    list_entity_page_dirty_keys,
    load_entity_page_backfill_snapshot,
    remove_entity_page_dirty_keys,
    save_entity_page_backfill_snapshot,
    save_entity_page_signal_snapshot,
)
from alphavault.worker.job_state import (
    load_worker_job_cursor,
    release_worker_job_lock,
    save_worker_job_cursor,
    try_acquire_worker_job_lock,
)
from alphavault.worker.stock_hot_payload_builder import (
    build_stock_hot_payload,
    normalize_stock_key,
)

STOCK_HOT_CACHE_LOCK_KEY = "stock_hot_cache.lock"
STOCK_HOT_CACHE_BOOTSTRAP_CURSOR_STATE_KEY = (
    "worker.progress.stock_hot.bootstrap_cursor"
)
STOCK_HOT_CACHE_LOCK_LEASE_SECONDS = 600
STOCK_HOT_CACHE_MAX_STOCKS_PER_RUN = 4
STOCK_HOT_CACHE_DIRTY_LIMIT = 16
STOCK_HOT_CACHE_SIGNAL_WINDOW_DAYS = 30
STOCK_HOT_CACHE_SIGNAL_CAP = 500
STOCK_EXTRAS_REFRESH_MIN_SECONDS = 900

_EXTRAS_FORCE_REFRESH_REASONS = {
    "backfill_cache",
    "queue_backfill",
}

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _log_stock_hot_cache(*, verbose: bool, message: str) -> None:
    if not verbose:
        return
    print(f"[stock_hot_cache] {message}", flush=True)


def _load_bootstrap_cursor(conn: object) -> str:
    if not isinstance(conn, TursoConnection):
        return ""
    try:
        return load_worker_job_cursor(
            conn,
            state_key=STOCK_HOT_CACHE_BOOTSTRAP_CURSOR_STATE_KEY,
        )
    except BaseException:
        return ""


def _save_bootstrap_cursor(conn: object, cursor: str) -> None:
    if not isinstance(conn, TursoConnection):
        return
    try:
        save_worker_job_cursor(
            conn,
            state_key=STOCK_HOT_CACHE_BOOTSTRAP_CURSOR_STATE_KEY,
            cursor=cursor,
        )
    except BaseException:
        return


def _list_missing_hot_cache_stock_keys(
    conn: TursoConnection,
    *,
    limit: int,
    after_stock_key: str = "",
) -> list[str]:
    cursor = str(after_stock_key or "").strip()
    cursor_sql = ""
    params: dict[str, object] = {"limit": max(1, int(limit))}
    if cursor:
        cursor_sql = "AND ae.entity_key > :after_stock_key"
        params["after_stock_key"] = cursor
    sql = f"""
SELECT DISTINCT ae.entity_key
FROM assertions a
JOIN assertion_entities ae
  ON ae.post_uid = a.post_uid AND ae.assertion_idx = a.idx
WHERE a.action LIKE 'trade.%'
  AND ae.entity_type = 'stock'
  AND ae.entity_key LIKE 'stock:%'
  {cursor_sql}
  AND NOT EXISTS (
    SELECT 1
    FROM {ENTITY_PAGE_SNAPSHOT_TABLE} hot
    WHERE hot.entity_key = ae.entity_key
  )
ORDER BY ae.entity_key ASC
LIMIT :limit
"""
    rows = conn.execute(sql, params).fetchall()
    return [
        str(row[0] or "").strip()
        for row in rows
        if row and str(row[0] or "").strip().startswith("stock:")
    ]


def _has_missing_hot_cache_stock_keys(
    conn: TursoConnection,
    *,
    after_stock_key: str = "",
) -> bool:
    keys = _list_missing_hot_cache_stock_keys(
        conn,
        limit=1,
        after_stock_key=after_stock_key,
    )
    return bool(keys)


def _parse_naive_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _extras_force_refresh(reason: str) -> bool:
    return str(reason or "").strip() in _EXTRAS_FORCE_REFRESH_REASONS


def _is_extras_snapshot_stale(
    conn: TursoConnection,
    *,
    stock_key: str,
    min_refresh_seconds: int,
) -> bool:
    snapshot = load_entity_page_backfill_snapshot(conn, stock_key=stock_key)
    updated_at = _parse_naive_datetime(snapshot.get("updated_at"))
    if updated_at is None:
        return True
    delta = datetime.now() - updated_at
    return float(delta.total_seconds()) >= max(0, int(min_refresh_seconds))


def refresh_stock_hot_for_key(
    conn: TursoConnection,
    *,
    stock_key: str,
    signal_window_days: int,
    signal_cap: int,
) -> str:
    payload = build_stock_hot_payload(
        conn,
        stock_key=stock_key,
        signal_window_days=int(signal_window_days),
        signal_cap=int(signal_cap),
    )
    entity_key = (
        str(payload.get("entity_key") or stock_key).strip() or str(stock_key).strip()
    )
    save_entity_page_signal_snapshot(conn, stock_key=entity_key, payload=payload)
    return entity_key


def refresh_stock_extras_snapshot_for_key(
    conn: TursoConnection,
    *,
    stock_key: str,
    min_refresh_seconds: int,
    force: bool,
) -> bool:
    entity_key = normalize_stock_key(stock_key)
    if not entity_key:
        return False
    if (not force) and (
        not _is_extras_snapshot_stale(
            conn,
            stock_key=entity_key,
            min_refresh_seconds=int(min_refresh_seconds),
        )
    ):
        return False
    backfill = list_stock_backfill_posts(
        conn,
        stock_key=entity_key,
        limit=12,
    )
    save_entity_page_backfill_snapshot(
        conn,
        stock_key=entity_key,
        backfill_posts=backfill,
    )
    return True


def sync_stock_hot_cache(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    max_stocks_per_run: int = STOCK_HOT_CACHE_MAX_STOCKS_PER_RUN,
    dirty_limit: int = STOCK_HOT_CACHE_DIRTY_LIMIT,
    signal_window_days: int = STOCK_HOT_CACHE_SIGNAL_WINDOW_DAYS,
    signal_cap: int = STOCK_HOT_CACHE_SIGNAL_CAP,
    extras_refresh_min_seconds: int = STOCK_EXTRAS_REFRESH_MIN_SECONDS,
    lock_lease_seconds: int = STOCK_HOT_CACHE_LOCK_LEASE_SECONDS,
    should_continue: Callable[[], bool] | None = None,
    verbose: bool = False,
) -> dict[str, int | bool]:
    _log_stock_hot_cache(
        verbose=verbose,
        message=(
            "start "
            f"max_stocks_per_run={max(1, int(max_stocks_per_run))} "
            f"dirty_limit={max(1, int(dirty_limit))} "
            f"signal_window_days={max(1, int(signal_window_days))} "
            f"signal_cap={max(1, int(signal_cap))}"
        ),
    )
    now_epoch = int(datetime.now(tz=UTC).timestamp())
    if not try_acquire_worker_job_lock(
        engine_or_conn,
        lock_key=STOCK_HOT_CACHE_LOCK_KEY,
        now_epoch=now_epoch,
        lease_seconds=int(lock_lease_seconds),
    ):
        _log_stock_hot_cache(verbose=verbose, message="lock_busy skip=1")
        return {"processed": 0, "written": 0, "has_more": False, "locked": True}
    try:
        ensure_research_stock_cache_schema(engine_or_conn)
        with (
            turso_connect_autocommit(engine_or_conn)
            if isinstance(engine_or_conn, TursoEngine)
            else engine_or_conn
        ) as conn:
            bootstrap_keys: list[str] = []
            dirty_entries = list_entity_page_dirty_entries(
                conn,
                limit=max(1, max(int(max_stocks_per_run), int(dirty_limit))),
            )
            dirty_entries = dirty_entries[: max(1, int(max_stocks_per_run))]
            if not dirty_entries:
                bootstrap_cursor = _load_bootstrap_cursor(conn)
                bootstrap_keys = _list_missing_hot_cache_stock_keys(
                    conn,
                    limit=max(1, int(max_stocks_per_run)),
                    after_stock_key=bootstrap_cursor,
                )
                if not bootstrap_keys and bootstrap_cursor:
                    _save_bootstrap_cursor(conn, "")
                    bootstrap_keys = _list_missing_hot_cache_stock_keys(
                        conn,
                        limit=max(1, int(max_stocks_per_run)),
                    )
                dirty_entries = [
                    {
                        "stock_key": key,
                        "reason": "bootstrap_missing_hot",
                        "updated_at": "",
                    }
                    for key in bootstrap_keys
                ]
            _log_stock_hot_cache(
                verbose=verbose,
                message=(
                    f"queue_loaded dirty={int(len(dirty_entries))} "
                    f"bootstrap={int(len(bootstrap_keys))}"
                ),
            )
            if not dirty_entries:
                _log_stock_hot_cache(verbose=verbose, message="queue_empty skip=1")
                return {"processed": 0, "written": 0, "has_more": False}

            written = 0
            extras_written = 0
            processed_keys: list[str] = []
            for entry in dirty_entries:
                stock_key = str(entry.get("stock_key") or "").strip()
                if not stock_key:
                    continue
                reason = str(entry.get("reason") or "").strip()
                entity_key = refresh_stock_hot_for_key(
                    conn,
                    stock_key=stock_key,
                    signal_window_days=int(signal_window_days),
                    signal_cap=int(signal_cap),
                )
                did_refresh_extras = refresh_stock_extras_snapshot_for_key(
                    conn,
                    stock_key=entity_key,
                    min_refresh_seconds=int(extras_refresh_min_seconds),
                    force=_extras_force_refresh(reason),
                )
                if did_refresh_extras:
                    extras_written += 1
                remove_entity_page_dirty_keys(conn, stock_keys=[stock_key])
                if entity_key != stock_key and entity_key.startswith("stock:"):
                    remove_entity_page_dirty_keys(conn, stock_keys=[entity_key])
                if reason == "bootstrap_missing_hot":
                    _save_bootstrap_cursor(conn, stock_key)
                processed_keys.append(stock_key)
                written += 1
                _log_stock_hot_cache(
                    verbose=verbose,
                    message=(
                        f"stock_done stock_key={stock_key} "
                        f"entity_key={entity_key} "
                        f"reason={reason or '-'} "
                        f"extras_refreshed={1 if did_refresh_extras else 0}"
                    ),
                )
                try:
                    continue_now = bool(should_continue()) if should_continue else True
                except Exception:
                    continue_now = False
                if not continue_now:
                    _log_stock_hot_cache(
                        verbose=verbose,
                        message=(
                            f"yield_to_rss processed={int(len(processed_keys))} "
                            f"written={int(written)}"
                        ),
                    )
                    break

            remaining = list_entity_page_dirty_keys(conn, limit=1)
            has_more = bool(remaining)
            if (not has_more) and bootstrap_keys:
                has_more = _has_missing_hot_cache_stock_keys(
                    conn,
                    after_stock_key=bootstrap_keys[-1],
                )
            _log_stock_hot_cache(
                verbose=verbose,
                message=(
                    f"done processed={int(len(processed_keys))} "
                    f"written={int(written)} "
                    f"extras_written={int(extras_written)} "
                    f"has_more={1 if has_more else 0}"
                ),
            )
            return {
                "processed": int(len(processed_keys)),
                "written": int(written),
                "extras_written": int(extras_written),
                "has_more": bool(has_more),
            }
    finally:
        try:
            release_worker_job_lock(engine_or_conn, lock_key=STOCK_HOT_CACHE_LOCK_KEY)
        except BaseException as err:
            if isinstance(err, _FATAL_BASE_EXCEPTIONS):
                raise


__all__ = [
    "refresh_stock_extras_snapshot_for_key",
    "refresh_stock_hot_for_key",
    "sync_stock_hot_cache",
]
