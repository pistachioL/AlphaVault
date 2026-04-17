from __future__ import annotations

from datetime import UTC, datetime
from typing import Callable

from alphavault.logging_config import get_logger
from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    postgres_connect_autocommit,
)
from alphavault.research_stock_cache import (
    EntityPageDirtyEntry,
    claim_entity_page_dirty_entries,
    fail_entity_page_dirty_claims,
    list_entity_page_dirty_keys,
    release_entity_page_dirty_claims,
    remove_entity_page_dirty_keys,
    save_entity_page_signal_snapshot,
)
from alphavault.worker.job_state import (
    release_worker_job_lock,
    try_acquire_worker_job_lock,
)
from alphavault.worker.stock_hot_payload_builder import (
    build_stock_hot_payload,
)
from alphavault.worker.sector_hot_payload_builder import (
    build_sector_hot_payload,
)

STOCK_HOT_CACHE_LOCK_KEY = "stock_hot_cache.lock"
STOCK_HOT_CACHE_LOCK_LEASE_SECONDS = 600
STOCK_HOT_CACHE_MAX_STOCKS_PER_RUN = 4
STOCK_HOT_CACHE_DIRTY_LIMIT = 16
STOCK_HOT_CACHE_SIGNAL_WINDOW_DAYS = 30
STOCK_HOT_CACHE_SIGNAL_CAP = 500

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
logger = get_logger(__name__)


def _log_stock_hot_cache(*, level: str, message: str) -> None:
    if level == "info":
        logger.info("[stock_hot_cache] %s", message)
        return
    logger.debug("[stock_hot_cache] %s", message)


def _is_sector_entity_key(value: str) -> bool:
    return str(value or "").strip().startswith("cluster:")


def _is_stock_entity_key(value: str) -> bool:
    return str(value or "").strip().startswith("stock:")


def _release_claimed_entries(
    conn: PostgresConnection,
    *,
    dirty_entries: list[EntityPageDirtyEntry],
) -> None:
    keys_by_claim: dict[str, list[str]] = {}
    for entry in dirty_entries:
        stock_key = str(entry["stock_key"]).strip()
        claim_until = str(entry["claim_until"]).strip()
        if (not stock_key) or (not claim_until):
            continue
        keys_by_claim.setdefault(claim_until, []).append(stock_key)
    for claim_until, keys in keys_by_claim.items():
        release_entity_page_dirty_claims(
            conn,
            stock_keys=keys,
            claim_until=claim_until,
        )


def refresh_stock_hot_for_key(
    conn: PostgresConnection,
    *,
    stock_key: str,
    signal_window_days: int,
    signal_cap: int,
) -> str:
    target_key = str(stock_key or "").strip()
    if _is_sector_entity_key(target_key):
        payload = build_sector_hot_payload(
            conn,
            sector_key=target_key,
            signal_window_days=int(signal_window_days),
            signal_cap=int(signal_cap),
        )
        entity_key = str(payload.get("entity_key") or target_key).strip() or target_key
        save_entity_page_signal_snapshot(conn, stock_key=entity_key, payload=payload)
        return entity_key

    payload = build_stock_hot_payload(
        conn,
        stock_key=target_key,
        signal_window_days=int(signal_window_days),
        signal_cap=int(signal_cap),
    )
    entity_key = str(payload.get("entity_key") or target_key).strip() or target_key
    save_entity_page_signal_snapshot(conn, stock_key=entity_key, payload=payload)
    return entity_key


def sync_stock_hot_cache(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    max_stocks_per_run: int = STOCK_HOT_CACHE_MAX_STOCKS_PER_RUN,
    dirty_limit: int = STOCK_HOT_CACHE_DIRTY_LIMIT,
    signal_window_days: int = STOCK_HOT_CACHE_SIGNAL_WINDOW_DAYS,
    signal_cap: int = STOCK_HOT_CACHE_SIGNAL_CAP,
    lock_lease_seconds: int = STOCK_HOT_CACHE_LOCK_LEASE_SECONDS,
    should_continue: Callable[[], bool] | None = None,
) -> dict[str, int | bool]:
    _log_stock_hot_cache(
        level="debug",
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
        _log_stock_hot_cache(level="debug", message="lock_busy skip=1")
        return {"processed": 0, "written": 0, "has_more": False, "locked": True}
    try:
        with (
            postgres_connect_autocommit(engine_or_conn)
            if isinstance(engine_or_conn, PostgresEngine)
            else engine_or_conn
        ) as conn:
            dirty_entries: list[EntityPageDirtyEntry] = claim_entity_page_dirty_entries(
                conn,
                limit=max(1, int(max_stocks_per_run)),
                claim_ttl_seconds=int(lock_lease_seconds),
            )
            _log_stock_hot_cache(
                level="debug",
                message=(f"queue_loaded dirty={int(len(dirty_entries))} bootstrap=0"),
            )
            if not dirty_entries:
                _log_stock_hot_cache(level="debug", message="queue_empty skip=1")
                return {"processed": 0, "written": 0, "has_more": False}

            written = 0
            processed_keys: list[str] = []
            for idx, entry in enumerate(dirty_entries):
                stock_key = str(entry["stock_key"]).strip()
                if not stock_key:
                    continue
                claim_until = str(entry["claim_until"]).strip()
                reason_mask = int(entry["reason_mask"])
                if _is_stock_entity_key(stock_key):
                    remove_entity_page_dirty_keys(
                        conn,
                        stock_keys=[stock_key],
                        claim_until=claim_until,
                    )
                    processed_keys.append(stock_key)
                    _log_stock_hot_cache(
                        level="debug",
                        message=f"stock_skip stock_key={stock_key}",
                    )
                    continue
                try:
                    entity_key = refresh_stock_hot_for_key(
                        conn,
                        stock_key=stock_key,
                        signal_window_days=int(signal_window_days),
                        signal_cap=int(signal_cap),
                    )
                    remove_entity_page_dirty_keys(
                        conn,
                        stock_keys=[stock_key],
                        claim_until=claim_until,
                    )
                    if entity_key != stock_key and entity_key.startswith("stock:"):
                        remove_entity_page_dirty_keys(
                            conn,
                            stock_keys=[entity_key],
                            claim_until=claim_until,
                        )
                    processed_keys.append(stock_key)
                    written += 1
                    _log_stock_hot_cache(
                        level="debug",
                        message=(
                            f"stock_done stock_key={stock_key} "
                            f"entity_key={entity_key} "
                            f"reason_mask={int(reason_mask)}"
                        ),
                    )
                except BaseException as err:
                    if claim_until:
                        fail_entity_page_dirty_claims(
                            conn,
                            stock_keys=[stock_key],
                            claim_until=claim_until,
                        )
                        _release_claimed_entries(
                            conn,
                            dirty_entries=dirty_entries[idx + 1 :],
                        )
                    if isinstance(err, _FATAL_BASE_EXCEPTIONS):
                        raise
                    raise
                try:
                    continue_now = bool(should_continue()) if should_continue else True
                except Exception:
                    continue_now = False
                if not continue_now:
                    _release_claimed_entries(
                        conn,
                        dirty_entries=dirty_entries[idx + 1 :],
                    )
                    _log_stock_hot_cache(
                        level="debug",
                        message=(
                            f"yield_to_rss processed={int(len(processed_keys))} "
                            f"written={int(written)}"
                        ),
                    )
                    break

            remaining = list_entity_page_dirty_keys(conn, limit=1)
            has_more = bool(remaining)
            _log_stock_hot_cache(
                level="info",
                message=(
                    f"done processed={int(len(processed_keys))} "
                    f"written={int(written)} "
                    f"has_more={1 if has_more else 0}"
                ),
            )
            return {
                "processed": int(len(processed_keys)),
                "written": int(written),
                "has_more": bool(has_more),
            }
    finally:
        try:
            release_worker_job_lock(engine_or_conn, lock_key=STOCK_HOT_CACHE_LOCK_KEY)
        except BaseException as err:
            if isinstance(err, _FATAL_BASE_EXCEPTIONS):
                raise


__all__ = [
    "refresh_stock_hot_for_key",
    "sync_stock_hot_cache",
]
