from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any, Callable

from alphavault.constants import (
    DEFAULT_RSS_FEED_SLEEP_SECONDS,
    DEFAULT_RSS_RETRIES,
    DEFAULT_RSS_TIMEOUT_SECONDS,
    ENV_RSS_FEED_SLEEP_SECONDS,
    ENV_RSS_RETRIES,
    ENV_RSS_TIMEOUT_SECONDS,
    ENV_WORKER_ADMIN_TRIGGER_KEY,
)
from alphavault.db.postgres_db import PostgresEngine, ensure_postgres_engine
from alphavault.db.source_queue import (
    load_failed_post_queue_rows,
    load_unprocessed_post_queue_rows,
)
from alphavault.rss.utils import env_float, env_int
from alphavault.worker.cli import RSSSourceConfig, resolve_rss_source_configs
from alphavault.worker.ingest import ingest_rss_many_once
from alphavault.worker.redis_client import try_get_redis
from alphavault.worker.redis_stream_queue import (
    REDIS_PUSH_STATUS_DUPLICATE,
    REDIS_PUSH_STATUS_ERROR,
    REDIS_PUSH_STATUS_PUSHED,
    redis_ai_pressure_snapshot,
    redis_try_push_ai_message_status,
    resolve_redis_ai_queue_maxlen,
    resolve_redis_dedup_ttl_seconds,
)
from alphavault.worker.source_runtime import build_source_redis_queue_key
from alphavault.worker.spool import ensure_spool_dir

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
MANUAL_DB_REQUEUE_BATCH_SIZE = 200
MANUAL_DB_REQUEUE_MODES = {"failed", "legacy_unprocessed"}


def _resolve_rss_timeout_seconds() -> float:
    value = env_float(ENV_RSS_TIMEOUT_SECONDS)
    if value is None:
        return float(DEFAULT_RSS_TIMEOUT_SECONDS)
    return max(1.0, float(value))


def _resolve_rss_retries() -> int:
    value = env_int(ENV_RSS_RETRIES)
    if value is None:
        return int(DEFAULT_RSS_RETRIES)
    return max(0, int(value))


def _resolve_rss_feed_sleep_seconds() -> float:
    value = env_float(ENV_RSS_FEED_SLEEP_SECONDS)
    if value is None:
        return float(DEFAULT_RSS_FEED_SLEEP_SECONDS)
    return max(0.0, float(value))


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(text)
    except Exception:
        return 0


def _normalize_limit(limit: int) -> int:
    return (
        max(1, int(limit or 0)) if int(limit or 0) > 0 else MANUAL_DB_REQUEUE_BATCH_SIZE
    )


def _build_source_spool_dir(
    *, base_spool_dir: Path, source_name: str, multi_source: bool
) -> Path:
    path = base_spool_dir if not multi_source else (base_spool_dir / source_name)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _build_source_redis_queue_key(
    *, base_queue_key: str, source_name: str, multi_source: bool
) -> str:
    return build_source_redis_queue_key(
        base_queue_key=base_queue_key,
        source_name=source_name,
        multi_source=multi_source,
    )


def _maybe_dispose_source_db_engine_on_transient_error(
    *, engine: PostgresEngine, err: BaseException
) -> None:
    del engine, err


def _run_manual_ingest_for_source(
    *,
    source: RSSSourceConfig,
    base_spool_dir: Path,
    base_redis_queue_key: str,
    redis_client: Any,
    multi_source: bool,
    rss_timeout: float,
    rss_retries: int,
    rss_feed_sleep_seconds: float,
) -> dict[str, object]:
    engine = ensure_postgres_engine(source.database_url, schema_name=source.name)
    result: dict[str, object] = {
        "source": source.name,
        "platform": source.platform,
        "rss_url_count": len(source.rss_urls),
        "accepted": 0,
        "enqueue_error": False,
        "error": "",
    }
    spool_dir = _build_source_spool_dir(
        base_spool_dir=base_spool_dir,
        source_name=source.name,
        multi_source=multi_source,
    )
    redis_queue_key = _build_source_redis_queue_key(
        base_queue_key=base_redis_queue_key,
        source_name=source.name,
        multi_source=multi_source,
    )

    try:
        accepted, enqueue_error = ingest_rss_many_once(
            rss_urls=source.rss_urls,
            engine=engine,
            spool_dir=spool_dir,
            redis_client=redis_client,
            redis_queue_key=redis_queue_key,
            platform=source.platform,
            author=source.author,
            user_id=source.user_id,
            limit=None,
            rss_timeout=rss_timeout,
            rss_retries=rss_retries,
            rss_feed_sleep_seconds=rss_feed_sleep_seconds,
        )
        result["accepted"] = int(accepted)
        result["enqueue_error"] = bool(enqueue_error)
        return result
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_source_db_engine_on_transient_error(engine=engine, err=err)
        result["enqueue_error"] = True
        result["error"] = f"{type(err).__name__}: {err}"
        return result


def run_manual_rss_ingest_once() -> dict[str, object]:
    source_configs = resolve_rss_source_configs(argparse.Namespace(log_level="info"))
    multi_source = len(source_configs) > 1
    base_spool_dir = ensure_spool_dir()
    redis_client, base_redis_queue_key = try_get_redis()
    rss_timeout = _resolve_rss_timeout_seconds()
    rss_retries = _resolve_rss_retries()
    rss_feed_sleep_seconds = _resolve_rss_feed_sleep_seconds()

    total_accepted = 0
    has_enqueue_error = False
    source_results: list[dict[str, object]] = []
    for source in source_configs:
        source_result = _run_manual_ingest_for_source(
            source=source,
            base_spool_dir=base_spool_dir,
            base_redis_queue_key=base_redis_queue_key,
            redis_client=redis_client,
            multi_source=multi_source,
            rss_timeout=rss_timeout,
            rss_retries=rss_retries,
            rss_feed_sleep_seconds=rss_feed_sleep_seconds,
        )
        total_accepted += _coerce_int(source_result.get("accepted"))
        has_enqueue_error = has_enqueue_error or bool(
            source_result.get("enqueue_error")
        )
        source_results.append(source_result)

    return {
        "accepted_total": total_accepted,
        "enqueue_error": has_enqueue_error,
        "sources": source_results,
    }


def _select_manual_loader(
    mode: str,
) -> Callable[[PostgresEngine, int], list[dict[str, object]]]:
    if mode == "failed":
        return lambda engine, limit: load_failed_post_queue_rows(engine, limit=limit)
    if mode == "legacy_unprocessed":
        return lambda engine, limit: load_unprocessed_post_queue_rows(
            engine, limit=limit
        )
    raise ValueError("invalid_mode")


def _matches_platform(source: RSSSourceConfig, platform: str | None) -> bool:
    if not platform:
        return True
    return (
        str(source.platform or "").strip().lower()
        == str(platform or "").strip().lower()
    )


def _build_requeue_payload(row: dict[str, object]) -> dict[str, object]:
    return {
        "post_uid": str(row.get("post_uid") or "").strip(),
        "platform": str(row.get("platform") or "").strip(),
        "platform_post_id": str(row.get("platform_post_id") or "").strip(),
        "author": str(row.get("author") or "").strip(),
        "created_at": str(row.get("created_at") or "").strip(),
        "url": str(row.get("url") or "").strip(),
        "raw_text": str(row.get("raw_text") or ""),
        "skip_db_processed_guard": True,
    }


def _available_manual_requeue_slots(snapshot: dict[str, int]) -> int:
    queue_maxlen = max(1, int(resolve_redis_ai_queue_maxlen()))
    return max(0, queue_maxlen - int(snapshot.get("total_backlog") or 0))


def run_manual_db_requeue_once(
    *,
    mode: str,
    platform: str | None,
    limit: int,
    dry_run: bool,
) -> dict[str, object]:
    resolved_mode = str(mode or "").strip().lower()
    if resolved_mode not in MANUAL_DB_REQUEUE_MODES:
        raise ValueError("invalid_mode")

    source_configs = [
        source
        for source in resolve_rss_source_configs(argparse.Namespace(log_level="info"))
        if _matches_platform(source, platform)
    ]
    redis_client, base_redis_queue_key = try_get_redis()
    if not redis_client or not str(base_redis_queue_key or "").strip():
        raise RuntimeError("missing_redis_queue")

    multi_source = len(source_configs) > 1
    requested_limit = _normalize_limit(limit)
    remaining = requested_limit
    source_results: list[dict[str, object]] = []
    scanned_total = 0
    enqueued_total = 0
    loader = _select_manual_loader(resolved_mode)

    for source in source_configs:
        if remaining <= 0:
            break

        engine = ensure_postgres_engine(source.database_url, schema_name=source.name)
        redis_queue_key = _build_source_redis_queue_key(
            base_queue_key=base_redis_queue_key,
            source_name=source.name,
            multi_source=multi_source,
        )
        snapshot = redis_ai_pressure_snapshot(redis_client, redis_queue_key)
        source_result: dict[str, object] = {
            "source": source.name,
            "platform": source.platform,
            "mode": resolved_mode,
            "scanned": 0,
            "enqueued": 0,
            "skipped_pressure": False,
            "queue_backlog": int(snapshot.get("total_backlog") or 0),
            "error": "",
        }
        load_limit = min(int(remaining), int(MANUAL_DB_REQUEUE_BATCH_SIZE))
        if not dry_run:
            load_limit = min(load_limit, _available_manual_requeue_slots(snapshot))
        if load_limit <= 0:
            source_result["skipped_pressure"] = True
            source_results.append(source_result)
            continue

        try:
            rows = loader(engine, int(load_limit))
        except BaseException as err:
            if isinstance(err, _FATAL_BASE_EXCEPTIONS):
                raise
            source_result["error"] = f"{type(err).__name__}: {err}"
            source_results.append(source_result)
            continue

        scanned_count = len(rows)
        source_result["scanned"] = scanned_count
        scanned_total += scanned_count
        if dry_run:
            remaining -= scanned_count
            source_results.append(source_result)
            continue

        for row in rows:
            payload = _build_requeue_payload(dict(row or {}))
            post_uid = str(payload.get("post_uid") or "").strip()
            if not post_uid:
                continue
            try:
                push_status = redis_try_push_ai_message_status(
                    redis_client,
                    redis_queue_key,
                    post_uid=post_uid,
                    payload=payload,
                    ttl_seconds=resolve_redis_dedup_ttl_seconds(),
                    queue_maxlen=resolve_redis_ai_queue_maxlen(),
                )
            except BaseException as err:
                if isinstance(err, _FATAL_BASE_EXCEPTIONS):
                    raise
                source_result["error"] = f"{type(err).__name__}: {err}"
                break
            if push_status == REDIS_PUSH_STATUS_DUPLICATE:
                continue
            if push_status != REDIS_PUSH_STATUS_PUSHED:
                source_result["error"] = REDIS_PUSH_STATUS_ERROR
                break
            source_result["enqueued"] = _coerce_int(source_result.get("enqueued")) + 1
            enqueued_total += 1
            remaining -= 1
            if remaining <= 0:
                break
        source_results.append(source_result)

    return {
        "mode": resolved_mode,
        "platform": str(platform or "").strip().lower() or None,
        "limit": requested_limit,
        "dry_run": bool(dry_run),
        "scanned_total": int(scanned_total),
        "enqueued_total": int(enqueued_total),
        "sources": source_results,
    }


def load_worker_admin_trigger_key() -> str:
    return os.getenv(ENV_WORKER_ADMIN_TRIGGER_KEY, "").strip()


__all__ = [
    "load_worker_admin_trigger_key",
    "run_manual_db_requeue_once",
    "run_manual_rss_ingest_once",
]
