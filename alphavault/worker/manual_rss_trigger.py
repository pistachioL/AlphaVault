from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

from alphavault.constants import (
    DEFAULT_RSS_RETRIES,
    DEFAULT_RSS_TIMEOUT_SECONDS,
    ENV_RSS_MANUAL_TRIGGER_KEY,
    ENV_RSS_RETRIES,
    ENV_RSS_TIMEOUT_SECONDS,
)
from alphavault.db.turso_db import (
    ensure_turso_engine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
)
from alphavault.db.turso_queue import ensure_cloud_queue_schema
from alphavault.rss.utils import env_float, env_int
from alphavault.worker.cli import RSSSourceConfig, resolve_rss_source_configs
from alphavault.worker.ingest import ingest_rss_many_once
from alphavault.worker.redis_queue import try_get_redis
from alphavault.worker.spool import ensure_spool_dir

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


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


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return 0
        try:
            return int(text)
        except Exception:
            return 0
    try:
        return int(str(value).strip())
    except Exception:
        return 0


def _build_source_spool_dir(
    *, base_spool_dir: Path, source_name: str, multi_source: bool
) -> Path:
    path = base_spool_dir if not multi_source else (base_spool_dir / source_name)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _build_source_redis_queue_key(
    *, base_queue_key: str, source_name: str, multi_source: bool
) -> str:
    if not base_queue_key:
        return ""
    if not multi_source:
        return base_queue_key
    return f"{base_queue_key}:{source_name}"


def _maybe_dispose_turso_engine_on_transient_error(
    *, engine: Engine, err: BaseException
) -> None:
    if not (is_turso_stream_not_found_error(err) or is_turso_libsql_panic_error(err)):
        return
    try:
        engine.dispose()
    except Exception:
        return


def _run_manual_ingest_for_source(
    *,
    source: RSSSourceConfig,
    base_spool_dir: Path,
    base_redis_queue_key: str,
    redis_client: Any,
    multi_source: bool,
    rss_timeout: float,
    rss_retries: int,
) -> dict[str, object]:
    engine = ensure_turso_engine(source.database_url, source.auth_token)
    result: dict[str, object] = {
        "source": source.name,
        "platform": source.platform,
        "rss_url_count": len(source.rss_urls),
        "inserted": 0,
        "turso_error": False,
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
        ensure_cloud_queue_schema(engine, verbose=False)
        inserted, turso_error = ingest_rss_many_once(
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
            verbose=False,
        )
        result["inserted"] = int(inserted)
        result["turso_error"] = bool(turso_error)
        return result
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(engine=engine, err=err)
        result["turso_error"] = True
        result["error"] = f"{type(err).__name__}: {err}"
        return result


def run_manual_rss_ingest_once() -> dict[str, object]:
    source_configs = resolve_rss_source_configs(argparse.Namespace(verbose=False))
    multi_source = len(source_configs) > 1
    base_spool_dir = ensure_spool_dir()
    redis_client, base_redis_queue_key = try_get_redis()
    rss_timeout = _resolve_rss_timeout_seconds()
    rss_retries = _resolve_rss_retries()

    total_inserted = 0
    has_turso_error = False
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
        )
        total_inserted += _coerce_int(source_result.get("inserted"))
        has_turso_error = has_turso_error or bool(source_result.get("turso_error"))
        source_results.append(source_result)

    return {
        "inserted_total": total_inserted,
        "turso_error": has_turso_error,
        "sources": source_results,
    }


def load_manual_rss_trigger_key() -> str:
    return os.getenv(ENV_RSS_MANUAL_TRIGGER_KEY, "").strip()


__all__ = ["run_manual_rss_ingest_once", "load_manual_rss_trigger_key"]
