from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
import importlib
from collections.abc import Mapping
import time
from types import ModuleType
from typing import Any, Callable, TypedDict

import psycopg

from alphavault.db.postgres_db import PostgresConnection, PostgresEngine
from alphavault.domains.stock.key_match import is_stock_code_value, normalize_stock_code
from alphavault.infra.ai.runtime_config import AiRuntimeConfig
from alphavault.logging_config import get_logger
from alphavault.rss.utils import RateLimiter

from .alias_task_repo import (
    auto_confirm_alias_resolve_task_if_needed,
    list_pending_alias_resolve_tasks,
    set_alias_resolve_task_status,
    should_auto_confirm_alias_resolve_task,
)
from .security_master_repo import get_official_names_by_stock_keys

DEFAULT_ALIAS_AI_BATCH_SIZE = 10
AUTO_CONFIRM_DB_RETRY_MAX_ATTEMPTS = 3
AUTO_CONFIRM_DB_RETRY_BASE_SLEEP_SECONDS = 0.5
AI_VALIDATION_MARKETS = frozenset(("SH", "SZ", "BJ", "HK"))
logger = get_logger(__name__)


AliasAiBatchSummary = TypedDict(
    "AliasAiBatchSummary",
    {
        "fetched": int,
        "enriched": int,
        "auto_confirmed": int,
    },
)

AliasAiRoundSummary = TypedDict(
    "AliasAiRoundSummary",
    {
        "fetched": int,
        "enriched": int,
        "auto_confirmed": int,
        "remaining_sample": int,
    },
)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_positive_int(value: object, *, default: int) -> int:
    try:
        resolved = int(str(value or "").strip() or "0")
    except (TypeError, ValueError):
        resolved = 0
    if resolved > 0:
        return resolved
    return max(1, int(default))


def _load_alias_resolve_predictor_module() -> ModuleType:
    return importlib.import_module("alphavault.infra.ai.alias_resolve_predictor")


def _normalize_non_negative_float(value: object, *, default: float) -> float:
    try:
        resolved = float(str(value or "").strip() or "0")
    except (TypeError, ValueError):
        resolved = float(default)
    return max(0.0, resolved)


def _chunk_rows(
    rows: list[dict[str, Any]],
    *,
    chunk_size: int,
) -> list[list[dict[str, Any]]]:
    resolved_chunk_size = _normalize_positive_int(
        chunk_size,
        default=DEFAULT_ALIAS_AI_BATCH_SIZE,
    )
    return [
        rows[start : start + resolved_chunk_size]
        for start in range(0, len(rows), resolved_chunk_size)
    ]


def stock_key_from_ai_stock_code(value: object) -> str:
    code = normalize_stock_code(str(value or "").strip())
    if not is_stock_code_value(code):
        return ""
    return f"stock:{code}"


def stock_market_from_key(stock_key: str) -> str:
    text = _clean_text(stock_key)
    if "." not in text:
        return ""
    return _clean_text(text.rsplit(".", 1)[1]).upper()


def _runtime_ai_max_inflight(runtime_config: AiRuntimeConfig | None) -> int:
    if runtime_config is None:
        return 1
    return _normalize_positive_int(
        runtime_config.ai_max_inflight,
        default=1,
    )


def _runtime_ai_rpm(runtime_config: AiRuntimeConfig | None) -> float:
    if runtime_config is None:
        return 0.0
    return _normalize_non_negative_float(runtime_config.ai_rpm, default=0.0)


def _round_fetch_limit(
    *,
    limit: int | None,
    ai_batch_size: int,
    runtime_config: AiRuntimeConfig | None,
) -> int:
    if limit is not None:
        return _normalize_positive_int(limit, default=DEFAULT_ALIAS_AI_BATCH_SIZE)
    return max(
        1,
        _normalize_positive_int(ai_batch_size, default=DEFAULT_ALIAS_AI_BATCH_SIZE)
        * _runtime_ai_max_inflight(runtime_config),
    )


def _enrich_alias_task_rows_chunk(
    *,
    predictor_module: ModuleType,
    rows: list[dict[str, Any]],
    ai_enabled: bool,
    ai_batch_size: int,
    runtime_config: AiRuntimeConfig | None,
    request_gate: Callable[[], None] | None,
) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in predictor_module.enrich_alias_tasks_with_ai(
            [dict(row) for row in rows],
            ai_enabled=ai_enabled,
            limit=ai_batch_size,
            runtime_config=runtime_config,
            request_gate=request_gate,
        )
    ]


def enrich_alias_task_rows_with_ai(
    rows: list[dict[str, Any]] | list[Mapping[str, object]],
    *,
    predictor_module: ModuleType | None = None,
    ai_enabled: bool,
    ai_batch_size: int = DEFAULT_ALIAS_AI_BATCH_SIZE,
    runtime_config: AiRuntimeConfig | None = None,
) -> list[dict[str, Any]]:
    target_rows = [dict(row) for row in rows if _clean_text(dict(row).get("alias_key"))]
    if not target_rows:
        return []
    resolved_ai_batch_size = _normalize_positive_int(
        ai_batch_size,
        default=DEFAULT_ALIAS_AI_BATCH_SIZE,
    )
    resolved_predictor_module = (
        predictor_module or _load_alias_resolve_predictor_module()
    )
    request_gate: Callable[[], None] | None = None
    limiter = RateLimiter(_runtime_ai_rpm(runtime_config))
    if limiter.has_limit():
        request_gate = limiter.wait
    batch_rows_list = _chunk_rows(target_rows, chunk_size=resolved_ai_batch_size)
    resolved_ai_max_inflight = _runtime_ai_max_inflight(runtime_config)
    enriched_rows: list[dict[str, Any]] = []
    if resolved_ai_max_inflight <= 1:
        for chunk in batch_rows_list:
            enriched_rows.extend(
                _enrich_alias_task_rows_chunk(
                    predictor_module=resolved_predictor_module,
                    rows=chunk,
                    ai_enabled=ai_enabled,
                    ai_batch_size=resolved_ai_batch_size,
                    runtime_config=runtime_config,
                    request_gate=request_gate,
                )
            )
        return enriched_rows

    futures: list[Future[list[dict[str, Any]]]] = []
    with ThreadPoolExecutor(max_workers=resolved_ai_max_inflight) as executor:
        for chunk in batch_rows_list:
            futures.append(
                executor.submit(
                    _enrich_alias_task_rows_chunk,
                    predictor_module=resolved_predictor_module,
                    rows=chunk,
                    ai_enabled=ai_enabled,
                    ai_batch_size=resolved_ai_batch_size,
                    runtime_config=runtime_config,
                    request_gate=request_gate,
                )
            )
        for future in futures:
            enriched_rows.extend(future.result())
    return enriched_rows


def apply_alias_ai_validation(
    rows: list[dict[str, Any]] | list[Mapping[str, object]],
    *,
    official_names_by_stock_key: dict[str, str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        next_row = dict(row)
        stock_key = stock_key_from_ai_stock_code(next_row.get("ai_stock_code"))
        validation_status = ""
        if stock_key:
            market = stock_market_from_key(stock_key)
            if market in AI_VALIDATION_MARKETS:
                official_name = _clean_text(official_names_by_stock_key.get(stock_key))
                if official_name:
                    validation_status = "validated"
                    next_row["ai_official_name"] = official_name
                else:
                    validation_status = "missing_security_master"
            else:
                validation_status = "unvalidated_market"
        next_row["ai_validation_status"] = validation_status
        out.append(next_row)
    return out


def _auto_confirm_alias_with_db_retry(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    row: Mapping[str, object],
) -> bool:
    alias_key = _clean_text(row.get("alias_key"))
    max_attempts = _normalize_positive_int(
        AUTO_CONFIRM_DB_RETRY_MAX_ATTEMPTS,
        default=3,
    )
    for attempt_no in range(1, max_attempts + 1):
        try:
            return auto_confirm_alias_resolve_task_if_needed(
                engine_or_conn,
                task_row=row,
            )
        except psycopg.OperationalError as err:
            if attempt_no >= max_attempts:
                raise
            wait_seconds = float(AUTO_CONFIRM_DB_RETRY_BASE_SLEEP_SECONDS) * attempt_no
            logger.warning(
                "[alias_manual_ai] auto_confirm_db_retry alias_key=%s attempt=%s "
                "max_attempts=%s wait_seconds=%s error=%s",
                alias_key or "(empty)",
                attempt_no,
                max_attempts,
                wait_seconds,
                err,
            )
            time.sleep(wait_seconds)
    return False


def _persist_alias_ai_row(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    row: Mapping[str, object],
    apply: bool,
) -> bool:
    if not apply:
        return should_auto_confirm_alias_resolve_task(row)
    if _auto_confirm_alias_with_db_retry(engine_or_conn, row=row):
        return True
    set_alias_resolve_task_status(
        engine_or_conn,
        alias_key=_clean_text(row.get("alias_key")),
        status=_clean_text(row.get("status")) or "pending",
        attempt_count=int(_clean_text(row.get("attempt_count")) or 0),
        sample_post_uid=_clean_text(row.get("sample_post_uid")),
        sample_evidence=_clean_text(row.get("sample_evidence")),
        sample_raw_text_excerpt=_clean_text(row.get("sample_raw_text_excerpt")),
        ai_status=_clean_text(row.get("ai_status")),
        ai_stock_code=_clean_text(row.get("ai_stock_code")),
        ai_official_name=_clean_text(row.get("ai_official_name")),
        ai_confidence=_clean_text(row.get("ai_confidence")),
        ai_reason=_clean_text(row.get("ai_reason")),
        ai_uncertain=_clean_text(row.get("ai_uncertain")),
        ai_validation_status=_clean_text(row.get("ai_validation_status")),
    )
    return False


def run_alias_manual_ai_rows(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    target_rows: list[dict[str, Any]] | list[Mapping[str, object]],
    predictor_module: ModuleType | None = None,
    ai_enabled: bool,
    apply: bool,
    ai_batch_size: int = DEFAULT_ALIAS_AI_BATCH_SIZE,
    runtime_config: AiRuntimeConfig | None = None,
) -> AliasAiBatchSummary:
    cleaned_rows = [
        dict(row) for row in target_rows if _clean_text(dict(row).get("alias_key"))
    ]
    if not cleaned_rows:
        return {
            "fetched": 0,
            "enriched": 0,
            "auto_confirmed": 0,
        }
    enriched_rows = enrich_alias_task_rows_with_ai(
        cleaned_rows,
        predictor_module=predictor_module,
        ai_enabled=ai_enabled,
        ai_batch_size=ai_batch_size,
        runtime_config=runtime_config,
    )
    official_names_by_stock_key = get_official_names_by_stock_keys(
        engine_or_conn,
        [
            stock_key
            for row in enriched_rows
            if (stock_key := stock_key_from_ai_stock_code(row.get("ai_stock_code")))
            and stock_market_from_key(stock_key) in AI_VALIDATION_MARKETS
        ],
    )
    validated_rows = apply_alias_ai_validation(
        enriched_rows,
        official_names_by_stock_key=official_names_by_stock_key,
    )
    auto_confirmed = 0
    for row in validated_rows:
        if _persist_alias_ai_row(engine_or_conn, row=row, apply=apply):
            auto_confirmed += 1
    return {
        "fetched": len(cleaned_rows),
        "enriched": len(validated_rows),
        "auto_confirmed": auto_confirmed,
    }


def run_alias_manual_pending_round(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    limit: int | None = None,
    predictor_module: ModuleType | None = None,
    ai_enabled: bool,
    apply: bool,
    ai_batch_size: int = DEFAULT_ALIAS_AI_BATCH_SIZE,
    runtime_config: AiRuntimeConfig | None = None,
) -> AliasAiRoundSummary:
    resolved_fetch_limit = _round_fetch_limit(
        limit=limit,
        ai_batch_size=ai_batch_size,
        runtime_config=runtime_config,
    )
    target_rows = list_pending_alias_resolve_tasks(
        engine_or_conn,
        limit=resolved_fetch_limit,
    )
    if not target_rows:
        return {
            "fetched": 0,
            "enriched": 0,
            "auto_confirmed": 0,
            "remaining_sample": 0,
        }
    batch_summary = run_alias_manual_ai_rows(
        engine_or_conn,
        target_rows=target_rows,
        predictor_module=predictor_module,
        ai_enabled=ai_enabled,
        apply=apply,
        ai_batch_size=ai_batch_size,
        runtime_config=runtime_config,
    )
    remaining_sample = len(
        list_pending_alias_resolve_tasks(engine_or_conn, limit=resolved_fetch_limit)
    )
    return {
        **batch_summary,
        "remaining_sample": remaining_sample,
    }


__all__ = [
    "AI_VALIDATION_MARKETS",
    "AliasAiBatchSummary",
    "AliasAiRoundSummary",
    "DEFAULT_ALIAS_AI_BATCH_SIZE",
    "apply_alias_ai_validation",
    "enrich_alias_task_rows_with_ai",
    "run_alias_manual_ai_rows",
    "run_alias_manual_pending_round",
    "stock_key_from_ai_stock_code",
    "stock_market_from_key",
]
