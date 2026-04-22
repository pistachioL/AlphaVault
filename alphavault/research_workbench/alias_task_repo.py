from __future__ import annotations

from collections.abc import Mapping
from typing import TypedDict

from alphavault.db.sql.research_workbench import (
    select_alias_resolve_task_status_summary,
    select_alias_resolve_tasks_by_status,
    select_alias_resolve_tasks_by_keys,
    upsert_alias_resolve_task_attempt,
    upsert_alias_resolve_task_status,
)
from alphavault.db.postgres_db import PostgresConnection, PostgresEngine
from alphavault.domains.stock.key_match import is_stock_code_value, normalize_stock_code
from alphavault.timeutil import now_cst_str

from .relation_repo import (
    get_confirmed_stock_alias_targets,
    record_stock_alias_relation,
)
from .security_master_repo import get_stock_keys_by_official_names
from .schema import RESEARCH_ALIAS_RESOLVE_TASKS_TABLE, handle_db_error, use_conn

ALIAS_TASK_STATUS_PENDING = "pending"
ALIAS_TASK_STATUS_MANUAL = "manual"
ALIAS_TASK_STATUS_BLOCKED = "blocked"
ALIAS_TASK_STATUS_RESOLVED = "resolved"
ALIAS_TASK_AUTO_CONFIRM_SOURCE = "ai_auto"
ALIAS_TASK_AUTO_CONFIRM_CONFIDENCE_THRESHOLD = 0.9


class AliasResolveTaskInfo(TypedDict):
    status: str
    attempt_count: int
    sample_post_uid: str
    sample_evidence: str
    sample_raw_text_excerpt: str
    ai_status: str
    ai_stock_code: str
    ai_official_name: str
    ai_confidence: str
    ai_reason: str
    ai_uncertain: str
    ai_validation_status: str


def _now_str() -> str:
    return now_cst_str()


def _coerce_count(value: object) -> int:
    raw = str(value or "").strip()
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 0


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _coerce_confidence(value: object) -> float:
    try:
        return float(_clean_text(value) or 0)
    except (TypeError, ValueError):
        return 0.0


def _normalize_bool_flag(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else ""
    raw = _clean_text(value).lower()
    return "true" if raw in {"1", "true", "yes", "y"} else ""


def _stock_key_from_ai_stock_code(value: object) -> str:
    stock_code = normalize_stock_code(str(value or ""))
    if not is_stock_code_value(stock_code):
        return ""
    return f"stock:{stock_code}"


def _empty_alias_resolve_task_status_summary() -> dict[str, int]:
    return {
        "total_count": 0,
        "pending_ai_count": 0,
        "pending_review_count": 0,
        "ai_error_count": 0,
        "resolved_count": 0,
        "blocked_count": 0,
    }


def get_alias_resolve_tasks_map(
    engine_or_conn: PostgresEngine | PostgresConnection,
    alias_keys: list[str],
) -> dict[str, AliasResolveTaskInfo]:
    cleaned = [
        str(item or "").strip() for item in alias_keys if str(item or "").strip()
    ]
    if not cleaned:
        return {}

    out: dict[str, AliasResolveTaskInfo] = {}
    chunk_size = 200
    try:
        with use_conn(engine_or_conn) as conn:
            for offset in range(0, len(cleaned), chunk_size):
                chunk = cleaned[offset : offset + chunk_size]
                sql = select_alias_resolve_tasks_by_keys(
                    RESEARCH_ALIAS_RESOLVE_TASKS_TABLE,
                    key_count=len(chunk),
                )
                rows = conn.execute(sql, chunk).mappings().all()
                for row in rows:
                    key = str(row.get("alias_key") or "").strip()
                    if not key:
                        continue
                    out[key] = AliasResolveTaskInfo(
                        status=str(row.get("status") or "").strip(),
                        attempt_count=int(row.get("attempt_count") or 0),
                        sample_post_uid=str(row.get("sample_post_uid") or "").strip(),
                        sample_evidence=str(row.get("sample_evidence") or "").strip(),
                        sample_raw_text_excerpt=str(
                            row.get("sample_raw_text_excerpt") or ""
                        ).strip(),
                        ai_status=str(row.get("ai_status") or "").strip(),
                        ai_stock_code=str(row.get("ai_stock_code") or "").strip(),
                        ai_official_name=str(row.get("ai_official_name") or "").strip(),
                        ai_confidence=str(row.get("ai_confidence") or "").strip(),
                        ai_reason=str(row.get("ai_reason") or "").strip(),
                        ai_uncertain=str(row.get("ai_uncertain") or "").strip(),
                        ai_validation_status=str(
                            row.get("ai_validation_status") or ""
                        ).strip(),
                    )
            return out
    except BaseException as err:
        handle_db_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def increment_alias_resolve_attempts(
    engine_or_conn: PostgresEngine | PostgresConnection,
    alias_keys: list[str],
) -> dict[str, int]:
    cleaned = [
        str(item or "").strip() for item in alias_keys if str(item or "").strip()
    ]
    if not cleaned:
        return {}
    now = _now_str()
    try:
        with use_conn(engine_or_conn) as conn:
            conn.execute(
                upsert_alias_resolve_task_attempt(RESEARCH_ALIAS_RESOLVE_TASKS_TABLE),
                [
                    {
                        "alias_key": key,
                        "status": ALIAS_TASK_STATUS_PENDING,
                        "now": now,
                    }
                    for key in cleaned
                ],
            )
            updated = get_alias_resolve_tasks_map(conn, cleaned)
            out: dict[str, int] = {}
            for key in cleaned:
                task = updated.get(key)
                out[key] = int(task["attempt_count"]) if task else 0
            return out
    except BaseException as err:
        handle_db_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def set_alias_resolve_task_status(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    alias_key: str,
    status: str,
    attempt_count: int = 0,
    sample_post_uid: str = "",
    sample_evidence: str = "",
    sample_raw_text_excerpt: str = "",
    ai_status: str = "",
    ai_stock_code: str = "",
    ai_official_name: str = "",
    ai_confidence: str = "",
    ai_reason: str = "",
    ai_uncertain: str = "",
    ai_validation_status: str = "",
) -> None:
    key = str(alias_key or "").strip()
    if not key:
        return
    now = _now_str()
    try:
        with use_conn(engine_or_conn) as conn:
            conn.execute(
                upsert_alias_resolve_task_status(RESEARCH_ALIAS_RESOLVE_TASKS_TABLE),
                {
                    "alias_key": key,
                    "status": str(status or "").strip(),
                    "attempt_count": int(attempt_count or 0),
                    "sample_post_uid": str(sample_post_uid or "").strip(),
                    "sample_evidence": str(sample_evidence or "").strip(),
                    "sample_raw_text_excerpt": str(
                        sample_raw_text_excerpt or ""
                    ).strip(),
                    "ai_status": str(ai_status or "").strip(),
                    "ai_stock_code": str(ai_stock_code or "").strip(),
                    "ai_official_name": str(ai_official_name or "").strip(),
                    "ai_confidence": str(ai_confidence or "").strip(),
                    "ai_reason": str(ai_reason or "").strip(),
                    "ai_uncertain": str(ai_uncertain or "").strip(),
                    "ai_validation_status": str(ai_validation_status or "").strip(),
                    "now": now,
                },
            )
    except BaseException as err:
        handle_db_error(engine_or_conn, err)


def should_auto_confirm_alias_resolve_task(
    task_row: Mapping[str, object],
) -> bool:
    if _clean_text(task_row.get("status")) != ALIAS_TASK_STATUS_PENDING:
        return False
    if _clean_text(task_row.get("ai_status")) != "ranked":
        return False
    if (
        _coerce_confidence(task_row.get("ai_confidence"))
        < ALIAS_TASK_AUTO_CONFIRM_CONFIDENCE_THRESHOLD
    ):
        return False
    if _normalize_bool_flag(task_row.get("ai_uncertain")) == "true":
        return False
    if not _stock_key_from_ai_stock_code(task_row.get("ai_stock_code")):
        return False
    if not _clean_text(task_row.get("ai_official_name")):
        return False
    if not _clean_text(task_row.get("alias_key")):
        return False
    return True


def auto_confirm_alias_resolve_task_if_needed(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    task_row: Mapping[str, object],
) -> bool:
    alias_key = _clean_text(task_row.get("alias_key"))
    stock_key = _stock_key_from_ai_stock_code(task_row.get("ai_stock_code"))
    if not alias_key or not stock_key:
        return False
    if not should_auto_confirm_alias_resolve_task(task_row):
        return False
    record_stock_alias_relation(
        engine_or_conn,
        stock_key=stock_key,
        alias_key=alias_key,
        source=ALIAS_TASK_AUTO_CONFIRM_SOURCE,
    )
    set_alias_resolve_task_status(
        engine_or_conn,
        alias_key=alias_key,
        status=ALIAS_TASK_STATUS_RESOLVED,
        attempt_count=int(_clean_text(task_row.get("attempt_count")) or 0),
        sample_post_uid=_clean_text(task_row.get("sample_post_uid")),
        sample_evidence=_clean_text(task_row.get("sample_evidence")),
        sample_raw_text_excerpt=_clean_text(task_row.get("sample_raw_text_excerpt")),
        ai_status=_clean_text(task_row.get("ai_status")),
        ai_stock_code=_clean_text(task_row.get("ai_stock_code")),
        ai_official_name=_clean_text(task_row.get("ai_official_name")),
        ai_confidence=_clean_text(task_row.get("ai_confidence")),
        ai_reason=_clean_text(task_row.get("ai_reason")),
        ai_uncertain=_clean_text(task_row.get("ai_uncertain")),
        ai_validation_status=_clean_text(task_row.get("ai_validation_status")),
    )
    return True


def cleanup_known_pending_alias_resolve_tasks(
    engine_or_conn: PostgresEngine | PostgresConnection,
    alias_keys: list[str],
) -> set[str]:
    cleaned_keys = [
        str(item or "").strip() for item in alias_keys if str(item or "").strip()
    ]
    if not cleaned_keys:
        return set()
    alias_text_by_key = {
        key: key.removeprefix("stock:").strip() for key in cleaned_keys if key
    }
    official_targets = get_stock_keys_by_official_names(
        engine_or_conn,
        list(alias_text_by_key.values()),
    )
    confirmed_targets = get_confirmed_stock_alias_targets(engine_or_conn, cleaned_keys)
    resolved_keys = {
        key
        for key, alias_text in alias_text_by_key.items()
        if alias_text in official_targets or key in confirmed_targets
    }
    for alias_key in sorted(resolved_keys):
        set_alias_resolve_task_status(
            engine_or_conn,
            alias_key=alias_key,
            status=ALIAS_TASK_STATUS_RESOLVED,
        )
    return resolved_keys


def list_manual_alias_resolve_tasks(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    limit: int | None = None,
) -> list[dict[str, object]]:
    try:
        with use_conn(engine_or_conn) as conn:
            return (
                conn.execute(
                    select_alias_resolve_tasks_by_status(
                        RESEARCH_ALIAS_RESOLVE_TASKS_TABLE,
                        limit_count=limit,
                    ),
                    {
                        "status": ALIAS_TASK_STATUS_MANUAL,
                        **({"limit": int(limit)} if limit is not None else {}),
                    },
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        handle_db_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def list_pending_alias_resolve_tasks(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    limit: int | None = None,
) -> list[dict[str, object]]:
    try:
        with use_conn(engine_or_conn) as conn:
            return (
                conn.execute(
                    select_alias_resolve_tasks_by_status(
                        RESEARCH_ALIAS_RESOLVE_TASKS_TABLE,
                        limit_count=limit,
                    ),
                    {
                        "status": ALIAS_TASK_STATUS_PENDING,
                        **({"limit": int(limit)} if limit is not None else {}),
                    },
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        handle_db_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def get_alias_resolve_task_status_summary(
    engine_or_conn: PostgresEngine | PostgresConnection,
) -> dict[str, int]:
    try:
        with use_conn(engine_or_conn) as conn:
            row = (
                conn.execute(
                    select_alias_resolve_task_status_summary(
                        RESEARCH_ALIAS_RESOLVE_TASKS_TABLE
                    )
                )
                .mappings()
                .fetchone()
            )
    except BaseException as err:
        handle_db_error(engine_or_conn, err)
    if not row:
        return _empty_alias_resolve_task_status_summary()
    return {
        "total_count": _coerce_count(row.get("total_count")),
        "pending_ai_count": _coerce_count(row.get("pending_ai_count")),
        "pending_review_count": _coerce_count(row.get("pending_review_count")),
        "ai_error_count": _coerce_count(row.get("ai_error_count")),
        "resolved_count": _coerce_count(row.get("resolved_count")),
        "blocked_count": _coerce_count(row.get("blocked_count")),
    }


__all__ = [
    "ALIAS_TASK_AUTO_CONFIRM_CONFIDENCE_THRESHOLD",
    "ALIAS_TASK_AUTO_CONFIRM_SOURCE",
    "ALIAS_TASK_STATUS_BLOCKED",
    "ALIAS_TASK_STATUS_MANUAL",
    "ALIAS_TASK_STATUS_PENDING",
    "ALIAS_TASK_STATUS_RESOLVED",
    "AliasResolveTaskInfo",
    "auto_confirm_alias_resolve_task_if_needed",
    "cleanup_known_pending_alias_resolve_tasks",
    "get_alias_resolve_task_status_summary",
    "get_alias_resolve_tasks_map",
    "increment_alias_resolve_attempts",
    "list_manual_alias_resolve_tasks",
    "list_pending_alias_resolve_tasks",
    "set_alias_resolve_task_status",
    "should_auto_confirm_alias_resolve_task",
]
