from __future__ import annotations

from typing import TypedDict

from alphavault.db.sql.research_workbench import (
    select_alias_resolve_tasks_by_status,
    select_alias_resolve_tasks_by_keys,
    upsert_alias_resolve_task_attempt,
    upsert_alias_resolve_task_status,
)
from alphavault.db.turso_db import TursoConnection, TursoEngine
from alphavault.timeutil import now_cst_str

from .schema import RESEARCH_ALIAS_RESOLVE_TASKS_TABLE, handle_turso_error, use_conn

ALIAS_TASK_STATUS_PENDING = "pending"
ALIAS_TASK_STATUS_MANUAL = "manual"
ALIAS_TASK_STATUS_BLOCKED = "blocked"
ALIAS_TASK_STATUS_RESOLVED = "resolved"


class AliasResolveTaskInfo(TypedDict):
    status: str
    attempt_count: int
    sample_post_uid: str
    sample_evidence: str
    sample_raw_text_excerpt: str


def _now_str() -> str:
    return now_cst_str()


def get_alias_resolve_tasks_map(
    engine_or_conn: TursoEngine | TursoConnection,
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
                    )
            return out
    except BaseException as err:
        handle_turso_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def increment_alias_resolve_attempts(
    engine_or_conn: TursoEngine | TursoConnection,
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
        handle_turso_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def set_alias_resolve_task_status(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    alias_key: str,
    status: str,
    attempt_count: int = 0,
    sample_post_uid: str = "",
    sample_evidence: str = "",
    sample_raw_text_excerpt: str = "",
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
                    "now": now,
                },
            )
    except BaseException as err:
        handle_turso_error(engine_or_conn, err)


def list_manual_alias_resolve_tasks(
    engine_or_conn: TursoEngine | TursoConnection,
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
        handle_turso_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def list_pending_alias_resolve_tasks(
    engine_or_conn: TursoEngine | TursoConnection,
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
        handle_turso_error(engine_or_conn, err)
    raise AssertionError("unreachable")


__all__ = [
    "ALIAS_TASK_STATUS_BLOCKED",
    "ALIAS_TASK_STATUS_MANUAL",
    "ALIAS_TASK_STATUS_PENDING",
    "ALIAS_TASK_STATUS_RESOLVED",
    "AliasResolveTaskInfo",
    "get_alias_resolve_tasks_map",
    "increment_alias_resolve_attempts",
    "list_manual_alias_resolve_tasks",
    "list_pending_alias_resolve_tasks",
    "set_alias_resolve_task_status",
]
