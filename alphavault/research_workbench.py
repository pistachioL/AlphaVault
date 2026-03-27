from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import os
from typing import Iterator, TypedDict

from alphavault.constants import (
    DATETIME_FMT,
    ENV_TURSO_AUTH_TOKEN,
    ENV_TURSO_DATABASE_URL,
)
from alphavault.db.sql.research_workbench import (
    create_research_alias_resolve_tasks_index,
    create_research_alias_resolve_tasks_table,
    create_research_object_index,
    create_research_objects_table,
    create_research_relation_candidate_index,
    create_research_relation_candidate_left_key_index,
    create_research_relation_candidates_table,
    create_research_relation_index,
    create_research_relations_table,
    select_alias_resolve_tasks_by_status,
    select_candidate_by_id,
    select_pending_candidates as select_pending_candidates_sql,
    select_pending_candidates_for_left_key as select_pending_candidates_for_left_key_sql,
    upsert_alias_resolve_task_attempt,
    upsert_alias_resolve_task_status,
    update_candidate_status,
    upsert_relation_candidate as upsert_relation_candidate_sql,
    upsert_research_object,
    upsert_research_relation,
)
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    ensure_turso_engine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
    turso_connect_autocommit,
    turso_savepoint,
)
from alphavault.env import load_dotenv_if_present

RESEARCH_OBJECTS_TABLE = "research_objects"
RESEARCH_RELATIONS_TABLE = "research_relations"
RESEARCH_RELATION_CANDIDATES_TABLE = "research_relation_candidates"
RESEARCH_ALIAS_RESOLVE_TASKS_TABLE = "research_alias_resolve_tasks"

STATUS_PENDING = "pending"
STATUS_ACCEPTED = "accepted"
STATUS_IGNORED = "ignored"
STATUS_BLOCKED = "blocked"

ALIAS_TASK_STATUS_PENDING = "pending"
ALIAS_TASK_STATUS_MANUAL = "manual"
ALIAS_TASK_STATUS_BLOCKED = "blocked"
ALIAS_TASK_STATUS_RESOLVED = "resolved"

RELATION_TYPE_STOCK_SECTOR = "stock_sector"
RELATION_TYPE_STOCK_ALIAS = "stock_alias"
RELATION_LABEL_ALIAS = "alias_of"

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _now_str() -> str:
    return datetime.now().strftime(DATETIME_FMT)


class AliasResolveTaskInfo(TypedDict):
    status: str
    attempt_count: int


@contextmanager
def _use_conn(
    engine_or_conn: TursoEngine | TursoConnection,
) -> Iterator[TursoConnection]:
    if isinstance(engine_or_conn, TursoConnection):
        yield engine_or_conn
        return
    with turso_connect_autocommit(engine_or_conn) as conn:
        yield conn


def _handle_turso_error(
    engine_or_conn: TursoEngine | TursoConnection, err: BaseException
) -> None:
    if isinstance(err, _FATAL_BASE_EXCEPTIONS):
        raise err
    engine = (
        engine_or_conn._engine
        if isinstance(engine_or_conn, TursoConnection)
        else engine_or_conn
    )
    if engine is not None and (
        is_turso_stream_not_found_error(err) or is_turso_libsql_panic_error(err)
    ):
        engine.dispose()
    raise err


def _display_name_from_key(object_key: str) -> str:
    value = str(object_key or "").strip()
    if ":" not in value:
        return value
    return value.split(":", 1)[1].strip()


def _object_type_from_key(object_key: str) -> str:
    value = str(object_key or "").strip()
    if value.startswith("stock:"):
        return "stock"
    if value.startswith("cluster:"):
        return "sector"
    return "unknown"


def _relation_id(
    *,
    relation_type: str,
    left_key: str,
    right_key: str,
    relation_label: str,
) -> str:
    return "|".join(
        [
            str(relation_type or "").strip(),
            str(relation_label or "").strip(),
            str(left_key or "").strip(),
            str(right_key or "").strip(),
        ]
    )


def make_candidate_id(
    *,
    relation_type: str,
    left_key: str,
    right_key: str,
    relation_label: str,
) -> str:
    return _relation_id(
        relation_type=relation_type,
        left_key=left_key,
        right_key=right_key,
        relation_label=relation_label,
    )


def ensure_research_workbench_schema(
    engine_or_conn: TursoEngine | TursoConnection,
) -> None:
    try:
        with _use_conn(engine_or_conn) as conn:
            conn.execute(create_research_objects_table(RESEARCH_OBJECTS_TABLE))
            conn.execute(create_research_relations_table(RESEARCH_RELATIONS_TABLE))
            conn.execute(
                create_research_relation_candidates_table(
                    RESEARCH_RELATION_CANDIDATES_TABLE
                )
            )
            conn.execute(
                create_research_alias_resolve_tasks_table(
                    RESEARCH_ALIAS_RESOLVE_TASKS_TABLE
                )
            )
            conn.execute(create_research_object_index(RESEARCH_OBJECTS_TABLE))
            conn.execute(create_research_relation_index(RESEARCH_RELATIONS_TABLE))
            conn.execute(
                create_research_relation_candidate_index(
                    RESEARCH_RELATION_CANDIDATES_TABLE
                )
            )
            conn.execute(
                create_research_relation_candidate_left_key_index(
                    RESEARCH_RELATION_CANDIDATES_TABLE
                )
            )
            conn.execute(
                create_research_alias_resolve_tasks_index(
                    RESEARCH_ALIAS_RESOLVE_TASKS_TABLE
                )
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def _upsert_object(conn: TursoConnection, object_key: str) -> None:
    key = str(object_key or "").strip()
    if not key:
        return
    now = _now_str()
    conn.execute(
        upsert_research_object(RESEARCH_OBJECTS_TABLE),
        {
            "object_key": key,
            "object_type": _object_type_from_key(key),
            "display_name": _display_name_from_key(key),
            "now": now,
        },
    )


def _record_relation(
    conn: TursoConnection,
    *,
    relation_type: str,
    left_key: str,
    right_key: str,
    relation_label: str,
    source: str,
) -> None:
    now = _now_str()
    _upsert_object(conn, left_key)
    _upsert_object(conn, right_key)
    conn.execute(
        upsert_research_relation(RESEARCH_RELATIONS_TABLE),
        {
            "relation_id": _relation_id(
                relation_type=relation_type,
                left_key=left_key,
                right_key=right_key,
                relation_label=relation_label,
            ),
            "relation_type": str(relation_type or "").strip(),
            "left_key": str(left_key or "").strip(),
            "right_key": str(right_key or "").strip(),
            "relation_label": str(relation_label or "").strip(),
            "source": str(source or "").strip(),
            "now": now,
        },
    )


def record_stock_sector_relation(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    sector_key: str,
    source: str,
) -> None:
    try:
        with _use_conn(engine_or_conn) as conn:
            with turso_savepoint(conn):
                _record_relation(
                    conn,
                    relation_type=RELATION_TYPE_STOCK_SECTOR,
                    left_key=stock_key,
                    right_key=sector_key,
                    relation_label="member_of",
                    source=source,
                )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def record_stock_alias_relation(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    stock_key: str,
    alias_key: str,
    source: str,
) -> None:
    try:
        with _use_conn(engine_or_conn) as conn:
            with turso_savepoint(conn):
                _record_relation(
                    conn,
                    relation_type=RELATION_TYPE_STOCK_ALIAS,
                    left_key=stock_key,
                    right_key=alias_key,
                    relation_label=RELATION_LABEL_ALIAS,
                    source=source,
                )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def upsert_relation_candidate(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    candidate_id: str,
    relation_type: str,
    left_key: str,
    right_key: str,
    relation_label: str,
    suggestion_reason: str,
    evidence_summary: str,
    score: float,
    ai_status: str,
    status: str = STATUS_PENDING,
) -> None:
    now = _now_str()
    try:
        with _use_conn(engine_or_conn) as conn:
            conn.execute(
                upsert_relation_candidate_sql(RESEARCH_RELATION_CANDIDATES_TABLE),
                {
                    "candidate_id": str(candidate_id or "").strip(),
                    "relation_type": str(relation_type or "").strip(),
                    "left_key": str(left_key or "").strip(),
                    "right_key": str(right_key or "").strip(),
                    "relation_label": str(relation_label or "").strip(),
                    "suggestion_reason": str(suggestion_reason or "").strip(),
                    "evidence_summary": str(evidence_summary or "").strip(),
                    "score": float(score),
                    "ai_status": str(ai_status or "").strip(),
                    "status": str(status or STATUS_PENDING).strip(),
                    "now": now,
                },
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def list_pending_candidates(
    engine_or_conn: TursoEngine | TursoConnection,
) -> list[dict[str, object]]:
    try:
        with _use_conn(engine_or_conn) as conn:
            return (
                conn.execute(
                    select_pending_candidates_sql(RESEARCH_RELATION_CANDIDATES_TABLE)
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def list_pending_candidates_for_left_key(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    left_key: str,
    limit: int = 12,
) -> list[dict[str, object]]:
    key = str(left_key or "").strip()
    if not key:
        return []
    try:
        with _use_conn(engine_or_conn) as conn:
            return (
                conn.execute(
                    select_pending_candidates_for_left_key_sql(
                        RESEARCH_RELATION_CANDIDATES_TABLE
                    ),
                    {"left_key": key, "limit": max(0, int(limit))},
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def list_candidate_status_map(
    engine_or_conn: TursoEngine | TursoConnection,
    candidate_ids: list[str],
) -> dict[str, str]:
    cleaned = [
        str(item or "").strip() for item in candidate_ids if str(item or "").strip()
    ]
    if not cleaned:
        return {}
    placeholders = ", ".join(["?"] * len(cleaned))
    sql = f"""
SELECT candidate_id, status
FROM {RESEARCH_RELATION_CANDIDATES_TABLE}
WHERE candidate_id IN ({placeholders})
"""
    try:
        with _use_conn(engine_or_conn) as conn:
            rows = conn.execute(sql, cleaned).mappings().all()
            return {
                str(row.get("candidate_id") or "").strip(): str(
                    row.get("status") or ""
                ).strip()
                for row in rows
                if str(row.get("candidate_id") or "").strip()
            }
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def _set_candidate_status(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    candidate_id: str,
    status: str,
) -> None:
    now = _now_str()
    try:
        with _use_conn(engine_or_conn) as conn:
            conn.execute(
                update_candidate_status(RESEARCH_RELATION_CANDIDATES_TABLE),
                {
                    "candidate_id": str(candidate_id or "").strip(),
                    "status": str(status or "").strip(),
                    "now": now,
                },
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def accept_relation_candidate(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    candidate_id: str,
    source: str,
) -> None:
    candidate_key = str(candidate_id or "").strip()
    if not candidate_key:
        return
    try:
        with _use_conn(engine_or_conn) as conn:
            row = (
                conn.execute(
                    select_candidate_by_id(RESEARCH_RELATION_CANDIDATES_TABLE),
                    {"candidate_id": candidate_key},
                )
                .mappings()
                .fetchone()
            )
            if not row:
                return
            with turso_savepoint(conn):
                conn.execute(
                    update_candidate_status(RESEARCH_RELATION_CANDIDATES_TABLE),
                    {
                        "candidate_id": candidate_key,
                        "status": STATUS_ACCEPTED,
                        "now": _now_str(),
                    },
                )
                _record_relation(
                    conn,
                    relation_type=str(row.get("relation_type") or "").strip(),
                    left_key=str(row.get("left_key") or "").strip(),
                    right_key=str(row.get("right_key") or "").strip(),
                    relation_label=str(row.get("relation_label") or "").strip(),
                    source=source,
                )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def ignore_relation_candidate(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    candidate_id: str,
) -> None:
    _set_candidate_status(
        engine_or_conn,
        candidate_id=candidate_id,
        status=STATUS_IGNORED,
    )


def block_relation_candidate(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    candidate_id: str,
) -> None:
    _set_candidate_status(
        engine_or_conn,
        candidate_id=candidate_id,
        status=STATUS_BLOCKED,
    )


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
        with _use_conn(engine_or_conn) as conn:
            for offset in range(0, len(cleaned), chunk_size):
                chunk = cleaned[offset : offset + chunk_size]
                placeholders = ", ".join(["?"] * len(chunk))
                sql = f"""
SELECT alias_key, status, attempt_count
FROM {RESEARCH_ALIAS_RESOLVE_TASKS_TABLE}
WHERE alias_key IN ({placeholders})
"""
                rows = conn.execute(sql, chunk).mappings().all()
                for row in rows:
                    key = str(row.get("alias_key") or "").strip()
                    if not key:
                        continue
                    out[key] = AliasResolveTaskInfo(
                        status=str(row.get("status") or "").strip(),
                        attempt_count=int(row.get("attempt_count") or 0),
                    )
            return out
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
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
        with _use_conn(engine_or_conn) as conn:
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
        _handle_turso_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def set_alias_resolve_task_status(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    alias_key: str,
    status: str,
    attempt_count: int = 0,
) -> None:
    key = str(alias_key or "").strip()
    if not key:
        return
    now = _now_str()
    try:
        with _use_conn(engine_or_conn) as conn:
            conn.execute(
                upsert_alias_resolve_task_status(RESEARCH_ALIAS_RESOLVE_TASKS_TABLE),
                {
                    "alias_key": key,
                    "status": str(status or "").strip(),
                    "attempt_count": int(attempt_count or 0),
                    "now": now,
                },
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)


def list_manual_alias_resolve_tasks(
    engine_or_conn: TursoEngine | TursoConnection,
) -> list[dict[str, object]]:
    try:
        with _use_conn(engine_or_conn) as conn:
            return (
                conn.execute(
                    select_alias_resolve_tasks_by_status(
                        RESEARCH_ALIAS_RESOLVE_TASKS_TABLE
                    ),
                    {"status": ALIAS_TASK_STATUS_MANUAL},
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        _handle_turso_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def get_research_workbench_engine_from_env() -> TursoEngine:
    load_dotenv_if_present()
    db_url = os.getenv(ENV_TURSO_DATABASE_URL, "").strip()
    auth_token = os.getenv(ENV_TURSO_AUTH_TOKEN, "").strip()
    if not db_url:
        raise RuntimeError(f"Missing {ENV_TURSO_DATABASE_URL}")
    return ensure_turso_engine(db_url, auth_token)


__all__ = [
    "ALIAS_TASK_STATUS_BLOCKED",
    "ALIAS_TASK_STATUS_MANUAL",
    "ALIAS_TASK_STATUS_PENDING",
    "ALIAS_TASK_STATUS_RESOLVED",
    "RESEARCH_OBJECTS_TABLE",
    "RESEARCH_ALIAS_RESOLVE_TASKS_TABLE",
    "RESEARCH_RELATIONS_TABLE",
    "RESEARCH_RELATION_CANDIDATES_TABLE",
    "accept_relation_candidate",
    "block_relation_candidate",
    "ensure_research_workbench_schema",
    "get_alias_resolve_tasks_map",
    "get_research_workbench_engine_from_env",
    "ignore_relation_candidate",
    "increment_alias_resolve_attempts",
    "list_pending_candidates",
    "list_manual_alias_resolve_tasks",
    "list_candidate_status_map",
    "list_pending_candidates_for_left_key",
    "make_candidate_id",
    "record_stock_alias_relation",
    "record_stock_sector_relation",
    "set_alias_resolve_task_status",
    "upsert_relation_candidate",
]
