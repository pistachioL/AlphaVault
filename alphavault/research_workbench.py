from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Iterator

from alphavault.constants import DATETIME_FMT
from alphavault.db.sql.research_workbench import (
    create_research_object_index,
    create_research_objects_table,
    create_research_relation_candidate_index,
    create_research_relation_candidates_table,
    create_research_relation_index,
    create_research_relations_table,
    select_candidate_by_id,
    select_pending_candidates as select_pending_candidates_sql,
    update_candidate_status,
    upsert_relation_candidate as upsert_relation_candidate_sql,
    upsert_research_object,
    upsert_research_relation,
)
from alphavault.db.turso_db import (
    TursoConnection,
    TursoEngine,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
    turso_connect_autocommit,
    turso_savepoint,
)

RESEARCH_OBJECTS_TABLE = "research_objects"
RESEARCH_RELATIONS_TABLE = "research_relations"
RESEARCH_RELATION_CANDIDATES_TABLE = "research_relation_candidates"

STATUS_PENDING = "pending"
STATUS_ACCEPTED = "accepted"
STATUS_IGNORED = "ignored"
STATUS_BLOCKED = "blocked"

RELATION_TYPE_STOCK_SECTOR = "stock_sector"

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _now_str() -> str:
    return datetime.now().strftime(DATETIME_FMT)


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
            conn.execute(create_research_object_index(RESEARCH_OBJECTS_TABLE))
            conn.execute(create_research_relation_index(RESEARCH_RELATIONS_TABLE))
            conn.execute(
                create_research_relation_candidate_index(
                    RESEARCH_RELATION_CANDIDATES_TABLE
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


__all__ = [
    "RESEARCH_OBJECTS_TABLE",
    "RESEARCH_RELATIONS_TABLE",
    "RESEARCH_RELATION_CANDIDATES_TABLE",
    "accept_relation_candidate",
    "block_relation_candidate",
    "ensure_research_workbench_schema",
    "ignore_relation_candidate",
    "list_pending_candidates",
    "record_stock_sector_relation",
    "upsert_relation_candidate",
]
