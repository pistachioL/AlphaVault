from __future__ import annotations

from alphavault.db.sql.research_workbench import (
    select_candidate_by_id,
    select_candidate_status_by_ids,
    select_pending_candidates as select_pending_candidates_sql,
    select_pending_candidates_for_left_key as select_pending_candidates_for_left_key_sql,
    update_candidate_status,
    upsert_relation_candidate as upsert_relation_candidate_sql,
)
from alphavault.db.turso_db import TursoConnection, TursoEngine
from alphavault.db.turso_db import turso_savepoint
from alphavault.infra.entity_match_redis import (
    sync_stock_alias_shadow_dict_best_effort,
)
from alphavault.timeutil import now_cst_str

from .relation_repo import (
    RELATION_LABEL_ALIAS,
    RELATION_TYPE_STOCK_ALIAS,
    record_relation,
)
from .schema import RESEARCH_RELATION_CANDIDATES_TABLE, handle_turso_error, use_conn

STATUS_PENDING = "pending"
STATUS_ACCEPTED = "accepted"
STATUS_IGNORED = "ignored"
STATUS_BLOCKED = "blocked"


def _now_str() -> str:
    return now_cst_str()


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
        with use_conn(engine_or_conn) as conn:
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
        handle_turso_error(engine_or_conn, err)


def list_pending_candidates(
    engine_or_conn: TursoEngine | TursoConnection,
) -> list[dict[str, object]]:
    try:
        with use_conn(engine_or_conn) as conn:
            return (
                conn.execute(
                    select_pending_candidates_sql(RESEARCH_RELATION_CANDIDATES_TABLE)
                )
                .mappings()
                .all()
            )
    except BaseException as err:
        handle_turso_error(engine_or_conn, err)
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
        with use_conn(engine_or_conn) as conn:
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
        handle_turso_error(engine_or_conn, err)
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
    sql = select_candidate_status_by_ids(
        RESEARCH_RELATION_CANDIDATES_TABLE,
        id_count=len(cleaned),
    )
    try:
        with use_conn(engine_or_conn) as conn:
            rows = conn.execute(sql, cleaned).mappings().all()
            return {
                str(row.get("candidate_id") or "").strip(): str(
                    row.get("status") or ""
                ).strip()
                for row in rows
                if str(row.get("candidate_id") or "").strip()
            }
    except BaseException as err:
        handle_turso_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def _set_candidate_status(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    candidate_id: str,
    status: str,
) -> None:
    now = _now_str()
    try:
        with use_conn(engine_or_conn) as conn:
            conn.execute(
                update_candidate_status(RESEARCH_RELATION_CANDIDATES_TABLE),
                {
                    "candidate_id": str(candidate_id or "").strip(),
                    "status": str(status or "").strip(),
                    "now": now,
                },
            )
    except BaseException as err:
        handle_turso_error(engine_or_conn, err)


def accept_relation_candidate(
    engine_or_conn: TursoEngine | TursoConnection,
    *,
    candidate_id: str,
    source: str,
) -> None:
    candidate_key = str(candidate_id or "").strip()
    if not candidate_key:
        return
    accepted_relation_type = ""
    accepted_left_key = ""
    accepted_right_key = ""
    accepted_relation_label = ""
    try:
        with use_conn(engine_or_conn) as conn:
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
            accepted_relation_type = str(row.get("relation_type") or "").strip()
            accepted_left_key = str(row.get("left_key") or "").strip()
            accepted_right_key = str(row.get("right_key") or "").strip()
            accepted_relation_label = str(row.get("relation_label") or "").strip()
            with turso_savepoint(conn):
                conn.execute(
                    update_candidate_status(RESEARCH_RELATION_CANDIDATES_TABLE),
                    {
                        "candidate_id": candidate_key,
                        "status": STATUS_ACCEPTED,
                        "now": _now_str(),
                    },
                )
                record_relation(
                    conn,
                    relation_type=accepted_relation_type,
                    left_key=accepted_left_key,
                    right_key=accepted_right_key,
                    relation_label=accepted_relation_label,
                    source=source,
                )
    except BaseException as err:
        handle_turso_error(engine_or_conn, err)
    if (
        accepted_relation_type == RELATION_TYPE_STOCK_ALIAS
        and accepted_relation_label == RELATION_LABEL_ALIAS
    ):
        try:
            sync_stock_alias_shadow_dict_best_effort(
                stock_key=accepted_left_key,
                alias_key=accepted_right_key,
            )
        except Exception:
            pass


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
    "STATUS_ACCEPTED",
    "STATUS_BLOCKED",
    "STATUS_IGNORED",
    "STATUS_PENDING",
    "accept_relation_candidate",
    "block_relation_candidate",
    "ignore_relation_candidate",
    "list_candidate_status_map",
    "list_pending_candidates",
    "list_pending_candidates_for_left_key",
    "upsert_relation_candidate",
]
