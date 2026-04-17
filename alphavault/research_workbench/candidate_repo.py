from __future__ import annotations

from collections.abc import Mapping

from alphavault.db.sql.research_workbench import (
    select_candidate_by_id,
    select_candidate_status_by_ids,
    select_pending_candidates as select_pending_candidates_sql,
    select_pending_candidates_for_left_key as select_pending_candidates_for_left_key_sql,
    update_candidate_status,
    upsert_relation_candidate as upsert_relation_candidate_sql,
)
from alphavault.db.postgres_db import (
    PostgresConnection,
    PostgresEngine,
    run_postgres_transaction,
)
from alphavault.infra.entity_match_redis import (
    sync_stock_alias_shadow_dict_best_effort,
)
from alphavault.timeutil import now_cst_str
from alphavault.domains.relation.ids import make_candidate_id
from alphavault.domains.stock.keys import normalize_stock_key

from .relation_repo import (
    RELATION_LABEL_ALIAS,
    RELATION_TYPE_STOCK_ALIAS,
    record_relation,
)
from .schema import RESEARCH_RELATION_CANDIDATES_TABLE, handle_db_error, use_conn

STATUS_PENDING = "pending"
STATUS_ACCEPTED = "accepted"
STATUS_IGNORED = "ignored"
STATUS_BLOCKED = "blocked"
AUTO_ACCEPT_SOURCE = "ai_auto"
AUTO_ACCEPT_CONFIDENCE_THRESHOLD = 0.9


def _now_str() -> str:
    return now_cst_str()


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _coerce_confidence(value: object) -> float:
    try:
        return float(_clean_text(value) or 0)
    except (TypeError, ValueError):
        return 0.0


def _normalize_relation_key(value: object) -> str:
    key = _clean_text(value)
    if not key.startswith("stock:"):
        return key
    normalized = normalize_stock_key(key)
    return normalized or key


def _build_normalized_candidate_payload(
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
    ai_reason: str,
    ai_confidence: str,
    sample_post_uid: str,
    sample_evidence: str,
    sample_raw_text_excerpt: str,
    status: str,
    now: str,
) -> dict[str, object]:
    resolved_relation_type = _clean_text(relation_type)
    resolved_left_key = _normalize_relation_key(left_key)
    resolved_right_key = _normalize_relation_key(right_key)
    resolved_relation_label = _clean_text(relation_label)
    resolved_candidate_id = make_candidate_id(
        relation_type=resolved_relation_type,
        left_key=resolved_left_key,
        right_key=resolved_right_key,
        relation_label=resolved_relation_label,
    )
    return {
        "raw_candidate_id": _clean_text(candidate_id),
        "candidate_id": resolved_candidate_id,
        "relation_type": resolved_relation_type,
        "left_key": resolved_left_key,
        "right_key": resolved_right_key,
        "relation_label": resolved_relation_label,
        "suggestion_reason": _clean_text(suggestion_reason),
        "evidence_summary": _clean_text(evidence_summary),
        "score": float(score),
        "ai_status": _clean_text(ai_status),
        "ai_reason": _clean_text(ai_reason),
        "ai_confidence": _clean_text(ai_confidence),
        "sample_post_uid": _clean_text(sample_post_uid),
        "sample_evidence": _clean_text(sample_evidence),
        "sample_raw_text_excerpt": _clean_text(sample_raw_text_excerpt),
        "status": _clean_text(status) or STATUS_PENDING,
        "now": now,
    }


def should_auto_accept_relation_candidate_row(
    candidate_row: Mapping[str, object],
) -> bool:
    if _clean_text(candidate_row.get("relation_type")) != RELATION_TYPE_STOCK_ALIAS:
        return False
    if _clean_text(candidate_row.get("relation_label")) != RELATION_LABEL_ALIAS:
        return False
    if _clean_text(candidate_row.get("ai_status")) != "merge":
        return False
    return (
        _coerce_confidence(candidate_row.get("ai_confidence"))
        >= AUTO_ACCEPT_CONFIDENCE_THRESHOLD
    )


def auto_accept_relation_candidate_if_needed(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    candidate_row: Mapping[str, object],
) -> bool:
    candidate_id = _clean_text(candidate_row.get("candidate_id"))
    if not candidate_id:
        return False
    if not should_auto_accept_relation_candidate_row(candidate_row):
        return False
    accept_relation_candidate(
        engine_or_conn,
        candidate_id=candidate_id,
        source=AUTO_ACCEPT_SOURCE,
    )
    return True


def upsert_relation_candidate(
    engine_or_conn: PostgresEngine | PostgresConnection,
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
    ai_reason: str = "",
    ai_confidence: str = "",
    sample_post_uid: str = "",
    sample_evidence: str = "",
    sample_raw_text_excerpt: str = "",
    status: str = STATUS_PENDING,
) -> dict[str, object]:
    now = _now_str()
    payload = _build_normalized_candidate_payload(
        candidate_id=candidate_id,
        relation_type=relation_type,
        left_key=left_key,
        right_key=right_key,
        relation_label=relation_label,
        suggestion_reason=suggestion_reason,
        evidence_summary=evidence_summary,
        score=score,
        ai_status=ai_status,
        ai_reason=ai_reason,
        ai_confidence=ai_confidence,
        sample_post_uid=sample_post_uid,
        sample_evidence=sample_evidence,
        sample_raw_text_excerpt=sample_raw_text_excerpt,
        status=status,
        now=now,
    )
    try:
        with use_conn(engine_or_conn) as conn:
            db_payload = dict(payload)
            db_payload.pop("raw_candidate_id", None)
            conn.execute(
                upsert_relation_candidate_sql(RESEARCH_RELATION_CANDIDATES_TABLE),
                db_payload,
            )
            raw_candidate_id = str(payload.get("raw_candidate_id") or "").strip()
            resolved_candidate_id = str(payload.get("candidate_id") or "").strip()
            if raw_candidate_id and raw_candidate_id != resolved_candidate_id:
                conn.execute(
                    f"DELETE FROM {RESEARCH_RELATION_CANDIDATES_TABLE} WHERE candidate_id = :candidate_id",
                    {"candidate_id": raw_candidate_id},
                )
    except BaseException as err:
        handle_db_error(engine_or_conn, err)
    return payload


def list_pending_candidates(
    engine_or_conn: PostgresEngine | PostgresConnection,
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
        handle_db_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def list_pending_candidates_for_left_key(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    left_key: str,
    limit: int = 12,
) -> list[dict[str, object]]:
    key = _normalize_relation_key(left_key)
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
        handle_db_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def list_candidate_status_map(
    engine_or_conn: PostgresEngine | PostgresConnection,
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
        handle_db_error(engine_or_conn, err)
    raise AssertionError("unreachable")


def _set_candidate_status(
    engine_or_conn: PostgresEngine | PostgresConnection,
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
        handle_db_error(engine_or_conn, err)


def accept_relation_candidate(
    engine_or_conn: PostgresEngine | PostgresConnection,
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
            accepted_left_key = _normalize_relation_key(row.get("left_key"))
            accepted_right_key = _normalize_relation_key(row.get("right_key"))
            accepted_relation_label = str(row.get("relation_label") or "").strip()

            def _accept(tx_conn: PostgresConnection) -> None:
                tx_conn.execute(
                    update_candidate_status(RESEARCH_RELATION_CANDIDATES_TABLE),
                    {
                        "candidate_id": candidate_key,
                        "status": STATUS_ACCEPTED,
                        "now": _now_str(),
                    },
                )
                record_relation(
                    tx_conn,
                    relation_type=accepted_relation_type,
                    left_key=accepted_left_key,
                    right_key=accepted_right_key,
                    relation_label=accepted_relation_label,
                    source=source,
                )

            run_postgres_transaction(engine_or_conn, _accept)
    except BaseException as err:
        handle_db_error(engine_or_conn, err)
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
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    candidate_id: str,
) -> None:
    _set_candidate_status(
        engine_or_conn,
        candidate_id=candidate_id,
        status=STATUS_IGNORED,
    )


def block_relation_candidate(
    engine_or_conn: PostgresEngine | PostgresConnection,
    *,
    candidate_id: str,
) -> None:
    _set_candidate_status(
        engine_or_conn,
        candidate_id=candidate_id,
        status=STATUS_BLOCKED,
    )


__all__ = [
    "AUTO_ACCEPT_CONFIDENCE_THRESHOLD",
    "AUTO_ACCEPT_SOURCE",
    "STATUS_ACCEPTED",
    "STATUS_BLOCKED",
    "STATUS_IGNORED",
    "STATUS_PENDING",
    "accept_relation_candidate",
    "auto_accept_relation_candidate_if_needed",
    "block_relation_candidate",
    "ignore_relation_candidate",
    "list_candidate_status_map",
    "list_pending_candidates",
    "list_pending_candidates_for_left_key",
    "should_auto_accept_relation_candidate_row",
    "upsert_relation_candidate",
]
