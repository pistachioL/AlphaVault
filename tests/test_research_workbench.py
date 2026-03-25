from __future__ import annotations

import libsql

from alphavault.db.turso_db import TursoConnection
from alphavault.research_workbench import (
    RESEARCH_RELATION_CANDIDATES_TABLE,
    RESEARCH_RELATIONS_TABLE,
    accept_relation_candidate,
    block_relation_candidate,
    ensure_research_workbench_schema,
    ignore_relation_candidate,
    list_pending_candidates,
    record_stock_sector_relation,
    upsert_relation_candidate,
)


def test_record_stock_sector_relation_and_pending_candidates() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        record_stock_sector_relation(
            conn,
            stock_key="stock:600519.SH",
            sector_key="cluster:white_liquor",
            source="manual",
        )
        rows = (
            conn.execute(
                f"SELECT left_key, right_key, relation_label, source FROM {RESEARCH_RELATIONS_TABLE}"
            )
            .mappings()
            .all()
        )
        assert rows == [
            {
                "left_key": "stock:600519.SH",
                "right_key": "cluster:white_liquor",
                "relation_label": "member_of",
                "source": "manual",
            }
        ]
        assert list_pending_candidates(conn) == []
    finally:
        conn.close()


def test_accept_ignore_and_block_candidate_status_flow() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        upsert_relation_candidate(
            conn,
            candidate_id="cand-1",
            relation_type="stock_sector",
            left_key="stock:600519.SH",
            right_key="cluster:white_liquor",
            relation_label="member_of",
            suggestion_reason="近期高频共现",
            evidence_summary="近30天共现 12 次",
            score=0.92,
            ai_status="ranked",
        )
        assert len(list_pending_candidates(conn)) == 1

        accept_relation_candidate(conn, candidate_id="cand-1", source="manual")
        candidate_status = conn.execute(
            f"SELECT status FROM {RESEARCH_RELATION_CANDIDATES_TABLE} WHERE candidate_id = :candidate_id",
            {"candidate_id": "cand-1"},
        ).scalar()
        assert candidate_status == "accepted"

        relation_rows = (
            conn.execute(
                f"SELECT left_key, right_key FROM {RESEARCH_RELATIONS_TABLE} WHERE left_key = :left_key",
                {"left_key": "stock:600519.SH"},
            )
            .mappings()
            .all()
        )
        assert relation_rows == [
            {
                "left_key": "stock:600519.SH",
                "right_key": "cluster:white_liquor",
            }
        ]

        upsert_relation_candidate(
            conn,
            candidate_id="cand-2",
            relation_type="stock_alias",
            left_key="stock:600519.SH",
            right_key="stock:贵州茅台",
            relation_label="alias_of",
            suggestion_reason="同票别名",
            evidence_summary="同代码共现 8 次",
            score=0.88,
            ai_status="ranked",
        )
        ignore_relation_candidate(conn, candidate_id="cand-2")

        upsert_relation_candidate(
            conn,
            candidate_id="cand-3",
            relation_type="sector_sector",
            left_key="cluster:gold",
            right_key="cluster:precious_metal",
            relation_label="related_to",
            suggestion_reason="高度相关",
            evidence_summary="相关个股重合 75%",
            score=0.84,
            ai_status="ranked",
        )
        block_relation_candidate(conn, candidate_id="cand-3")

        statuses = (
            conn.execute(
                f"SELECT candidate_id, status FROM {RESEARCH_RELATION_CANDIDATES_TABLE} ORDER BY candidate_id"
            )
            .mappings()
            .all()
        )
        assert statuses == [
            {"candidate_id": "cand-1", "status": "accepted"},
            {"candidate_id": "cand-2", "status": "ignored"},
            {"candidate_id": "cand-3", "status": "blocked"},
        ]
        assert list_pending_candidates(conn) == []
    finally:
        conn.close()
