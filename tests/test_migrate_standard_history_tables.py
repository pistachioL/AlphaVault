from __future__ import annotations

import importlib
from contextlib import contextmanager

import libsql

from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.turso_db import TursoConnection


def _make_conn() -> TursoConnection:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    apply_cloud_schema(conn)
    return conn


def _insert_relation(
    conn: TursoConnection,
    *,
    relation_id: str,
    relation_type: str,
    left_key: str,
    right_key: str,
    relation_label: str,
    source: str,
    created_at: str,
    updated_at: str,
) -> None:
    conn.execute(
        """
INSERT INTO relations(
    relation_id,
    relation_type,
    left_key,
    right_key,
    relation_label,
    source,
    created_at,
    updated_at
)
VALUES(
    :relation_id,
    :relation_type,
    :left_key,
    :right_key,
    :relation_label,
    :source,
    :created_at,
    :updated_at
)
""",
        {
            "relation_id": relation_id,
            "relation_type": relation_type,
            "left_key": left_key,
            "right_key": right_key,
            "relation_label": relation_label,
            "source": source,
            "created_at": created_at,
            "updated_at": updated_at,
        },
    )


def _insert_relation_candidate(
    conn: TursoConnection,
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
    status: str,
    created_at: str,
    updated_at: str,
) -> None:
    conn.execute(
        """
INSERT INTO relation_candidates(
    candidate_id,
    relation_type,
    left_key,
    right_key,
    relation_label,
    suggestion_reason,
    evidence_summary,
    score,
    ai_status,
    status,
    created_at,
    updated_at
)
VALUES(
    :candidate_id,
    :relation_type,
    :left_key,
    :right_key,
    :relation_label,
    :suggestion_reason,
    :evidence_summary,
    :score,
    :ai_status,
    :status,
    :created_at,
    :updated_at
)
""",
        {
            "candidate_id": candidate_id,
            "relation_type": relation_type,
            "left_key": left_key,
            "right_key": right_key,
            "relation_label": relation_label,
            "suggestion_reason": suggestion_reason,
            "evidence_summary": evidence_summary,
            "score": score,
            "ai_status": ai_status,
            "status": status,
            "created_at": created_at,
            "updated_at": updated_at,
        },
    )


def _insert_alias_task(
    conn: TursoConnection,
    *,
    alias_key: str,
    status: str,
    attempt_count: int,
    sample_post_uid: str,
    sample_evidence: str,
    sample_raw_text_excerpt: str,
    created_at: str,
    updated_at: str,
) -> None:
    conn.execute(
        """
INSERT INTO alias_resolve_tasks(
    alias_key,
    status,
    attempt_count,
    sample_post_uid,
    sample_evidence,
    sample_raw_text_excerpt,
    created_at,
    updated_at
)
VALUES(
    :alias_key,
    :status,
    :attempt_count,
    :sample_post_uid,
    :sample_evidence,
    :sample_raw_text_excerpt,
    :created_at,
    :updated_at
)
""",
        {
            "alias_key": alias_key,
            "status": status,
            "attempt_count": attempt_count,
            "sample_post_uid": sample_post_uid,
            "sample_evidence": sample_evidence,
            "sample_raw_text_excerpt": sample_raw_text_excerpt,
            "created_at": created_at,
            "updated_at": updated_at,
        },
    )


def test_migrate_standard_history_tables_keeps_latest_rows_and_old_fields() -> None:
    script = importlib.import_module("migrate_standard_history_tables")
    weibo_conn = _make_conn()
    xueqiu_conn = _make_conn()
    standard_conn = _make_conn()

    try:
        _insert_relation(
            weibo_conn,
            relation_id="rel:weibo",
            relation_type="stock_alias",
            left_key="stock:600000.SH",
            right_key="stock:浦发",
            relation_label="alias_of",
            source="weibo-old",
            created_at="2026-04-01 09:00:00",
            updated_at="2026-04-01 09:00:00",
        )
        _insert_relation(
            weibo_conn,
            relation_id="rel:shared",
            relation_type="stock_alias",
            left_key="stock:000001.SZ",
            right_key="stock:平安",
            relation_label="alias_of",
            source="weibo-shared",
            created_at="2026-04-01 10:00:00",
            updated_at="2026-04-02 10:00:00",
        )
        _insert_relation(
            xueqiu_conn,
            relation_id="rel:xueqiu",
            relation_type="stock_sector",
            left_key="stock:600519.SH",
            right_key="cluster:白酒",
            relation_label="member_of",
            source="xueqiu-only",
            created_at="2026-04-01 11:00:00",
            updated_at="2026-04-01 11:00:00",
        )
        _insert_relation(
            standard_conn,
            relation_id="rel:shared",
            relation_type="stock_alias",
            left_key="stock:000001.SZ",
            right_key="stock:平安银行",
            relation_label="alias_of",
            source="standard-newer",
            created_at="2026-04-01 10:00:00",
            updated_at="2026-04-06 10:00:00",
        )

        _insert_relation_candidate(
            weibo_conn,
            candidate_id="cand:weibo",
            relation_type="stock_sector",
            left_key="stock:600000.SH",
            right_key="cluster:银行",
            relation_label="member_of",
            suggestion_reason="weibo-only",
            evidence_summary="weibo-only-summary",
            score=0.41,
            ai_status="",
            status="pending",
            created_at="2026-04-01 12:00:00",
            updated_at="2026-04-01 12:00:00",
        )
        _insert_relation_candidate(
            weibo_conn,
            candidate_id="cand:shared",
            relation_type="stock_alias",
            left_key="stock:002475.SZ",
            right_key="stock:立讯",
            relation_label="alias_of",
            suggestion_reason="old-reason",
            evidence_summary="old-summary",
            score=0.25,
            ai_status="pending",
            status="pending",
            created_at="2026-04-01 13:00:00",
            updated_at="2026-04-02 13:00:00",
        )
        _insert_relation_candidate(
            xueqiu_conn,
            candidate_id="cand:shared",
            relation_type="stock_alias",
            left_key="stock:002475.SZ",
            right_key="stock:立讯精密",
            relation_label="alias_of",
            suggestion_reason="new-reason",
            evidence_summary="new-summary",
            score=0.88,
            ai_status="done",
            status="blocked",
            created_at="2026-04-01 13:00:00",
            updated_at="2026-04-05 13:00:00",
        )
        _insert_relation_candidate(
            standard_conn,
            candidate_id="cand:standard",
            relation_type="stock_alias",
            left_key="stock:300750.SZ",
            right_key="stock:宁王",
            relation_label="alias_of",
            suggestion_reason="standard-keep",
            evidence_summary="standard-summary",
            score=0.99,
            ai_status="done",
            status="accepted",
            created_at="2026-04-01 14:00:00",
            updated_at="2026-04-06 14:00:00",
        )

        _insert_alias_task(
            weibo_conn,
            alias_key="stock:长电",
            status="pending",
            attempt_count=2,
            sample_post_uid="weibo:1",
            sample_evidence="old-evidence",
            sample_raw_text_excerpt="old-excerpt",
            created_at="2026-04-01 15:00:00",
            updated_at="2026-04-02 15:00:00",
        )
        _insert_alias_task(
            xueqiu_conn,
            alias_key="stock:长电",
            status="manual",
            attempt_count=5,
            sample_post_uid="xueqiu:2",
            sample_evidence="new-evidence",
            sample_raw_text_excerpt="new-excerpt",
            created_at="2026-04-01 15:00:00",
            updated_at="2026-04-05 15:00:00",
        )
        _insert_alias_task(
            standard_conn,
            alias_key="stock:宁王",
            status="resolved",
            attempt_count=7,
            sample_post_uid="weibo:9",
            sample_evidence="keep-evidence",
            sample_raw_text_excerpt="keep-excerpt",
            created_at="2026-04-01 16:00:00",
            updated_at="2026-04-06 16:00:00",
        )

        stats = script.migrate_standard_history_tables(
            weibo_conn=weibo_conn,
            xueqiu_conn=xueqiu_conn,
            standard_conn=standard_conn,
            dry_run=False,
        )

        assert stats["relations"] == {
            "weibo_source_count": 2,
            "xueqiu_source_count": 1,
            "standard_source_count": 1,
            "merged_unique_count": 3,
            "conflict_count": 1,
            "target_before_count": 1,
            "target_count": 3,
        }
        assert stats["relation_candidates"] == {
            "weibo_source_count": 2,
            "xueqiu_source_count": 1,
            "standard_source_count": 1,
            "merged_unique_count": 3,
            "conflict_count": 1,
            "target_before_count": 1,
            "target_count": 3,
        }
        assert stats["alias_resolve_tasks"] == {
            "weibo_source_count": 1,
            "xueqiu_source_count": 1,
            "standard_source_count": 1,
            "merged_unique_count": 2,
            "conflict_count": 1,
            "target_before_count": 1,
            "target_count": 2,
        }

        relation_rows = (
            standard_conn.execute(
                """
SELECT relation_id, right_key, source, created_at, updated_at
FROM relations
ORDER BY relation_id ASC
"""
            )
            .mappings()
            .all()
        )
        assert relation_rows == [
            {
                "relation_id": "rel:shared",
                "right_key": "stock:平安银行",
                "source": "standard-newer",
                "created_at": "2026-04-01 10:00:00",
                "updated_at": "2026-04-06 10:00:00",
            },
            {
                "relation_id": "rel:weibo",
                "right_key": "stock:浦发",
                "source": "weibo-old",
                "created_at": "2026-04-01 09:00:00",
                "updated_at": "2026-04-01 09:00:00",
            },
            {
                "relation_id": "rel:xueqiu",
                "right_key": "cluster:白酒",
                "source": "xueqiu-only",
                "created_at": "2026-04-01 11:00:00",
                "updated_at": "2026-04-01 11:00:00",
            },
        ]

        candidate_rows = (
            standard_conn.execute(
                """
SELECT candidate_id, right_key, suggestion_reason, evidence_summary,
       score, ai_status, status, created_at, updated_at
FROM relation_candidates
ORDER BY candidate_id ASC
"""
            )
            .mappings()
            .all()
        )
        assert candidate_rows == [
            {
                "candidate_id": "cand:shared",
                "right_key": "stock:立讯精密",
                "suggestion_reason": "new-reason",
                "evidence_summary": "new-summary",
                "score": 0.88,
                "ai_status": "done",
                "status": "blocked",
                "created_at": "2026-04-01 13:00:00",
                "updated_at": "2026-04-05 13:00:00",
            },
            {
                "candidate_id": "cand:standard",
                "right_key": "stock:宁王",
                "suggestion_reason": "standard-keep",
                "evidence_summary": "standard-summary",
                "score": 0.99,
                "ai_status": "done",
                "status": "accepted",
                "created_at": "2026-04-01 14:00:00",
                "updated_at": "2026-04-06 14:00:00",
            },
            {
                "candidate_id": "cand:weibo",
                "right_key": "cluster:银行",
                "suggestion_reason": "weibo-only",
                "evidence_summary": "weibo-only-summary",
                "score": 0.41,
                "ai_status": "",
                "status": "pending",
                "created_at": "2026-04-01 12:00:00",
                "updated_at": "2026-04-01 12:00:00",
            },
        ]

        alias_rows = (
            standard_conn.execute(
                """
SELECT alias_key, status, attempt_count, sample_post_uid,
       sample_evidence, sample_raw_text_excerpt, created_at, updated_at
FROM alias_resolve_tasks
ORDER BY alias_key ASC
"""
            )
            .mappings()
            .all()
        )
        assert alias_rows == [
            {
                "alias_key": "stock:宁王",
                "status": "resolved",
                "attempt_count": 7,
                "sample_post_uid": "weibo:9",
                "sample_evidence": "keep-evidence",
                "sample_raw_text_excerpt": "keep-excerpt",
                "created_at": "2026-04-01 16:00:00",
                "updated_at": "2026-04-06 16:00:00",
            },
            {
                "alias_key": "stock:长电",
                "status": "manual",
                "attempt_count": 5,
                "sample_post_uid": "xueqiu:2",
                "sample_evidence": "new-evidence",
                "sample_raw_text_excerpt": "new-excerpt",
                "created_at": "2026-04-01 15:00:00",
                "updated_at": "2026-04-05 15:00:00",
            },
        ]
    finally:
        weibo_conn.close()
        xueqiu_conn.close()
        standard_conn.close()


def test_migrate_standard_history_tables_dry_run_only_prints_summary(
    capsys,
) -> None:
    script = importlib.import_module("migrate_standard_history_tables")
    weibo_conn = _make_conn()
    xueqiu_conn = _make_conn()
    standard_conn = _make_conn()

    try:
        _insert_relation(
            weibo_conn,
            relation_id="rel:1",
            relation_type="stock_alias",
            left_key="stock:600519.SH",
            right_key="stock:茅台",
            relation_label="alias_of",
            source="weibo",
            created_at="2026-04-01 09:00:00",
            updated_at="2026-04-01 09:00:00",
        )

        stats = script.migrate_standard_history_tables(
            weibo_conn=weibo_conn,
            xueqiu_conn=xueqiu_conn,
            standard_conn=standard_conn,
            dry_run=True,
        )

        out = capsys.readouterr().out
        assert "[verify] relations" in out
        assert "dry_run=1" in out
        assert stats["relations"] == {
            "weibo_source_count": 1,
            "xueqiu_source_count": 0,
            "standard_source_count": 0,
            "merged_unique_count": 1,
            "conflict_count": 0,
            "target_before_count": 0,
            "target_count": 1,
        }
        assert standard_conn.execute("SELECT COUNT(*) FROM relations").scalar() == 0
    finally:
        weibo_conn.close()
        xueqiu_conn.close()
        standard_conn.close()


def test_migrate_standard_history_tables_rolls_back_all_tables_on_midway_failure(
    monkeypatch,
) -> None:
    script = importlib.import_module("migrate_standard_history_tables")
    weibo_conn = _make_conn()
    xueqiu_conn = _make_conn()
    standard_conn = _make_conn()

    try:
        _insert_relation(
            weibo_conn,
            relation_id="rel:new",
            relation_type="stock_alias",
            left_key="stock:600036.SH",
            right_key="stock:招行",
            relation_label="alias_of",
            source="weibo-new",
            created_at="2026-04-01 09:00:00",
            updated_at="2026-04-05 09:00:00",
        )
        _insert_relation(
            standard_conn,
            relation_id="rel:old",
            relation_type="stock_alias",
            left_key="stock:600036.SH",
            right_key="stock:招行旧名",
            relation_label="alias_of",
            source="standard-old",
            created_at="2026-04-01 08:00:00",
            updated_at="2026-04-01 08:00:00",
        )
        _insert_relation_candidate(
            standard_conn,
            candidate_id="cand:old",
            relation_type="stock_alias",
            left_key="stock:300014.SZ",
            right_key="stock:亿纬旧名",
            relation_label="alias_of",
            suggestion_reason="old",
            evidence_summary="old",
            score=0.1,
            ai_status="",
            status="pending",
            created_at="2026-04-01 08:00:00",
            updated_at="2026-04-01 08:00:00",
        )
        _insert_alias_task(
            standard_conn,
            alias_key="stock:旧任务",
            status="pending",
            attempt_count=1,
            sample_post_uid="weibo:old",
            sample_evidence="old",
            sample_raw_text_excerpt="old",
            created_at="2026-04-01 08:00:00",
            updated_at="2026-04-01 08:00:00",
        )

        original_execute = standard_conn.execute

        def _fail_execute(statement, params=None):  # type: ignore[no-untyped-def]
            query = str(getattr(statement, "text", statement) or "")
            if query.strip().startswith("DELETE FROM relation_candidates"):
                raise RuntimeError("boom_on_second_table")
            return original_execute(statement, params)

        monkeypatch.setattr(standard_conn, "execute", _fail_execute)

        try:
            script.migrate_standard_history_tables(
                weibo_conn=weibo_conn,
                xueqiu_conn=xueqiu_conn,
                standard_conn=standard_conn,
                dry_run=False,
            )
        except RuntimeError as err:
            assert str(err) == "boom_on_second_table"
        else:
            raise AssertionError("expected midway failure")

        relation_rows = (
            standard_conn.execute(
                """
SELECT relation_id, right_key, source, updated_at
FROM relations
ORDER BY relation_id ASC
"""
            )
            .mappings()
            .all()
        )
        candidate_rows = (
            standard_conn.execute(
                """
SELECT candidate_id, right_key, status
FROM relation_candidates
ORDER BY candidate_id ASC
"""
            )
            .mappings()
            .all()
        )
        alias_rows = (
            standard_conn.execute(
                """
SELECT alias_key, status, attempt_count
FROM alias_resolve_tasks
ORDER BY alias_key ASC
"""
            )
            .mappings()
            .all()
        )
        assert relation_rows == [
            {
                "relation_id": "rel:old",
                "right_key": "stock:招行旧名",
                "source": "standard-old",
                "updated_at": "2026-04-01 08:00:00",
            }
        ]
        assert candidate_rows == [
            {
                "candidate_id": "cand:old",
                "right_key": "stock:亿纬旧名",
                "status": "pending",
            }
        ]
        assert alias_rows == [
            {
                "alias_key": "stock:旧任务",
                "status": "pending",
                "attempt_count": 1,
            }
        ]
    finally:
        weibo_conn.close()
        xueqiu_conn.close()
        standard_conn.close()


def test_migrate_standard_history_tables_conflict_count_counts_unique_keys(
    capsys,
) -> None:
    script = importlib.import_module("migrate_standard_history_tables")
    weibo_conn = _make_conn()
    xueqiu_conn = _make_conn()
    standard_conn = _make_conn()

    try:
        _insert_relation(
            standard_conn,
            relation_id="rel:shared",
            relation_type="stock_alias",
            left_key="stock:000001.SZ",
            right_key="stock:平安旧名",
            relation_label="alias_of",
            source="standard-old",
            created_at="2026-04-01 08:00:00",
            updated_at="2026-04-01 08:00:00",
        )
        _insert_relation(
            weibo_conn,
            relation_id="rel:shared",
            relation_type="stock_alias",
            left_key="stock:000001.SZ",
            right_key="stock:平安中间名",
            relation_label="alias_of",
            source="weibo-mid",
            created_at="2026-04-01 08:00:00",
            updated_at="2026-04-02 08:00:00",
        )
        _insert_relation(
            xueqiu_conn,
            relation_id="rel:shared",
            relation_type="stock_alias",
            left_key="stock:000001.SZ",
            right_key="stock:平安最新名",
            relation_label="alias_of",
            source="xueqiu-new",
            created_at="2026-04-01 08:00:00",
            updated_at="2026-04-03 08:00:00",
        )

        stats = script.migrate_standard_history_tables(
            weibo_conn=weibo_conn,
            xueqiu_conn=xueqiu_conn,
            standard_conn=standard_conn,
            dry_run=True,
        )

        out = capsys.readouterr().out
        assert "conflict_count=1" in out
        assert stats["relations"]["conflict_count"] == 1
        assert stats["relations"]["merged_unique_count"] == 1
    finally:
        weibo_conn.close()
        xueqiu_conn.close()
        standard_conn.close()


def test_parse_args_supports_batch_size() -> None:
    script = importlib.import_module("migrate_standard_history_tables")

    args = script.parse_args(["--dry-run", "--batch-size", "2"])

    assert args.dry_run is True
    assert args.batch_size == 2


def test_migrate_standard_history_tables_writes_in_batches_and_prints_progress(
    capsys,
) -> None:
    script = importlib.import_module("migrate_standard_history_tables")
    weibo_conn = _make_conn()
    xueqiu_conn = _make_conn()
    standard_conn = _make_conn()

    try:
        _insert_relation(
            weibo_conn,
            relation_id="rel:1",
            relation_type="stock_alias",
            left_key="stock:000001.SZ",
            right_key="stock:平安",
            relation_label="alias_of",
            source="weibo",
            created_at="2026-04-01 09:00:00",
            updated_at="2026-04-01 09:00:00",
        )
        _insert_relation(
            xueqiu_conn,
            relation_id="rel:2",
            relation_type="stock_alias",
            left_key="stock:600519.SH",
            right_key="stock:茅台",
            relation_label="alias_of",
            source="xueqiu",
            created_at="2026-04-01 10:00:00",
            updated_at="2026-04-01 10:00:00",
        )

        script.migrate_standard_history_tables(
            weibo_conn=weibo_conn,
            xueqiu_conn=xueqiu_conn,
            standard_conn=standard_conn,
            dry_run=False,
            batch_size=1,
        )

        out = capsys.readouterr().out
        assert "[write-batch] relations batch=1/2 rows=1" in out
        assert "[write-batch] relations batch=2/2 rows=1" in out
        assert "batch_size=1" in out
        assert standard_conn.execute("SELECT COUNT(*) FROM relations").scalar() == 2
    finally:
        weibo_conn.close()
        xueqiu_conn.close()
        standard_conn.close()


def test_migrate_standard_history_tables_reuses_one_connection_per_db(
    monkeypatch,
) -> None:
    script = importlib.import_module("migrate_standard_history_tables")
    weibo_conn = _make_conn()
    xueqiu_conn = _make_conn()
    standard_conn = _make_conn()

    try:
        engines = {
            "weibo-engine": weibo_conn,
            "xueqiu-engine": xueqiu_conn,
            "standard-engine": standard_conn,
        }
        open_counts = {name: 0 for name in engines}

        @contextmanager
        def _fake_connect(engine):  # type: ignore[no-untyped-def]
            name = str(engine)
            open_counts[name] += 1
            yield engines[name]

        monkeypatch.setattr(
            script,
            "turso_connect_autocommit",
            _fake_connect,
        )

        script.migrate_standard_history_tables(
            weibo_conn="weibo-engine",
            xueqiu_conn="xueqiu-engine",
            standard_conn="standard-engine",
            dry_run=True,
            batch_size=100,
        )

        assert open_counts == {
            "weibo-engine": 1,
            "xueqiu-engine": 1,
            "standard-engine": 1,
        }
    finally:
        weibo_conn.close()
        xueqiu_conn.close()
        standard_conn.close()
