from __future__ import annotations

from contextlib import contextmanager
import libsql

import pytest

from alphavault.db.cloud_schema import (
    apply_cloud_schema as ensure_research_workbench_schema,
)
from alphavault.db.turso_db import TursoConnection
from alphavault.research_workbench import candidate_repo, relation_repo
from alphavault.research_workbench import (
    RESEARCH_RELATION_CANDIDATES_TABLE,
    RESEARCH_RELATIONS_TABLE,
    RESEARCH_SECURITY_MASTER_TABLE,
    accept_relation_candidate,
    block_relation_candidate,
    ignore_relation_candidate,
    get_stock_keys_by_official_names,
    list_pending_candidates,
    list_pending_candidates_for_left_key,
    record_stock_alias_relation,
    record_stock_sector_relation,
    upsert_security_master_stock,
    upsert_relation_candidate,
)


class _FakeRedisHashClient:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}

    def hset(
        self,
        key: str,
        field: str | None = None,
        value: str | None = None,
        mapping: dict[str, str] | None = None,
    ) -> int:
        bucket = self.hashes.setdefault(key, {})
        if mapping is not None:
            bucket.update(mapping)
            return len(mapping)
        if field is None or value is None:
            return 0
        bucket[field] = value
        return 1

    def hmget(self, key: str, fields: list[str]) -> list[str | None]:
        bucket = self.hashes.get(key, {})
        return [bucket.get(field) for field in fields]

    def hdel(self, key: str, *fields: str) -> int:
        bucket = self.hashes.get(key, {})
        deleted = 0
        for field in fields:
            if field in bucket:
                del bucket[field]
                deleted += 1
        return deleted

    def delete(self, key: str) -> int:
        if key not in self.hashes:
            return 0
        del self.hashes[key]
        return 1


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


def test_workbench_schema_uses_target_tables_without_objects() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        table_names = {
            str(row["name"])
            for row in conn.execute(
                """
SELECT name
FROM sqlite_schema
WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
"""
            )
            .mappings()
            .all()
        }

        assert {
            "alias_resolve_tasks",
            "relation_candidates",
            "relations",
            "security_master",
        }.issubset(table_names)

        record_stock_sector_relation(
            conn,
            stock_key="stock:600519.SH",
            sector_key="cluster:white_liquor",
            source="manual",
        )
        relation_rows = (
            conn.execute(f"SELECT left_key, right_key FROM {RESEARCH_RELATIONS_TABLE}")
            .mappings()
            .all()
        )
        assert relation_rows == [
            {
                "left_key": "stock:600519.SH",
                "right_key": "cluster:white_liquor",
            }
        ]
    finally:
        conn.close()


def test_record_stock_alias_relation_writes_alias_of_relation() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        record_stock_alias_relation(
            conn,
            stock_key="stock:601899.SH",
            alias_key="stock:紫金",
            source="ai_worker",
        )
        rows = (
            conn.execute(
                f"SELECT relation_type, left_key, right_key, relation_label, source FROM {RESEARCH_RELATIONS_TABLE}"
            )
            .mappings()
            .all()
        )
        assert rows == [
            {
                "relation_type": "stock_alias",
                "left_key": "stock:601899.SH",
                "right_key": "stock:紫金",
                "relation_label": "alias_of",
                "source": "ai_worker",
            }
        ]
    finally:
        conn.close()


def test_record_stock_alias_relation_refreshes_redis_shadow_dict(
    monkeypatch,
) -> None:
    from alphavault.research_workbench import relation_repo

    synced: list[tuple[str, str]] = []
    monkeypatch.setattr(
        relation_repo,
        "sync_stock_alias_shadow_dict_best_effort",
        lambda *, stock_key, alias_key: synced.append((stock_key, alias_key)),
        raising=False,
    )

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        record_stock_alias_relation(
            conn,
            stock_key="stock:601899.SH",
            alias_key="stock:紫金",
            source="manual",
        )
    finally:
        conn.close()

    assert synced == [("stock:601899.SH", "stock:紫金")]


def test_security_master_roundtrip_and_lookup_by_official_name() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        upsert_security_master_stock(
            conn,
            stock_key="stock:601899.SH",
            market="SH",
            code="601899",
            official_name="紫金矿业",
        )

        rows = (
            conn.execute(
                f"""
SELECT stock_key, market, code, official_name
FROM {RESEARCH_SECURITY_MASTER_TABLE}
ORDER BY stock_key
"""
            )
            .mappings()
            .all()
        )
        assert rows == [
            {
                "stock_key": "stock:601899.SH",
                "market": "SH",
                "code": "601899",
                "official_name": "紫金矿业",
            }
        ]
        assert get_stock_keys_by_official_names(conn, ["紫金矿业", "不存在"]) == {
            "紫金矿业": "stock:601899.SH"
        }
    finally:
        conn.close()


def test_upsert_security_master_stock_keeps_code_column_as_pure_code() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        upsert_security_master_stock(
            conn,
            stock_key="stock:SZ000725.US",
            market="SZ",
            code="SZ000725",
            official_name="京东方A",
        )

        rows = (
            conn.execute(
                f"""
SELECT stock_key, market, code, official_name
FROM {RESEARCH_SECURITY_MASTER_TABLE}
ORDER BY stock_key
"""
            )
            .mappings()
            .all()
        )
        assert rows == [
            {
                "stock_key": "stock:000725.SZ",
                "market": "SZ",
                "code": "000725",
                "official_name": "京东方A",
            }
        ]
    finally:
        conn.close()


def test_upsert_security_master_stock_refreshes_redis_name_shadow_dict(
    monkeypatch,
) -> None:
    from alphavault.research_workbench import security_master_repo

    synced: list[tuple[str, str, str]] = []
    monkeypatch.setattr(
        security_master_repo,
        "sync_stock_name_shadow_dict_best_effort",
        lambda *, stock_key, official_name, previous_official_name="": synced.append(
            (stock_key, official_name, previous_official_name)
        ),
        raising=False,
    )

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        upsert_security_master_stock(
            conn,
            stock_key="stock:601899.SH",
            market="SH",
            code="601899",
            official_name="紫金矿业",
        )
    finally:
        conn.close()

    assert synced == [("stock:601899.SH", "紫金矿业", "")]


def test_upsert_security_master_stock_replaces_old_redis_name_shadow_field(
    monkeypatch,
) -> None:
    from alphavault.infra import entity_match_redis as redis_mod

    fake_redis = _FakeRedisHashClient()
    monkeypatch.setattr(
        redis_mod,
        "try_get_redis",
        lambda: (fake_redis, "queue"),
        raising=False,
    )

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        upsert_security_master_stock(
            conn,
            stock_key="stock:601899.SH",
            market="SH",
            code="601899",
            official_name="旧名",
        )
        upsert_security_master_stock(
            conn,
            stock_key="stock:601899.SH",
            market="SH",
            code="601899",
            official_name="新名",
        )
    finally:
        conn.close()

    assert fake_redis.hashes[redis_mod.ENTITY_MATCH_STOCK_DICT_KEY] == {
        "name:新名": "stock:601899.SH"
    }


def test_bulk_upsert_security_master_stocks_writes_rows_without_per_row_redis_sync(
    monkeypatch,
) -> None:
    from alphavault.research_workbench import security_master_repo

    synced: list[tuple[str, str, str]] = []
    monkeypatch.setattr(
        security_master_repo,
        "sync_stock_name_shadow_dict_best_effort",
        lambda *, stock_key, official_name, previous_official_name="": synced.append(
            (stock_key, official_name, previous_official_name)
        ),
        raising=False,
    )

    bulk_upsert = getattr(
        security_master_repo,
        "bulk_upsert_security_master_stocks",
        None,
    )
    assert callable(bulk_upsert)

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        bulk_upsert(
            conn,
            [
                {
                    "stock_key": " stock:601899.sh ",
                    "market": "",
                    "code": " 601899 ",
                    "official_name": " 紫金矿业 ",
                },
                {
                    "stock_key": "stock:1810.hk",
                    "market": "hk",
                    "code": " 1810 ",
                    "official_name": " 小米集团-W ",
                },
            ],
        )
        rows = (
            conn.execute(
                f"""
SELECT stock_key, market, code, official_name
FROM {RESEARCH_SECURITY_MASTER_TABLE}
ORDER BY stock_key
"""
            )
            .mappings()
            .all()
        )
    finally:
        conn.close()

    assert rows == [
        {
            "stock_key": "stock:1810.HK",
            "market": "HK",
            "code": "1810",
            "official_name": "小米集团-W",
        },
        {
            "stock_key": "stock:601899.SH",
            "market": "SH",
            "code": "601899",
            "official_name": "紫金矿业",
        },
    ]
    assert synced == []


def test_rebuild_stock_dict_shadow_replaces_unique_names_and_aliases(
    monkeypatch,
) -> None:
    from alphavault.research_workbench import relation_repo
    from alphavault.research_workbench import shadow_dict_repo

    replaced: list[tuple[dict[str, str], dict[str, str]]] = []
    monkeypatch.setattr(
        relation_repo,
        "sync_stock_alias_shadow_dict_best_effort",
        lambda **_kwargs: True,
        raising=False,
    )

    def _fake_replace_stock_dict_shadow_best_effort(
        *, official_name_targets: dict[str, str], alias_targets: dict[str, str]
    ) -> bool:
        replaced.append((dict(official_name_targets), dict(alias_targets)))
        return True

    monkeypatch.setattr(
        shadow_dict_repo,
        "replace_stock_dict_shadow_best_effort",
        _fake_replace_stock_dict_shadow_best_effort,
        raising=False,
    )

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        upsert_security_master_stock(
            conn,
            stock_key="stock:600519.SH",
            market="SH",
            code="600519",
            official_name="贵州茅台",
        )
        upsert_security_master_stock(
            conn,
            stock_key="stock:600036.SH",
            market="SH",
            code="600036",
            official_name="招商银行",
        )
        upsert_security_master_stock(
            conn,
            stock_key="stock:600000.SH",
            market="SH",
            code="600000",
            official_name="重名股",
        )
        upsert_security_master_stock(
            conn,
            stock_key="stock:600001.SH",
            market="SH",
            code="600001",
            official_name="重名股",
        )

        record_stock_alias_relation(
            conn,
            stock_key="stock:600519.SH",
            alias_key="stock:茅台",
            source="manual",
        )
        record_stock_alias_relation(
            conn,
            stock_key="stock:600036.SH",
            alias_key="stock:招行",
            source="manual",
        )
        record_stock_alias_relation(
            conn,
            stock_key="stock:600519.SH",
            alias_key="stock:双关",
            source="manual",
        )
        record_stock_alias_relation(
            conn,
            stock_key="stock:600036.SH",
            alias_key="stock:双关",
            source="manual",
        )

        shadow_dict_repo.rebuild_stock_dict_shadow_best_effort(conn)
    finally:
        conn.close()

    assert replaced == [
        (
            {
                "贵州茅台": "stock:600519.SH",
                "招商银行": "stock:600036.SH",
            },
            {
                "茅台": "stock:600519.SH",
                "招行": "stock:600036.SH",
            },
        )
    ]


def test_rebuild_stock_dict_shadow_skips_normalized_field_conflicts(
    monkeypatch,
) -> None:
    from alphavault.infra import entity_match_redis as redis_mod
    from alphavault.research_workbench import shadow_dict_repo

    fake_redis = _FakeRedisHashClient()
    monkeypatch.setattr(
        redis_mod,
        "try_get_redis",
        lambda: (fake_redis, "queue"),
        raising=False,
    )

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        upsert_security_master_stock(
            conn,
            stock_key="stock:000001.SZ",
            market="SZ",
            code="000001",
            official_name="ABC",
        )
        upsert_security_master_stock(
            conn,
            stock_key="stock:000002.SZ",
            market="SZ",
            code="000002",
            official_name="abc",
        )
        upsert_security_master_stock(
            conn,
            stock_key="stock:600519.SH",
            market="SH",
            code="600519",
            official_name="贵州茅台",
        )

        record_stock_alias_relation(
            conn,
            stock_key="stock:000001.SZ",
            alias_key="stock:Alias",
            source="manual",
        )
        record_stock_alias_relation(
            conn,
            stock_key="stock:000002.SZ",
            alias_key="stock:alias",
            source="manual",
        )
        record_stock_alias_relation(
            conn,
            stock_key="stock:600519.SH",
            alias_key="stock:茅台",
            source="manual",
        )

        assert shadow_dict_repo.rebuild_stock_dict_shadow_best_effort(conn) is True
    finally:
        conn.close()

    assert fake_redis.hashes[redis_mod.ENTITY_MATCH_STOCK_DICT_KEY] == {
        "name:贵州茅台": "stock:600519.SH",
        "alias:茅台": "stock:600519.SH",
    }


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


def test_upsert_relation_candidate_does_not_reset_non_pending_status() -> None:
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
        accept_relation_candidate(conn, candidate_id="cand-1", source="manual")

        upsert_relation_candidate(
            conn,
            candidate_id="cand-1",
            relation_type="stock_sector",
            left_key="stock:600519.SH",
            right_key="cluster:white_liquor",
            relation_label="member_of",
            suggestion_reason="更新理由",
            evidence_summary="更新摘要",
            score=0.11,
            ai_status="ranked",
        )
        status = conn.execute(
            f"SELECT status FROM {RESEARCH_RELATION_CANDIDATES_TABLE} WHERE candidate_id = :candidate_id",
            {"candidate_id": "cand-1"},
        ).scalar()
        assert status == "accepted"
    finally:
        conn.close()


def test_accept_stock_alias_candidate_refreshes_redis_shadow_dict(
    monkeypatch,
) -> None:
    from alphavault.research_workbench import candidate_repo

    synced: list[tuple[str, str]] = []
    monkeypatch.setattr(
        candidate_repo,
        "sync_stock_alias_shadow_dict_best_effort",
        lambda *, stock_key, alias_key: synced.append((stock_key, alias_key)),
        raising=False,
    )

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        upsert_relation_candidate(
            conn,
            candidate_id="cand-alias-1",
            relation_type="stock_alias",
            left_key="stock:600519.SH",
            right_key="stock:茅台",
            relation_label="alias_of",
            suggestion_reason="人工确认",
            evidence_summary="同票简称",
            score=0.99,
            ai_status="skipped",
        )
        accept_relation_candidate(conn, candidate_id="cand-alias-1", source="manual")
    finally:
        conn.close()

    assert synced == [("stock:600519.SH", "stock:茅台")]


def test_accept_non_alias_candidate_does_not_refresh_redis_shadow_dict(
    monkeypatch,
) -> None:
    from alphavault.research_workbench import candidate_repo

    synced: list[tuple[str, str]] = []
    monkeypatch.setattr(
        candidate_repo,
        "sync_stock_alias_shadow_dict_best_effort",
        lambda *, stock_key, alias_key: synced.append((stock_key, alias_key)),
        raising=False,
    )

    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        upsert_relation_candidate(
            conn,
            candidate_id="cand-sector-1",
            relation_type="stock_sector",
            left_key="stock:600519.SH",
            right_key="cluster:white_liquor",
            relation_label="member_of",
            suggestion_reason="人工确认",
            evidence_summary="行业归属",
            score=0.99,
            ai_status="skipped",
        )
        accept_relation_candidate(conn, candidate_id="cand-sector-1", source="manual")
    finally:
        conn.close()

    assert synced == []


def test_list_pending_candidates_for_left_key_includes_candidate_key() -> None:
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
            suggestion_reason="共现",
            evidence_summary="共现 12 次",
            score=0.92,
            ai_status="ranked",
        )
        upsert_relation_candidate(
            conn,
            candidate_id="cand-2",
            relation_type="stock_alias",
            left_key="stock:600519.SH",
            right_key="stock:贵州茅台",
            relation_label="alias_of",
            suggestion_reason="别名",
            evidence_summary="同名共现",
            score=0.88,
            ai_status="ranked",
        )

        rows = list_pending_candidates_for_left_key(
            conn,
            left_key="stock:600519.SH",
            limit=10,
        )
        assert rows[0]["candidate_key"] == "white_liquor"
        assert rows[1]["candidate_key"] == "stock:贵州茅台"
    finally:
        conn.close()


def test_record_stock_sector_relation_uses_run_turso_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    helper_calls: list[object] = []

    def _fake_run(engine_or_conn, fn):  # type: ignore[no-untyped-def]
        del fn
        helper_calls.append(engine_or_conn)
        return None

    engine = object()
    monkeypatch.setattr(
        relation_repo, "run_turso_transaction", _fake_run, raising=False
    )

    relation_repo.record_stock_sector_relation(
        engine,  # type: ignore[arg-type]
        stock_key="stock:600519.SH",
        sector_key="cluster:white_liquor",
        source="manual",
    )
    assert helper_calls == [engine]


def test_record_stock_alias_relation_uses_run_turso_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    helper_calls: list[object] = []

    def _fake_run(engine_or_conn, fn):  # type: ignore[no-untyped-def]
        del fn
        helper_calls.append(engine_or_conn)
        return None

    monkeypatch.setattr(
        relation_repo, "run_turso_transaction", _fake_run, raising=False
    )
    monkeypatch.setattr(
        relation_repo,
        "sync_stock_alias_shadow_dict_best_effort",
        lambda **_kwargs: None,
        raising=False,
    )

    engine = object()
    relation_repo.record_stock_alias_relation(
        engine,  # type: ignore[arg-type]
        stock_key="stock:600519.SH",
        alias_key="stock:茅台",
        source="manual",
    )
    assert helper_calls == [engine]


def test_accept_relation_candidate_uses_run_turso_transaction_for_write_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    helper_calls: list[object] = []

    class _FakeMappings:
        def fetchone(self) -> dict[str, object]:
            return {
                "relation_type": "stock_alias",
                "left_key": "stock:600519.SH",
                "right_key": "stock:茅台",
                "relation_label": "alias_of",
            }

    class _FakeResult:
        def mappings(self) -> _FakeMappings:
            return _FakeMappings()

    class _FakeConn:
        def execute(self, _query, _params=None):  # type: ignore[no-untyped-def]
            return _FakeResult()

    @contextmanager
    def _fake_use_conn(_engine_or_conn):  # type: ignore[no-untyped-def]
        yield _FakeConn()

    def _fake_run(engine_or_conn, fn):  # type: ignore[no-untyped-def]
        del fn
        helper_calls.append(engine_or_conn)
        return None

    monkeypatch.setattr(candidate_repo, "use_conn", _fake_use_conn)
    monkeypatch.setattr(
        candidate_repo, "run_turso_transaction", _fake_run, raising=False
    )
    monkeypatch.setattr(
        candidate_repo,
        "sync_stock_alias_shadow_dict_best_effort",
        lambda **_kwargs: None,
        raising=False,
    )

    engine = object()
    candidate_repo.accept_relation_candidate(
        engine,  # type: ignore[arg-type]
        candidate_id="cand-1",
        source="manual",
    )
    assert helper_calls == [engine]
