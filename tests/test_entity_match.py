from __future__ import annotations

import libsql

from alphavault.db.cloud_schema import (
    apply_cloud_schema as ensure_research_workbench_schema,
)
from alphavault.db.turso_db import TursoConnection
from alphavault.domains.entity_match.resolve import (
    load_entity_match_lookup_maps,
    persist_entity_match_followups,
    resolve_assertion_mentions,
)
from alphavault.research_workbench import (
    RESEARCH_ALIAS_RESOLVE_TASKS_TABLE,
    RESEARCH_RELATION_CANDIDATES_TABLE,
    record_stock_alias_relation,
    upsert_security_master_stock,
)


def test_resolve_assertion_mentions_creates_candidate_for_unconfirmed_alias() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)

        result = resolve_assertion_mentions(
            conn,
            assertion_mentions=[
                {
                    "mention_text": "600519",
                    "mention_type": "stock_code",
                    "confidence": 0.95,
                },
                {
                    "mention_text": "茅台",
                    "mention_type": "stock_alias",
                    "confidence": 0.9,
                },
            ],
        )

        assert result.entities == [
            {
                "entity_key": "stock:600519.SH",
                "entity_type": "stock",
                "match_source": "stock_code",
                "is_primary": 1,
            }
        ]
        assert len(result.relation_candidates) == 1
        candidate = result.relation_candidates[0]
        assert candidate["left_key"] == "stock:600519.SH"
        assert candidate["right_key"] == "stock:茅台"
        assert candidate["relation_type"] == "stock_alias"
        assert result.alias_task_keys == []
    finally:
        conn.close()


def test_resolve_assertion_mentions_normalizes_prefixed_cn_stock_code() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)

        result = resolve_assertion_mentions(
            conn,
            assertion_mentions=[
                {
                    "mention_text": "SZ000725",
                    "mention_type": "stock_code",
                    "confidence": 0.95,
                }
            ],
        )

        assert result.entities == [
            {
                "entity_key": "stock:000725.SZ",
                "entity_type": "stock",
                "match_source": "stock_code",
                "is_primary": 1,
            }
        ]
        assert result.relation_candidates == []
        assert result.alias_task_keys == []
    finally:
        conn.close()


def test_resolve_assertion_mentions_uses_confirmed_alias_relation() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        record_stock_alias_relation(
            conn,
            stock_key="stock:601899.SH",
            alias_key="stock:紫金",
            source="manual",
        )

        result = resolve_assertion_mentions(
            conn,
            assertion_mentions=[
                {
                    "mention_text": "紫金",
                    "mention_type": "stock_alias",
                    "confidence": 0.88,
                }
            ],
        )

        assert result.entities == [
            {
                "entity_key": "stock:601899.SH",
                "entity_type": "stock",
                "match_source": "stock_alias",
                "is_primary": 1,
            }
        ]
        assert result.relation_candidates == []
        assert result.alias_task_keys == []
    finally:
        conn.close()


def test_resolve_assertion_mentions_uses_security_master_official_name() -> None:
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

        result = resolve_assertion_mentions(
            conn,
            assertion_mentions=[
                {
                    "mention_text": "紫金矿业",
                    "mention_type": "stock_name",
                    "confidence": 0.91,
                }
            ],
        )

        assert result.entities == [
            {
                "entity_key": "stock:601899.SH",
                "entity_type": "stock",
                "match_source": "stock_name",
                "is_primary": 1,
            }
        ]
        assert result.relation_candidates == []
        assert result.alias_task_keys == []
    finally:
        conn.close()


def test_resolve_assertion_mentions_allows_stock_name_to_fallback_to_confirmed_alias() -> (
    None
):
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        record_stock_alias_relation(
            conn,
            stock_key="stock:600519.SH",
            alias_key="stock:茅台",
            source="manual",
        )

        result = resolve_assertion_mentions(
            conn,
            assertion_mentions=[
                {
                    "mention_text": "茅台",
                    "mention_type": "stock_name",
                    "confidence": 0.9,
                }
            ],
        )

        assert result.entities == [
            {
                "entity_key": "stock:600519.SH",
                "entity_type": "stock",
                "match_source": "stock_name",
                "is_primary": 1,
            }
        ]
        assert result.relation_candidates == []
        assert result.alias_task_keys == []
    finally:
        conn.close()


def test_resolve_assertion_mentions_does_not_use_historical_stock_name_mapping() -> (
    None
):
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)
        conn.execute(
            """
INSERT INTO assertion_mentions(
  assertion_id, mention_seq, mention_text, mention_norm, mention_type, evidence, confidence
)
VALUES (?, ?, ?, ?, ?, ?, ?)
""",
            ("weibo:1#1", 1, "紫金矿业", "紫金矿业", "stock_name", "原文", 0.95),
        )
        conn.execute(
            """
INSERT INTO assertion_entities(
  assertion_id, entity_key, entity_type, match_source, is_primary
)
VALUES (?, ?, ?, ?, ?)
""",
            (
                "weibo:1#1",
                "stock:601899.SH",
                "stock",
                "stock_name",
                1,
            ),
        )

        result = resolve_assertion_mentions(
            conn,
            assertion_mentions=[
                {
                    "mention_text": "紫金矿业",
                    "mention_type": "stock_name",
                    "confidence": 0.91,
                }
            ],
        )

        assert result.entities == []
        assert result.relation_candidates == []
        assert result.alias_task_keys == []
    finally:
        conn.close()


def test_load_entity_match_lookup_maps_uses_one_redis_batch_and_batch_fallback(
    monkeypatch,
) -> None:
    from alphavault.domains.entity_match import resolve as resolve_module

    redis_calls: list[tuple[list[str], list[str]]] = []
    name_fallback_calls: list[list[str]] = []
    alias_fallback_calls: list[list[str]] = []

    def _fake_load_stock_dict_targets_best_effort(
        *, official_names: list[str], alias_texts: list[str]
    ) -> tuple[dict[str, str], dict[str, str]]:
        redis_calls.append((list(official_names), list(alias_texts)))
        return (
            {"贵州茅台": "stock:600519.SH"},
            {"茅台": "stock:600519.SH"},
        )

    def _fake_get_stock_keys_by_official_names(
        _engine_or_conn, official_names: list[str]
    ) -> dict[str, str]:
        name_fallback_calls.append(list(official_names))
        return {"腾讯控股": "stock:00700.HK"}

    def _fake_load_confirmed_alias_targets(
        _engine_or_conn, *, mention_texts: list[str]
    ) -> dict[str, str]:
        alias_fallback_calls.append(list(mention_texts))
        return {"招行": "stock:600036.SH"}

    monkeypatch.setattr(
        resolve_module,
        "load_stock_dict_targets_best_effort",
        _fake_load_stock_dict_targets_best_effort,
    )
    monkeypatch.setattr(
        resolve_module,
        "get_stock_keys_by_official_names",
        _fake_get_stock_keys_by_official_names,
    )
    monkeypatch.setattr(
        resolve_module,
        "_load_confirmed_alias_targets",
        _fake_load_confirmed_alias_targets,
    )
    monkeypatch.setattr(
        resolve_module,
        "_should_load_redis_shadow",
        lambda _engine_or_conn: True,
    )

    stock_name_targets, stock_alias_targets = load_entity_match_lookup_maps(
        object(),
        stock_name_texts=["贵州茅台", "腾讯控股", "贵州茅台"],
        stock_alias_texts=["茅台", "招行", "茅台"],
    )

    assert redis_calls == [
        (
            ["贵州茅台", "腾讯控股"],
            ["贵州茅台", "腾讯控股", "茅台", "招行"],
        )
    ]
    assert name_fallback_calls == [["腾讯控股"]]
    assert alias_fallback_calls == [["招行"]]
    assert stock_name_targets == {
        "贵州茅台": "stock:600519.SH",
        "腾讯控股": "stock:00700.HK",
    }
    assert stock_alias_targets == {
        "茅台": "stock:600519.SH",
        "招行": "stock:600036.SH",
    }


def test_persist_entity_match_followups_writes_candidate_and_alias_task() -> None:
    conn = TursoConnection(libsql.connect(":memory:", isolation_level=None))
    try:
        ensure_research_workbench_schema(conn)

        candidate_result = resolve_assertion_mentions(
            conn,
            assertion_mentions=[
                {
                    "mention_text": "600519",
                    "mention_type": "stock_code",
                    "confidence": 0.95,
                },
                {
                    "mention_text": "茅台",
                    "mention_type": "stock_alias",
                    "confidence": 0.9,
                },
            ],
        )
        task_result = resolve_assertion_mentions(
            conn,
            assertion_mentions=[
                {
                    "mention_text": "长电",
                    "mention_type": "stock_alias",
                    "confidence": 0.8,
                }
            ],
            alias_task_sample={
                "sample_post_uid": "weibo:1",
                "sample_evidence": "长电今天继续涨",
                "sample_raw_text_excerpt": "原文里提到长电、封测和景气度。",
            },
        )
        persist_entity_match_followups(conn, candidate_result)
        persist_entity_match_followups(conn, task_result)

        candidate_rows = (
            conn.execute(
                f"""
SELECT relation_type, left_key, right_key, relation_label, ai_status, status
FROM {RESEARCH_RELATION_CANDIDATES_TABLE}
ORDER BY candidate_id
"""
            )
            .mappings()
            .all()
        )
        assert candidate_rows == [
            {
                "relation_type": "stock_alias",
                "left_key": "stock:600519.SH",
                "right_key": "stock:茅台",
                "relation_label": "alias_of",
                "ai_status": "skipped",
                "status": "pending",
            }
        ]

        alias_rows = (
            conn.execute(
                f"""
SELECT alias_key, status, sample_post_uid, sample_evidence, sample_raw_text_excerpt
FROM {RESEARCH_ALIAS_RESOLVE_TASKS_TABLE}
ORDER BY alias_key
"""
            )
            .mappings()
            .all()
        )
        assert alias_rows == [
            {
                "alias_key": "stock:长电",
                "status": "pending",
                "sample_post_uid": "weibo:1",
                "sample_evidence": "长电今天继续涨",
                "sample_raw_text_excerpt": "原文里提到长电、封测和景气度。",
            }
        ]
    finally:
        conn.close()
