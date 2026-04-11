from __future__ import annotations

from contextlib import contextmanager
from typing import Callable, cast

import pytest

from alphavault import research_stock_cache as research_stock_cache_module
from alphavault.constants import SCHEMA_WEIBO
from alphavault.db.cloud_schema import apply_cloud_schema
from alphavault.db.postgres_db import PostgresConnection
from alphavault.db.postgres_db import PostgresEngine
from alphavault.research_stock_cache import (
    EntityPageDirtyEntry,
    claim_entity_page_dirty_entries,
    dirty_reason_mask_for,
    fail_entity_page_dirty_claims,
    list_entity_page_dirty_keys,
    load_entity_page_signal_snapshot,
    mark_entity_page_dirty,
    mark_entity_page_dirty_from_assertions,
    remove_entity_page_dirty_keys,
    save_entity_page_signal_snapshot,
)


ENTITY_PAGE_SNAPSHOT_SOURCE_TABLE = f"{SCHEMA_WEIBO}.entity_page_snapshot"
PROJECTION_DIRTY_SOURCE_TABLE = f"{SCHEMA_WEIBO}.projection_dirty"
TOPIC_CLUSTER_TOPICS_SOURCE_TABLE = f"{SCHEMA_WEIBO}.topic_cluster_topics"


@pytest.fixture()
def stock_cache_conn(pg_conn) -> PostgresConnection:
    apply_cloud_schema(pg_conn, target="source", schema_name=SCHEMA_WEIBO)
    return PostgresConnection(pg_conn, schema_name=SCHEMA_WEIBO)


def test_save_and_load_entity_page_signal_snapshot(
    stock_cache_conn: PostgresConnection,
) -> None:
    save_entity_page_signal_snapshot(
        stock_cache_conn,
        stock_key="stock:601899.SH",
        payload={
            "entity_key": "stock:601899.SH",
            "entity_type": "stock",
            "header": {"title": "紫金矿业 (601899.SH)"},
            "signal_top": [
                {
                    "post_uid": "weibo:2",
                    "summary": "继续拿着",
                    "action": "trade.hold",
                    "author": "alice",
                    "created_at": "2026-03-26 10:00:00",
                    "created_at_line": "2026-03-26 10:00 · 1小时前",
                    "raw_text": "原文2",
                    "tree_label": "主贴",
                    "tree_text": "root -> child",
                },
                {
                    "post_uid": "weibo:1",
                    "summary": "小仓试错",
                    "action": "trade.buy",
                    "author": "bob",
                    "created_at": "2026-03-25 10:00:00",
                    "created_at_line": "2026-03-25 10:00 · 1天前",
                    "raw_text": "原文1",
                    "tree_label": "",
                    "tree_text": "",
                },
            ],
            "related": [
                {
                    "entity_key": "cluster:gold",
                    "entity_type": "sector",
                    "mention_count": "2",
                }
            ],
            "counters": {"signal_total": 2},
        },
    )
    loaded = load_entity_page_signal_snapshot(
        stock_cache_conn,
        stock_key="stock:601899.SH",
    )
    assert cast(str, loaded["entity_key"]) == "stock:601899.SH"
    assert cast(str, loaded["entity_type"]) == "stock"
    header = cast(dict[str, str], loaded["header"])
    counters = cast(dict[str, str], loaded["counters"])
    signal_top = cast(list[dict[str, str]], loaded["signal_top"])
    related = cast(list[dict[str, str]], loaded["related"])
    assert header["title"] == "紫金矿业 (601899.SH)"
    assert counters["signal_total"] == "2"
    assert signal_top[0]["post_uid"] == "weibo:2"
    assert signal_top[0]["tree_text"] == "root -> child"
    assert related[0]["entity_key"] == "cluster:gold"


def test_load_entity_page_signal_snapshot_filters_by_author(monkeypatch) -> None:
    @contextmanager
    def _fake_use_conn(_engine_or_conn):
        yield cast(PostgresConnection, object())

    monkeypatch.setattr(research_stock_cache_module, "_use_conn", _fake_use_conn)
    monkeypatch.setattr(
        research_stock_cache_module,
        "_select_entity_page_snapshot_row",
        lambda *_args, **_kwargs: {
            "entity_key": "stock:601899.SH",
            "entity_type": "stock",
            "header_json": '{"title":"紫金矿业 (601899.SH)"}',
            "signal_top_json": (
                '[{"post_uid":"weibo:2","summary":"继续拿着","action":"trade.hold",'
                '"author":"alice","created_at":"2026-03-26 10:00:00"},'
                '{"post_uid":"weibo:1","summary":"小仓试错","action":"trade.buy",'
                '"author":"bob","created_at":"2026-03-25 10:00:00"}]'
            ),
            "related_json": "[]",
            "counters_json": '{"signal_total":"2"}',
            "updated_at": "2026-03-26 11:00:00",
        },
    )

    loaded = load_entity_page_signal_snapshot(
        cast(PostgresConnection, object()),
        stock_key="stock:601899.SH",
        author="alice",
    )

    signal_top = cast(list[dict[str, str]], loaded["signal_top"])
    counters = cast(dict[str, str], loaded["counters"])

    assert [row["post_uid"] for row in signal_top] == ["weibo:2"]
    assert counters["signal_total"] == "1"


def test_save_and_load_entity_page_signal_snapshot_for_sector(
    stock_cache_conn: PostgresConnection,
) -> None:
    save_entity_page_signal_snapshot(
        stock_cache_conn,
        stock_key="cluster:white_liquor",
        payload={
            "entity_key": "cluster:white_liquor",
            "entity_type": "sector",
            "header": {"title": "white_liquor"},
            "signal_top": [
                {
                    "post_uid": "weibo:3",
                    "summary": "板块继续走强",
                    "action": "trade.buy",
                    "author": "alice",
                    "created_at": "2026-03-26 10:00:00",
                }
            ],
            "related": [
                {
                    "entity_key": "stock:600519.SH",
                    "entity_type": "stock",
                    "mention_count": "2",
                }
            ],
            "counters": {"signal_total": 1},
        },
    )
    loaded = load_entity_page_signal_snapshot(
        stock_cache_conn,
        stock_key="cluster:white_liquor",
    )
    assert cast(str, loaded["entity_key"]) == "cluster:white_liquor"
    assert cast(str, loaded["entity_type"]) == "sector"
    header = cast(dict[str, str], loaded["header"])
    counters = cast(dict[str, str], loaded["counters"])
    related = cast(list[dict[str, str]], loaded["related"])
    assert header["title"] == "white_liquor"
    assert counters["signal_total"] == "1"
    assert related[0]["entity_key"] == "stock:600519.SH"


def test_entity_page_signal_snapshot_skips_write_when_content_unchanged(
    monkeypatch,
    stock_cache_conn: PostgresConnection,
) -> None:
    timestamps = iter(
        [
            "2026-04-04 10:00:00",
            "2026-04-04 10:00:01",
            "2026-04-04 10:00:02",
        ]
    )
    monkeypatch.setattr(
        "alphavault.research_stock_cache._now_str",
        lambda: next(timestamps),
    )
    payload: dict[str, object] = {
        "entity_key": "stock:601899.SH",
        "entity_type": "stock",
        "header": {"title": "紫金矿业 (601899.SH)"},
        "signal_top": [{"post_uid": "weibo:1", "summary": "继续拿着"}],
        "related": [{"entity_key": "cluster:gold", "entity_type": "sector"}],
        "counters": {"signal_total": 1},
    }
    save_entity_page_signal_snapshot(
        stock_cache_conn,
        stock_key="stock:601899.SH",
        payload=payload,
    )
    first = (
        stock_cache_conn.execute(
            f"""
SELECT updated_at, content_hash
FROM {ENTITY_PAGE_SNAPSHOT_SOURCE_TABLE}
WHERE entity_key = :entity_key
""",
            {"entity_key": "stock:601899.SH"},
        )
        .mappings()
        .fetchone()
    )
    assert first is not None

    save_entity_page_signal_snapshot(
        stock_cache_conn,
        stock_key="stock:601899.SH",
        payload=payload,
    )
    second = (
        stock_cache_conn.execute(
            f"""
SELECT updated_at, content_hash
FROM {ENTITY_PAGE_SNAPSHOT_SOURCE_TABLE}
WHERE entity_key = :entity_key
""",
            {"entity_key": "stock:601899.SH"},
        )
        .mappings()
        .fetchone()
    )
    assert second == first

    save_entity_page_signal_snapshot(
        stock_cache_conn,
        stock_key="stock:601899.SH",
        payload={
            **payload,
            "signal_top": [
                {"post_uid": "weibo:1", "summary": "继续拿着"},
                {"post_uid": "weibo:2", "summary": "小仓试错"},
            ],
            "counters": {"signal_total": 2},
        },
    )
    third = (
        stock_cache_conn.execute(
            f"""
SELECT updated_at, content_hash
FROM {ENTITY_PAGE_SNAPSHOT_SOURCE_TABLE}
WHERE entity_key = :entity_key
""",
            {"entity_key": "stock:601899.SH"},
        )
        .mappings()
        .fetchone()
    )
    assert third is not None
    assert third["updated_at"] == "2026-04-04 10:00:01"
    assert third["content_hash"] != first["content_hash"]


def test_stock_page_snapshot_uses_single_entity_page_snapshot_table(
    stock_cache_conn: PostgresConnection,
) -> None:
    save_entity_page_signal_snapshot(
        stock_cache_conn,
        stock_key="stock:601899.SH",
        payload={
            "entity_key": "stock:601899.SH",
            "entity_type": "stock",
            "header": {"title": "紫金矿业 (601899.SH)"},
            "signal_top": [{"post_uid": "weibo:1"}],
            "related": [
                {
                    "entity_key": "cluster:gold",
                    "entity_type": "sector",
                }
            ],
            "counters": {"signal_total": 1},
        },
    )

    table_names = {
        str(row["tablename"])
        for row in stock_cache_conn.execute(
            """
SELECT tablename
FROM pg_tables
WHERE schemaname = :schema_name
""",
            {"schema_name": SCHEMA_WEIBO},
        )
        .mappings()
        .all()
    }
    assert "entity_page_snapshot" in table_names
    assert "research_stock_signals_hot" not in table_names
    assert "research_stock_extras_snapshot" not in table_names

    rows = (
        stock_cache_conn.execute(
            f"""
SELECT entity_key, entity_type, counters_json
FROM {ENTITY_PAGE_SNAPSHOT_SOURCE_TABLE}
ORDER BY entity_key ASC
"""
        )
        .mappings()
        .all()
    )
    assert len(rows) == 1
    assert rows[0]["entity_key"] == "stock:601899.SH"
    assert rows[0]["entity_type"] == "stock"
    assert rows[0]["counters_json"] == '{"signal_total": "1"}'

    hot = load_entity_page_signal_snapshot(
        stock_cache_conn,
        stock_key="stock:601899.SH",
    )
    counters = cast(dict[str, str], hot["counters"])
    signal_top = cast(list[dict[str, str]], hot["signal_top"])
    assert counters["signal_total"] == "1"
    assert signal_top[0]["post_uid"] == "weibo:1"


def test_mark_entity_page_dirty_merges_reason_mask_and_keeps_dirty_since(
    monkeypatch,
    stock_cache_conn: PostgresConnection,
) -> None:
    timestamps = iter(
        [
            "2026-04-04 10:00:00+08:00",
            "2026-04-04 10:05:00+08:00",
        ]
    )
    monkeypatch.setattr(
        "alphavault.research_stock_cache._now_str",
        lambda: next(timestamps),
    )

    mark_entity_page_dirty(
        stock_cache_conn,
        stock_key="stock:601899.SH",
        reason="ai_done",
    )
    mark_entity_page_dirty(
        stock_cache_conn,
        stock_key="stock:601899.SH",
        reason="relation_candidates_cache",
    )

    row = (
        stock_cache_conn.execute(
            f"""
SELECT reason_mask, dirty_since, last_dirty_at, claim_until, attempt_count
FROM {PROJECTION_DIRTY_SOURCE_TABLE}
WHERE job_type = 'entity_page' AND target_key = 'stock:601899.SH'
"""
        )
        .mappings()
        .fetchone()
    )
    assert row is not None
    assert int(row["reason_mask"] or 0) == (
        dirty_reason_mask_for("ai_done")
        | dirty_reason_mask_for("relation_candidates_cache")
    )
    assert str(row["dirty_since"]) == "2026-04-04 10:00:00+08:00"
    assert str(row["last_dirty_at"]) == "2026-04-04 10:05:00+08:00"
    assert str(row["claim_until"]) == ""
    assert int(row["attempt_count"] or 0) == 0


def test_claim_and_fail_entity_page_dirty_entries_track_attempts(
    stock_cache_conn: PostgresConnection,
) -> None:
    mark_entity_page_dirty(
        stock_cache_conn,
        stock_key="stock:601899.SH",
        reason="ai_done",
    )

    claimed = claim_entity_page_dirty_entries(
        stock_cache_conn,
        limit=10,
        claim_ttl_seconds=600,
    )
    assert len(claimed) == 1
    assert str(claimed[0]["stock_key"]) == "stock:601899.SH"
    assert int(claimed[0]["reason_mask"]) == dirty_reason_mask_for("ai_done")
    claim_until = str(claimed[0]["claim_until"])
    assert claim_until != ""
    assert (
        claim_entity_page_dirty_entries(
            stock_cache_conn,
            limit=10,
            claim_ttl_seconds=600,
        )
        == []
    )

    failed = fail_entity_page_dirty_claims(
        stock_cache_conn,
        stock_keys=["stock:601899.SH"],
        claim_until=claim_until,
    )
    assert failed == 1
    row = (
        stock_cache_conn.execute(
            f"""
SELECT attempt_count, claim_until
FROM {PROJECTION_DIRTY_SOURCE_TABLE}
WHERE job_type = 'entity_page' AND target_key = 'stock:601899.SH'
"""
        )
        .mappings()
        .fetchone()
    )
    assert row is not None
    assert int(row["attempt_count"] or 0) == 1
    assert str(row["claim_until"]) == ""


def test_mark_list_and_remove_entity_page_dirty_keys(
    stock_cache_conn: PostgresConnection,
) -> None:
    mark_entity_page_dirty(stock_cache_conn, stock_key="stock:601899.SH", reason="rss")
    mark_entity_page_dirty(stock_cache_conn, stock_key="stock:600519.SH", reason="ai")
    keys = list_entity_page_dirty_keys(stock_cache_conn, limit=10)
    assert set(keys) == {"stock:601899.SH", "stock:600519.SH"}
    remove_entity_page_dirty_keys(stock_cache_conn, stock_keys=["stock:601899.SH"])
    keys_after = list_entity_page_dirty_keys(stock_cache_conn, limit=10)
    assert keys_after == ["stock:600519.SH"]


def test_stock_dirty_uses_projection_dirty_table(
    stock_cache_conn: PostgresConnection,
) -> None:
    mark_entity_page_dirty(stock_cache_conn, stock_key="stock:601899.SH", reason="rss")

    table_names = {
        str(row["tablename"])
        for row in stock_cache_conn.execute(
            """
SELECT tablename
FROM pg_tables
WHERE schemaname = :schema_name
""",
            {"schema_name": SCHEMA_WEIBO},
        )
        .mappings()
        .all()
    }
    assert "projection_dirty" in table_names
    assert "research_stock_dirty_keys" not in table_names

    rows = (
        stock_cache_conn.execute(
            f"""
SELECT job_type, target_key, reason_mask, dirty_since, last_dirty_at, claim_until, attempt_count
FROM {PROJECTION_DIRTY_SOURCE_TABLE}
ORDER BY job_type ASC, target_key ASC
"""
        )
        .mappings()
        .all()
    )
    assert rows == [
        {
            "job_type": "entity_page",
            "target_key": "stock:601899.SH",
            "reason_mask": dirty_reason_mask_for("rss"),
            "dirty_since": str(rows[0]["dirty_since"]),
            "last_dirty_at": str(rows[0]["last_dirty_at"]),
            "claim_until": "",
            "attempt_count": 0,
        }
    ]


def test_mark_entity_page_dirty_from_assertions_reads_stock_and_sector_entities(
    stock_cache_conn: PostgresConnection,
) -> None:
    stock_cache_conn.execute(
        f"""
INSERT INTO {TOPIC_CLUSTER_TOPICS_SOURCE_TABLE}(
    topic_key,
    cluster_key,
    source,
    confidence,
    created_at
)
VALUES (
    'industry:黄金',
    'gold',
    'manual',
    1.0,
    '2026-04-06 10:00:00'
)
"""
    )
    marked = mark_entity_page_dirty_from_assertions(
        stock_cache_conn,
        assertions=[
            {
                "assertion_entities": [
                    {
                        "entity_key": "stock:601899.SH",
                        "entity_type": "stock",
                        "match_source": "stock_code",
                        "is_primary": 1,
                    },
                    {
                        "entity_key": "industry:黄金",
                        "entity_type": "industry",
                        "match_source": "industry_name",
                        "is_primary": 0,
                    },
                ],
            },
            {"assertion_entities": []},
            {
                "assertion_entities": [
                    {
                        "entity_key": "cluster:energy",
                        "entity_type": "sector",
                        "match_source": "manual",
                        "is_primary": 1,
                    }
                ]
            },
        ],
        reason="ai",
    )
    assert marked == 3
    keys = set(list_entity_page_dirty_keys(stock_cache_conn, limit=10))
    assert keys == {"stock:601899.SH", "cluster:gold", "cluster:energy"}


def _assert_transaction_helper_used(
    monkeypatch: pytest.MonkeyPatch,
    *,
    expected_result: object,
    call_fn: Callable[[PostgresEngine], object],
) -> None:
    helper_calls: list[object] = []

    def _fake_run(engine_or_conn, fn):  # type: ignore[no-untyped-def]
        del fn
        helper_calls.append(engine_or_conn)
        return expected_result

    @contextmanager
    def _fail_use_conn(_engine_or_conn):  # type: ignore[no-untyped-def]
        raise AssertionError("old_transaction_path_used")
        yield

    engine = cast(PostgresEngine, object())
    monkeypatch.setattr(
        research_stock_cache_module,
        "run_postgres_transaction",
        _fake_run,
        raising=False,
    )
    monkeypatch.setattr(research_stock_cache_module, "_use_conn", _fail_use_conn)

    assert call_fn(engine) == expected_result
    assert helper_calls == [engine]


def test_remove_entity_page_dirty_keys_uses_run_postgres_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _call(engine: PostgresEngine) -> int:
        return research_stock_cache_module.remove_entity_page_dirty_keys(
            engine,
            stock_keys=["stock:601899.SH"],
        )

    _assert_transaction_helper_used(
        monkeypatch,
        expected_result=11,
        call_fn=_call,
    )


def test_claim_entity_page_dirty_entries_uses_run_postgres_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected: list[EntityPageDirtyEntry] = [
        {
            "stock_key": "stock:601899.SH",
            "reason_mask": 0,
            "dirty_since": "",
            "last_dirty_at": "",
            "claim_until": "",
            "attempt_count": 0,
            "updated_at": "",
        }
    ]

    def _call(engine: PostgresEngine) -> list[EntityPageDirtyEntry]:
        return research_stock_cache_module.claim_entity_page_dirty_entries(
            engine,
            limit=2,
            claim_ttl_seconds=60,
        )

    _assert_transaction_helper_used(
        monkeypatch,
        expected_result=expected,
        call_fn=_call,
    )


def test_release_entity_page_dirty_claims_uses_run_postgres_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _call(engine: PostgresEngine) -> int:
        return research_stock_cache_module.release_entity_page_dirty_claims(
            engine,
            stock_keys=["stock:601899.SH"],
            claim_until="2026-04-07 10:00:00",
        )

    _assert_transaction_helper_used(
        monkeypatch,
        expected_result=12,
        call_fn=_call,
    )


def test_fail_entity_page_dirty_claims_uses_run_postgres_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _call(engine: PostgresEngine) -> int:
        return research_stock_cache_module.fail_entity_page_dirty_claims(
            engine,
            stock_keys=["stock:601899.SH"],
            claim_until="2026-04-07 10:00:00",
        )

    _assert_transaction_helper_used(
        monkeypatch,
        expected_result=13,
        call_fn=_call,
    )
