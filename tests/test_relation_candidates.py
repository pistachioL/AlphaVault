from __future__ import annotations

import pandas as pd

from alphavault_reflex.services.relation_candidates import (
    build_sector_relation_candidates,
    build_stock_alias_candidates,
    build_stock_sector_candidates,
    classify_sector_relation_label,
    enrich_candidates_with_ai,
)


def test_build_stock_sector_candidates_prefers_high_overlap_entities() -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:600519.SH",
                "cluster_keys": ["white_liquor", "consumer"],
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:600519.SH",
                "cluster_keys": ["white_liquor"],
            },
        ]
    )
    rows = build_stock_sector_candidates(assertions, stock_key="stock:600519.SH")
    assert rows[0]["sector_key"] == "white_liquor"
    assert rows[0]["reason_code"] == "stock_sector_overlap"


def test_classify_sector_relation_label_without_ai_returns_related() -> None:
    assert classify_sector_relation_label(ai_enabled=False, explanation="") == "related"


def test_build_stock_alias_candidates_and_sector_relation_candidates() -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:600519.SH",
                "stock_names": ["贵州茅台"],
                "cluster_keys": ["white_liquor", "consumer"],
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:000858.SZ",
                "stock_names": ["五粮液"],
                "cluster_keys": ["white_liquor"],
            },
            {
                "post_uid": "p3",
                "topic_key": "stock:600887.SH",
                "stock_names": ["伊利股份"],
                "cluster_keys": ["consumer"],
            },
        ]
    )
    alias_rows = build_stock_alias_candidates(assertions, stock_key="stock:600519.SH")
    assert alias_rows[0]["alias_key"] == "stock:贵州茅台"

    sector_rows = build_sector_relation_candidates(
        assertions, sector_key="white_liquor"
    )
    assert sector_rows[0]["sector_key"] == "consumer"


def test_enrich_candidates_with_ai_skips_when_disabled() -> None:
    rows = enrich_candidates_with_ai(
        [
            {
                "candidate_key": "white_liquor",
                "sector_key": "white_liquor",
                "score": "2",
                "evidence_summary": "该个股与板块共现 2 次",
                "reason_code": "stock_sector_overlap",
            }
        ],
        relation_type="stock_sector",
        ai_enabled=False,
    )
    assert rows[0]["ai_status"] == "skipped"
