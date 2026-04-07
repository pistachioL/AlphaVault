from __future__ import annotations

import pandas as pd

from alphavault.domains.stock.object_index import (
    build_stock_object_index,
    filter_assertions_for_stock_object,
    resolve_stock_object_key,
)


def test_build_stock_object_index_merges_code_and_full_name_but_not_short_alias() -> (
    None
):
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "entity_key": "stock:601899.SH",
                "stock_codes": ["601899.SH"],
                "stock_names": ["紫金矿业"],
            },
            {
                "post_uid": "p2",
                "entity_key": "stock:紫金矿业",
                "stock_codes": [],
                "stock_names": ["紫金矿业"],
            },
            {
                "post_uid": "p3",
                "entity_key": "stock:紫金",
                "stock_codes": [],
                "stock_names": ["紫金"],
            },
        ]
    )

    index = build_stock_object_index(assertions)

    assert index.display_name_by_object_key["stock:601899.SH"] == "紫金矿业"
    assert index.member_keys_by_object_key["stock:601899.SH"] == {
        "stock:601899.SH",
        "stock:紫金矿业",
    }
    assert resolve_stock_object_key(assertions, stock_key="stock:紫金") == "stock:紫金"


def test_filter_assertions_for_stock_object_uses_accepted_alias_relations() -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "entity_key": "stock:601899.SH",
                "stock_codes": ["601899.SH"],
                "stock_names": ["紫金矿业"],
            },
            {
                "post_uid": "p2",
                "entity_key": "stock:阿紫",
                "stock_codes": [],
                "stock_names": ["阿紫"],
            },
        ]
    )
    relations = pd.DataFrame(
        [
            {
                "relation_type": "stock_alias",
                "left_key": "stock:601899.SH",
                "right_key": "stock:阿紫",
                "relation_label": "alias_of",
            }
        ]
    )

    filtered = filter_assertions_for_stock_object(
        assertions,
        stock_key="stock:601899.SH",
        stock_relations=relations,
    )

    assert filtered["post_uid"].tolist() == ["p1", "p2"]
