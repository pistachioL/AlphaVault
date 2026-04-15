from __future__ import annotations

from alphavault.domains.stock.object_index import (
    build_stock_object_index,
    filter_assertions_for_stock_object,
    resolve_stock_object_key,
)


def _rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return rows


def test_build_stock_object_index_merges_code_and_full_name_but_not_short_alias() -> (
    None
):
    assertions = _rows(
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


def test_build_stock_object_index_normalizes_prefixed_cn_code_with_wrong_us_suffix() -> (
    None
):
    assertions = _rows(
        [
            {
                "post_uid": "p1",
                "entity_key": "stock:SZ000725.US",
                "stock_codes": ["SZ000725.US"],
                "stock_names": ["京东方A"],
            }
        ]
    )

    index = build_stock_object_index(assertions)

    assert index.resolve("stock:SZ000725.US") == "stock:000725.SZ"
    assert index.page_title("stock:SZ000725.US") == "京东方A (000725.SZ)"


def test_filter_assertions_for_stock_object_uses_accepted_alias_relations() -> None:
    assertions = _rows(
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
    relations = _rows(
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

    assert [row["post_uid"] for row in filtered] == ["p1", "p2"]
