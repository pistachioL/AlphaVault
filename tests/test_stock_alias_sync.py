from __future__ import annotations

import pandas as pd

from alphavault.worker.stock_alias_sync import _candidate_alias_pairs
from alphavault.worker.stock_alias_sync import _existing_alias_pairs


def test_candidate_alias_pairs_filters_invalid_and_dedupes() -> None:
    ai_alias_map = {
        "stock:紫金": "stock:601899.SH",
        " stock:紫金 ": "stock:601899.SH",
        "stock:601899.SH": "stock:601899.SH",
        "": "stock:600519.SH",
        "stock:茅台": "",
        "cluster:white_liquor": "stock:600519.SH",
    }

    pairs = _candidate_alias_pairs(ai_alias_map)

    assert pairs == [("stock:601899.SH", "stock:紫金")]


def test_existing_alias_pairs_reads_left_right_from_relations_frame() -> None:
    relations = pd.DataFrame(
        [
            {
                "relation_type": "stock_alias",
                "left_key": "stock:601899.SH",
                "right_key": "stock:紫金",
                "relation_label": "alias_of",
            },
            {
                "relation_type": "stock_alias",
                "left_key": "",
                "right_key": "stock:无效",
                "relation_label": "alias_of",
            },
        ]
    )

    pairs = _existing_alias_pairs(relations)

    assert pairs == {("stock:601899.SH", "stock:紫金")}
