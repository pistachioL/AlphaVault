from __future__ import annotations

import json

from alphavault_reflex.pages.stock_research import _signal_card


def _rendered_text(child: dict) -> str:
    return json.loads(child["children"][0]["contents"])


def test_signal_card_renders_action_author_and_time_in_one_row() -> None:
    component = _signal_card(
        {
            "summary": "继续看",
            "action": "trade.watch",
            "author": "挖地瓜的超级鹿鼎公",
            "created_at_line": "2026-03-25 06:19 · 19小时前",
            "tree_text": "",
            "display_md": "",
            "raw_text": "原文内容",
        }
    )
    rendered = component.render()
    meta_row = rendered["children"][1]
    time_fragment = meta_row["children"][2]
    time_child = time_fragment["children"][0]["true_value"]["children"][0]

    assert meta_row["name"] == '"div"'
    assert [_rendered_text(child) for child in meta_row["children"][:2]] == [
        "trade.watch",
        "挖地瓜的超级鹿鼎公",
    ]
    assert _rendered_text(time_child) == "2026-03-25 06:19 · 19小时前"
