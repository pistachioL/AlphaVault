from __future__ import annotations

from alphavault.ai.topic_prompt_v4 import build_prompt_header


def test_build_prompt_header_requires_empty_assertions_for_empty_mentions() -> None:
    header = build_prompt_header(focus_username="老王")

    assert "这时 `assertions` 也必须是空数组" in header
    assert (
        "只有“转发”“转发微博”这类极短态度，且没有点名任何具体对象时，不要生成 assertion"
        in header
    )
    assert (
        "只要生成了 assertion，`assertions[*].mentions` 就必须至少有 1 个字符串"
        in header
    )
