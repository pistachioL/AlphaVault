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


def test_build_prompt_header_requires_single_object_per_mention_text() -> None:
    header = build_prompt_header(focus_username="老王")

    assert "一个 `mention_text` 只能放一个对象" in header
    assert "如果原文里同时提到多个对象，必须拆成多个 mentions" in header
    assert "不要把“北、上、港”这种并列简称合并成一个 `mention_text`" in header


def test_build_prompt_header_requires_manual_feedback_to_be_hint_only() -> None:
    header = build_prompt_header(focus_username="老王")

    assert "`manual_feedback_hint`" in header
    assert "只当复核提示" in header
    assert "要回到 `message_tree` 和原文" in header
