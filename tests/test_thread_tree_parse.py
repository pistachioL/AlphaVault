from __future__ import annotations

from alphavault.domains.thread_tree.api import (
    FORWARD_ORIGINAL_MARKER,
    extract_platform_post_id,
    parse_display_md_segments,
)


def test_parse_display_md_segments_strip_meta_and_forward_original() -> None:
    display_md = (
        "📌挖地瓜的超级鹿鼎公：有色可能会有相对大的调整\n\n---\n\n"
        "兔子山铂爵：紫金真要30以下嘛 [微博元信息] @用户: 兔子山铂爵 "
        "[转发原文] 有色可能会有相对大的调整"
    )
    assert parse_display_md_segments(display_md) == [
        "📌挖地瓜的超级鹿鼎公：有色可能会有相对大的调整",
        "兔子山铂爵：紫金真要30以下嘛",
    ]


def test_parse_display_md_segments_forward_original_keep_comment() -> None:
    display_md = f"A：评论 {FORWARD_ORIGINAL_MARKER} 原文"
    assert parse_display_md_segments(display_md) == ["A：评论"]


def test_parse_display_md_segments_forward_original_keep_original() -> None:
    display_md = f"{FORWARD_ORIGINAL_MARKER} 原文"
    assert parse_display_md_segments(display_md) == ["原文"]


def test_extract_platform_post_id_keeps_full_xueqiu_guid_suffix() -> None:
    assert extract_platform_post_id("xueqiu:status:381213336") == "status:381213336"
