from __future__ import annotations

from alphavault.domains.thread_tree.api import (
    FORWARD_ORIGINAL_MARKER,
    extract_platform_post_id,
    parse_thread_segments,
)


def test_parse_thread_segments_strip_meta_and_forward_original() -> None:
    thread_text = (
        "📌挖地瓜的超级鹿鼎公：有色可能会有相对大的调整\n\n---\n\n"
        "兔子山铂爵：紫金真要30以下嘛 [微博元信息] @用户: 兔子山铂爵 "
        "[转发原文] 有色可能会有相对大的调整"
    )
    assert parse_thread_segments(thread_text) == [
        "📌挖地瓜的超级鹿鼎公：有色可能会有相对大的调整",
        "兔子山铂爵：紫金真要30以下嘛",
    ]


def test_parse_thread_segments_forward_original_keep_comment() -> None:
    thread_text = f"A：评论 {FORWARD_ORIGINAL_MARKER} 原文"
    assert parse_thread_segments(thread_text) == ["A：评论"]


def test_parse_thread_segments_forward_original_keep_original() -> None:
    thread_text = f"{FORWARD_ORIGINAL_MARKER} 原文"
    assert parse_thread_segments(thread_text) == ["原文"]


def test_parse_thread_segments_drops_image_label_lines() -> None:
    thread_text = (
        "甲：根\n[图片] https://img.example.com/root.png\n\n---\n\n"
        "乙：叶\n[图片] https://img.example.com/leaf.png"
    )

    assert parse_thread_segments(thread_text) == ["甲：根", "乙：叶"]


def test_extract_platform_post_id_keeps_full_xueqiu_guid_suffix() -> None:
    assert extract_platform_post_id("xueqiu:status:381213336") == "status:381213336"
