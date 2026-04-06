from __future__ import annotations

from alphavault.weibo.thread_text import SEGMENT_SEPARATOR
from alphavault.weibo.topic_prompt_tree import thread_root_info_for_post


def test_thread_root_info_for_post_uses_segmented_raw_text() -> None:
    raw_text = (
        f"泽元投资：根{SEGMENT_SEPARATOR}预知者90：中{SEGMENT_SEPARATOR}泽元投资：叶"
    )

    _root_key, root_segment, _root_content_key = thread_root_info_for_post(
        raw_text=raw_text,
        author="泽元投资",
    )

    assert root_segment == "泽元投资：根"
