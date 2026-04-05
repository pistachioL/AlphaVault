from __future__ import annotations

from alphavault.weibo.display import SEGMENT_SEPARATOR
from alphavault.weibo.topic_prompt_tree import (
    build_topic_runtime_context,
    thread_root_info_for_post,
)


def test_build_topic_runtime_context_keeps_focus_virtual_replies_as_talk_reply() -> (
    None
):
    focus = "老王"
    raw_text = f"{focus}：根{SEGMENT_SEPARATOR}小李：问{SEGMENT_SEPARATOR}{focus}：答{SEGMENT_SEPARATOR}{focus}：叶"
    root_key, root_segment, root_content_key = thread_root_info_for_post(
        raw_text=raw_text,
        author=focus,
    )

    ctx, _truncated = build_topic_runtime_context(
        root_key=root_key,
        root_segment=root_segment,
        root_content_key=root_content_key,
        focus_username=focus,
        posts=[
            {
                "post_uid": "weibo:1",
                "platform_post_id": "1",
                "author": focus,
                "created_at": "2026-03-28 10:00:00",
                "raw_text": raw_text,
            }
        ],
    )

    message_lookup = ctx["message_lookup"]
    assert isinstance(message_lookup, dict)

    talk_reply_texts = [
        str(node.get("text") or "")
        for (kind, _sid), node in message_lookup.items()
        if kind == "talk_reply" and isinstance(node, dict)
    ]
    assert "答" in talk_reply_texts
