from __future__ import annotations

from alphavault.worker.ingest import _build_post_raw_text


def test_build_post_raw_text_keeps_full_xueqiu_context_in_raw_text() -> None:
    raw_text = _build_post_raw_text(
        title="",
        content_text="A：根 --- B：中 --- A：叶",
        platform="xueqiu",
        author="A",
        image_urls=[],
    )

    assert "A：根" in raw_text
    assert "B：中" in raw_text
    assert "A：叶" in raw_text
    assert "\n\n---\n\n" in raw_text
    assert raw_text.strip() != "A：叶"


def test_build_post_raw_text_appends_image_labels_to_last_segment() -> None:
    raw_text = _build_post_raw_text(
        title="",
        content_text="A：根 --- B：中 --- A：叶",
        platform="xueqiu",
        author="A",
        image_urls=["https://img.example.com/1.png"],
    )

    assert raw_text.endswith("A：叶\n[图片] https://img.example.com/1.png")
