from __future__ import annotations

from alphavault.worker.ingest import _build_post_texts


def test_build_post_texts_keeps_full_xueqiu_context_in_raw_text() -> None:
    raw_text, display_text = _build_post_texts(
        title="",
        content_text="A：根 --- B：中 --- A：叶",
        platform="xueqiu",
    )

    assert raw_text == display_text
    assert "A：根" in raw_text
    assert "B：中" in raw_text
    assert "A：叶" in raw_text
    assert "\n\n---\n\n" in raw_text
    assert raw_text.strip() != "A：叶"
