from __future__ import annotations

from alphavault.worker.ingest import _build_post_raw_text


def test_build_post_raw_text_keeps_weibo_text_and_image_labels() -> None:
    raw_text = _build_post_raw_text(
        title="",
        content_text="正文",
        platform="weibo",
        author="作者",
        image_urls=["https://img.example.com/1.png"],
    )

    assert raw_text == "作者：正文\n[图片] https://img.example.com/1.png"


def test_build_post_raw_text_keeps_image_only_weibo() -> None:
    raw_text = _build_post_raw_text(
        title="",
        content_text="",
        platform="weibo",
        author="作者",
        image_urls=["https://img.example.com/1.png"],
    )

    assert raw_text == "[图片] https://img.example.com/1.png"
