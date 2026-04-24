from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

POST_CONTEXT_PROMPT_VERSION = "post-context-v1"

_HEADER_TEMPLATE_PATH = Path(__file__).with_name("post_context_prompt_header.txt")


@lru_cache(maxsize=1)
def _load_prompt_header_template() -> str:
    return _HEADER_TEMPLATE_PATH.read_text(encoding="utf-8")


def build_post_context_prompt(
    *,
    ai_topic_package: dict[str, Any],
    compact_json: bool = False,
) -> str:
    header = _load_prompt_header_template()
    if compact_json:
        topic_json = json.dumps(
            ai_topic_package,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    else:
        topic_json = json.dumps(ai_topic_package, ensure_ascii=False, indent=2)
    return (
        "\n\n".join(
            [
                header,
                "\n".join(
                    [
                        "话题块 1",
                        "JSON：",
                        "```json",
                        topic_json,
                        "```",
                    ]
                ),
            ]
        )
        + "\n"
    )


__all__ = [
    "POST_CONTEXT_PROMPT_VERSION",
    "build_post_context_prompt",
]
