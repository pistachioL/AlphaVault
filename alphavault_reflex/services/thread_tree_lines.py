from __future__ import annotations

import re

TREE_PREFIX_RE = re.compile(r"^(((?:│   |    )*)(?:├── |└── ))")
TREE_ID_SUFFIX_RE = re.compile(r"\s*(\[(?:原帖|转发|源帖|帖子) ID: [^\]]+\])\s*$")

TREE_COLLAPSE_HINT_PREFIX = "…… 已折叠 "


def _split_tree_line(raw_line: object) -> tuple[str, str]:
    line = str(raw_line or "")
    match = TREE_PREFIX_RE.match(line)
    if match is None:
        return "", line
    prefix = str(match.group(1) or "")
    content = line[len(prefix) :]
    return prefix, content


def _split_tree_id_suffix(raw_content: object) -> tuple[str, str]:
    content = str(raw_content or "")
    match = TREE_ID_SUFFIX_RE.search(content)
    if match is None:
        return content, ""
    suffix = str(match.group(1) or "")
    main = content[: match.start()].rstrip()
    return main, suffix


def build_tree_render_lines(
    text: object,
    *,
    collapse_hint_prefix: str = TREE_COLLAPSE_HINT_PREFIX,
) -> list[dict[str, str]]:
    raw = str(text or "")
    if not raw:
        return []

    lines: list[dict[str, str]] = []
    last_tree_prefix = ""
    for raw_line in raw.splitlines():
        prefix, content = _split_tree_line(raw_line)
        if prefix:
            last_tree_prefix = prefix
            main, id_suffix = _split_tree_id_suffix(content)
            lines.append(
                {
                    "prefix": prefix,
                    "content": main if main != "" else " ",
                    "id_suffix": id_suffix,
                    "row_class": "av-tree-line",
                    "prefix_class": "av-tree-line-prefix",
                }
            )
            continue

        line = str(raw_line or "")
        stripped = line.strip()
        is_collapsed_hint = bool(
            collapse_hint_prefix and stripped.startswith(collapse_hint_prefix)
        )
        is_continuation = bool(last_tree_prefix and stripped and not is_collapsed_hint)
        if is_continuation:
            main, id_suffix = _split_tree_id_suffix(line)
            lines.append(
                {
                    "prefix": " " * len(last_tree_prefix),
                    "content": main if main != "" else " ",
                    "id_suffix": id_suffix,
                    "row_class": "av-tree-line av-tree-line-continuation",
                    "prefix_class": (
                        "av-tree-line-prefix av-tree-line-prefix-continuation"
                    ),
                }
            )
            continue

        if stripped:
            last_tree_prefix = ""
        main, id_suffix = _split_tree_id_suffix(line)
        lines.append(
            {
                "prefix": "",
                "content": main if main != "" else " ",
                "id_suffix": id_suffix,
                "row_class": "av-tree-line av-tree-line-no-prefix",
                "prefix_class": "",
            }
        )

    return lines


__all__ = [
    "TREE_COLLAPSE_HINT_PREFIX",
    "TREE_ID_SUFFIX_RE",
    "TREE_PREFIX_RE",
    "build_tree_render_lines",
]
