from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import re
from typing import TypedDict

from alphavault.domains.thread_tree.api import parse_weibo_csv_raw_fields
from alphavault.weibo.thread_text import SEGMENT_SEPARATOR
from alphavault.weibo.thread_text import format_weibo_thread_text
from alphavault.research_signal_view import coerce_signal_timestamp
from alphavault.research_signal_view import default_signal_reference_time
from alphavault.research_signal_view import format_signal_created_at_line
from alphavault_reflex.services.thread_tree_lines import build_tree_render_lines


RELATED_FILTER_ALL = "all"
RELATED_FILTER_SIGNAL = "signal"
RELATED_FILTERS = {RELATED_FILTER_ALL, RELATED_FILTER_SIGNAL}
MATCH_KIND_ASSERTION = "assertion"
MATCH_KIND_CONTEXT = "context"

DEFAULT_RELATED_LIMIT = 20
RELATED_LIMIT_STEP = 20
MAX_RELATED_LIMIT = 500
TREE_ROOT_PREVIEW_CHAR_LIMIT = 88
TREE_ROOT_EXPAND_LABEL = "展开原文"

_WEIBO_REPLY_CHAIN_MARKERS = (
    "//@",
    "回复@",
    "转发 @",
    "转发@",
    "[微博元信息]",
    "[转发原文]",
)
_TREE_PREFIX_RE = re.compile(r"^(((?:│   |    )*)(?:├── |└── ))")
_SPEAKER_LINE_RE = re.compile(r"^([^：:\s][^：:]{0,40})([：:])(.+)$", re.DOTALL)
_TREE_ID_SUFFIX_RE = re.compile(r"\[(?:原帖|转发|源帖|帖子) ID: [^\]]+\]$")
_META_MARKERS = ("[微博元信息]", "[转发原文]")
_CSV_FIELDS_MARKER = "[CSV原始字段]"
_FORWARD_ORIGINAL_MARKER = "[转发原文]"


class StockRelatedPostRow(TypedDict):
    post_uid: str
    match_kind: str
    title: str
    is_signal: str
    signal_badge: str
    action: str
    author: str
    author_href: str
    created_at_sort: str
    created_at_line: str
    url: str
    raw_text: str
    preview: str
    tree_text: str
    tree_lines: list[dict[str, str]]
    tree_preview_lines: list[dict[str, str]]
    tree_can_expand: str
    tree_expand_label: str
    tree_root_preview_line: dict[str, str]
    tree_root_full_line: dict[str, str]
    tree_root_expand_line: dict[str, str]
    tree_tail_lines: list[dict[str, str]]
    tree_root_can_expand: str
    tree_root_expand_label: str


def normalize_related_filter(value: object) -> str:
    text = str(value or "").strip().lower()
    return text if text in RELATED_FILTERS else RELATED_FILTER_ALL


def normalize_related_limit(value: object) -> int:
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        parsed = DEFAULT_RELATED_LIMIT
    if parsed <= 0:
        parsed = DEFAULT_RELATED_LIMIT
    return max(1, min(int(parsed), int(MAX_RELATED_LIMIT)))


def _signal_badge(action: str) -> str:
    a = str(action or "").strip().lower()
    if a.startswith("trade.buy"):
        return "买"
    if a.startswith("trade.sell"):
        return "卖"
    if a.startswith("trade."):
        return "信号"
    return ""


def _as_time_sort_text(created_at: object) -> str:
    text = str(created_at or "").strip()
    return text


def _coerce_row_dict(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): str(raw or "").strip()
        for key, raw in value.items()
        if str(key or "").strip()
    }


@dataclass(frozen=True)
class RelatedFeed:
    rows: list[StockRelatedPostRow]
    total: int


def _contains_weibo_reply_chain_marker(text: object) -> bool:
    value = str(text or "")
    if not value:
        return False
    return any(marker in value for marker in _WEIBO_REPLY_CHAIN_MARKERS)


def _split_tree_prefix(line: object) -> tuple[str, str]:
    text = str(line or "")
    match = _TREE_PREFIX_RE.match(text)
    if match is None:
        return "", text
    prefix = str(match.group(1) or "")
    return prefix, text[len(prefix) :]


def _format_dialogue_chain_line(line: str, *, author: str) -> str:
    text = str(line or "").strip()
    if not text:
        return ""
    match = _SPEAKER_LINE_RE.match(text)
    if match is None:
        return text
    speaker = str(match.group(1) or "").strip()
    sep = str(match.group(2) or "：")
    content = str(match.group(3) or "").lstrip()
    if not speaker:
        return text
    if speaker == str(author or "").strip():
        return f"📌{speaker}{sep}{content}"
    return f"@{speaker}{sep}{content}"


def _normalize_compare_key(line: str) -> str:
    text = str(line or "").strip()
    if not text:
        return ""
    text = _TREE_ID_SUFFIX_RE.sub("", text).rstrip()
    if text.startswith("📌") or text.startswith("@"):
        text = text[1:].strip()
    match = _SPEAKER_LINE_RE.match(text)
    content = str(match.group(3) if match else text)
    normalized = re.sub(r"\s+", " ", content).strip().lower()
    return normalized[:180]


def _extract_forward_original_block(raw_text: str) -> str:
    text = str(raw_text or "")
    idx = text.find(_FORWARD_ORIGINAL_MARKER)
    if idx < 0:
        return ""
    tail = text[idx + len(_FORWARD_ORIGINAL_MARKER) :]
    cut_idx = tail.find(_CSV_FIELDS_MARKER)
    if cut_idx >= 0:
        tail = tail[:cut_idx]
    cleaned = re.sub(r"\s+", " ", tail).strip()
    return cleaned


def _extract_forward_original_post_id(raw_text: str) -> str:
    fields = parse_weibo_csv_raw_fields(str(raw_text or ""))
    value = str(fields.get("源微博id") or "").strip()
    if not value:
        return ""
    value = value.replace("\t", "")
    value = re.sub(r"(?:\\+t)+$", "", value)
    return value.strip()


def _is_weibo_reply_like(*, post_uid: str, raw_text: str) -> bool:
    uid = str(post_uid or "").strip().lower()
    if not uid.startswith("weibo:"):
        return False
    if _extract_forward_original_post_id(raw_text):
        return True
    return _contains_weibo_reply_chain_marker(raw_text)


def _strip_first_tree_level(line: str) -> str:
    value = str(line or "")
    if value.startswith(("└── ", "├── ")):
        return value[4:]
    if value.startswith("    ") or value.startswith("│   "):
        return value[4:]
    return value


def _remove_source_root_if_present(tree_text: str) -> str:
    lines = str(tree_text or "").splitlines()
    if len(lines) <= 1:
        return str(tree_text or "").strip()
    first = str(lines[0] or "")
    if "[源帖 ID:" not in first:
        return str(tree_text or "").strip()
    rest = [_strip_first_tree_level(line) for line in lines[1:]]
    return "\n".join(rest).strip()


def _strip_meta_lines(tree_text: str) -> str:
    out: list[str] = []
    for raw_line in str(tree_text or "").splitlines():
        line = str(raw_line or "")
        stripped = line.strip()
        if not stripped:
            out.append(line)
            continue
        if any(marker in stripped for marker in _META_MARKERS):
            continue
        if stripped.startswith("@用户:"):
            continue
        out.append(line)
    return "\n".join(out).strip()


def _format_linear_tree_text(lines: list[str]) -> str:
    cleaned = [str(line or "").strip() for line in lines if str(line or "").strip()]
    if not cleaned:
        return ""
    out = [cleaned[0]]
    for depth, line in enumerate(cleaned[1:], start=1):
        out.append(("    " * (depth - 1)) + "└── " + line)
    return "\n".join(out).strip()


def _build_dialogue_chain_tree_text(row: Mapping[str, object]) -> str:
    raw_text = str(row.get("raw_text") or "").strip()
    author = str(row.get("author") or "").strip()
    if not raw_text:
        return ""

    original_block = _extract_forward_original_block(raw_text)
    original_post_id = _extract_forward_original_post_id(raw_text)
    root_line = ""
    if original_block and author:
        root_line = f"📌{author}：{original_block}"
    elif original_block:
        root_line = original_block
    if root_line and original_post_id:
        root_line = f"{root_line} [原帖 ID: {original_post_id}]"

    rendered_text = format_weibo_thread_text(raw_text, author=author)
    rebuilt_lines = [
        segment.strip()
        for segment in rendered_text.split(SEGMENT_SEPARATOR)
        if str(segment or "").strip()
    ]
    rendered_lines_raw = [
        _format_dialogue_chain_line(line, author=author)
        for line in rebuilt_lines
        if str(line or "").strip()
    ]
    if root_line:
        root_key = _normalize_compare_key(root_line)
        rendered_lines = [
            line
            for line in rendered_lines_raw
            if _normalize_compare_key(line) != root_key
        ]
        chain = [root_line, *rendered_lines]
    else:
        chain = rendered_lines_raw
    return _format_linear_tree_text(chain)


def _empty_tree_line() -> dict[str, str]:
    return {
        "prefix": "",
        "content": "",
        "id_suffix": "",
        "row_class": "av-tree-line av-tree-line-no-prefix",
        "prefix_class": "",
    }


def _truncate_root_content(content: str) -> tuple[str, bool]:
    text = str(content or "")
    if len(text) <= int(TREE_ROOT_PREVIEW_CHAR_LIMIT):
        return text, False
    clipped = text[: int(TREE_ROOT_PREVIEW_CHAR_LIMIT) - 1].rstrip() + "…"
    return clipped, True


def _build_root_expand_line(
    root_line: dict[str, str], *, preview_content: str
) -> dict[str, str]:
    full_content = str(root_line.get("content") or "")
    if not full_content:
        return _empty_tree_line()
    preview_prefix = str(preview_content or "").rstrip("…").rstrip()
    remainder = full_content
    if preview_prefix and full_content.startswith(preview_prefix):
        remainder = full_content[len(preview_prefix) :].lstrip()
    if not remainder:
        return _empty_tree_line()
    return {
        "prefix": str(root_line.get("prefix") or ""),
        "content": remainder,
        "id_suffix": "",
        "row_class": str(root_line.get("row_class") or "av-tree-line"),
        "prefix_class": str(root_line.get("prefix_class") or ""),
    }


def _split_tree_for_root_expand(
    tree_lines: list[dict[str, str]],
) -> tuple[dict[str, str], dict[str, str], list[dict[str, str]], bool]:
    if not tree_lines:
        empty = _empty_tree_line()
        return empty, empty, [], False
    root_preview = dict(tree_lines[0])
    content = str(root_preview.get("content") or "")
    clipped, can_expand = _truncate_root_content(content)
    if can_expand:
        root_preview["content"] = clipped
    root_expand_line = (
        _build_root_expand_line(dict(tree_lines[0]), preview_content=clipped)
        if can_expand
        else _empty_tree_line()
    )
    tail_lines = [dict(line) for line in tree_lines[1:]]
    can_expand_final = bool(
        can_expand and str(root_expand_line.get("content") or "").strip()
    )
    return (
        root_preview,
        root_expand_line if can_expand_final else _empty_tree_line(),
        tail_lines,
        can_expand_final,
    )


def _normalize_context_tree_text(tree_text: str) -> str:
    lines = build_tree_render_lines(tree_text)
    if not lines:
        return str(tree_text or "").strip()
    rebuilt: list[str] = []
    for idx, line in enumerate(lines):
        prefix = str(line.get("prefix") or "")
        content = str(line.get("content") or "").rstrip()
        suffix = str(line.get("id_suffix") or "").strip()
        if idx > 0 and not prefix:
            continue
        if idx > 0:
            starts_dialogue = content.startswith("@") or content.startswith("📌")
            if (not starts_dialogue) and (_SPEAKER_LINE_RE.match(content) is None):
                continue
        if idx == 0 and suffix.startswith("[源帖 ID:"):
            suffix = suffix.replace("[源帖 ID:", "[原帖 ID:")
        if idx > 0:
            suffix = ""
        merged = content
        if suffix:
            merged = f"{merged} {suffix}".rstrip()
        if idx > 0:
            merged = _TREE_ID_SUFFIX_RE.sub("", merged).rstrip()
        rebuilt.append(prefix + merged)
    return "\n".join(rebuilt).strip()


def _normalize_full_tree_text(tree_text: str) -> str:
    lines = build_tree_render_lines(tree_text)
    if not lines:
        return str(tree_text or "").strip()
    rebuilt: list[str] = []
    for idx, line in enumerate(lines):
        prefix = str(line.get("prefix") or "")
        content = str(line.get("content") or "").rstrip()
        suffix = str(line.get("id_suffix") or "").strip()
        if idx == 0 and suffix.startswith("[源帖 ID:"):
            suffix = suffix.replace("[源帖 ID:", "[原帖 ID:")
        merged = content
        if suffix:
            merged = f"{merged} {suffix}".rstrip()
        rebuilt.append(prefix + merged)
    return "\n".join(rebuilt).strip()


def _resolve_tree_text(row: Mapping[str, object]) -> str:
    post_uid = str(row.get("post_uid") or "").strip().lower()
    raw_text = str(row.get("raw_text") or "").strip()
    existing_tree_text = str(row.get("tree_text") or "").strip()
    is_reply_like = _is_weibo_reply_like(post_uid=post_uid, raw_text=raw_text)
    should_rebuild = bool(is_reply_like and raw_text)
    if should_rebuild:
        rebuilt = _build_dialogue_chain_tree_text(row)
        if rebuilt:
            return rebuilt

    tree_text = _strip_meta_lines(existing_tree_text)
    if not tree_text:
        return ""
    if is_reply_like:
        cleaned = _remove_source_root_if_present(tree_text)
        return _normalize_context_tree_text(cleaned)
    return _normalize_full_tree_text(tree_text)


def _ensure_created_at_line(row: Mapping[str, object], *, now) -> str:
    """Return a stable `created_at_line` with relative age ("· xx小时前")."""
    existing = str(row.get("created_at_line") or "").strip()
    if existing and "·" in existing:
        return existing

    created_at = str(row.get("created_at") or "").strip()
    source = created_at or existing
    if not source:
        return existing

    filled = format_signal_created_at_line(source, now=now)
    return filled or existing


def _fallback_title_from_raw_text(raw_text: str) -> str:
    for raw_line in str(raw_text or "").splitlines():
        line = re.sub(r"\s+", " ", str(raw_line or "").strip())
        if line:
            return line[:80]
    return ""


def build_related_feed(
    *,
    signals: list[dict[str, str]] | list[object],
    related_filter: object,
    limit: object,
    now: object | None = None,
) -> RelatedFeed:
    """
    Build the stock feed from resolved stock post rows.

    This is UI-focused: keep it deterministic and cheap (no DB calls).
    """
    wanted_filter = normalize_related_filter(related_filter)
    wanted_limit = normalize_related_limit(limit)
    reference_now = coerce_signal_timestamp(now) or default_signal_reference_time()

    items: list[StockRelatedPostRow] = []
    seen: set[str] = set()

    for raw in signals or []:
        row = _coerce_row_dict(raw)
        post_uid = str(row.get("post_uid") or "").strip()
        if not post_uid or post_uid in seen:
            continue
        seen.add(post_uid)
        match_kind = str(row.get("match_kind") or "").strip() or MATCH_KIND_ASSERTION
        action = str(row.get("action") or "").strip()
        badge = _signal_badge(action)
        is_signal = str(row.get("is_signal") or "").strip() or ("1" if badge else "")
        raw_text = str(row.get("raw_text") or "").strip()
        tree_text = _resolve_tree_text(row)
        tree_lines = build_tree_render_lines(tree_text)
        root_preview_line, root_expand_line, tree_tail_lines, root_can_expand = (
            _split_tree_for_root_expand(tree_lines)
        )
        root_full_line = dict(tree_lines[0]) if tree_lines else _empty_tree_line()
        preview_lines = [root_preview_line, *tree_tail_lines] if tree_lines else []
        items.append(
            {
                "post_uid": post_uid,
                "match_kind": match_kind,
                "is_signal": is_signal,
                "signal_badge": badge,
                "created_at_line": _ensure_created_at_line(row, now=reference_now),
                "created_at_sort": _as_time_sort_text(row.get("created_at")),
                "title": str(row.get("summary") or "").strip()
                or _fallback_title_from_raw_text(raw_text),
                "action": action,
                "author": str(row.get("author") or "").strip(),
                "author_href": "",
                "preview": "",
                "tree_text": tree_text,
                "tree_lines": tree_lines,
                "tree_preview_lines": preview_lines,
                "tree_can_expand": "1" if root_can_expand else "",
                "tree_expand_label": TREE_ROOT_EXPAND_LABEL if root_can_expand else "",
                "tree_root_preview_line": root_preview_line,
                "tree_root_full_line": root_full_line,
                "tree_root_expand_line": root_expand_line,
                "tree_tail_lines": tree_tail_lines,
                "tree_root_can_expand": "1" if root_can_expand else "",
                "tree_root_expand_label": (
                    TREE_ROOT_EXPAND_LABEL if root_can_expand else ""
                ),
                "url": str(row.get("url") or "").strip(),
                "raw_text": raw_text,
            }
        )

    filtered = (
        [row for row in items if str(row.get("is_signal") or "") == "1"]
        if wanted_filter == RELATED_FILTER_SIGNAL
        else items
    )

    filtered.sort(
        key=lambda row: (
            str(row.get("created_at_sort") or ""),
            str(row.get("post_uid") or ""),
        ),
        reverse=True,
    )
    sliced = filtered[: max(1, int(wanted_limit))]
    return RelatedFeed(rows=sliced, total=int(len(filtered)))


__all__ = [
    "DEFAULT_RELATED_LIMIT",
    "MAX_RELATED_LIMIT",
    "MATCH_KIND_ASSERTION",
    "MATCH_KIND_CONTEXT",
    "RELATED_FILTER_ALL",
    "RELATED_FILTER_SIGNAL",
    "RELATED_LIMIT_STEP",
    "RelatedFeed",
    "build_related_feed",
    "normalize_related_filter",
    "normalize_related_limit",
]
