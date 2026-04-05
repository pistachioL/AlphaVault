"""
Thread tree parsing helpers.

Keep this module focused on:
- text normalization
- parsing repost/quote info from raw_text
- parsing the JSON object after '[CSV原始字段]'
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict

from alphavault.text.html import html_to_text
from alphavault.weibo.display import build_weibo_display_lines, strip_image_label_lines

CSV_RAW_FIELDS_MARKER = "[CSV原始字段]"
FORWARD_ORIGINAL_MARKER = "[转发原文]"
WEIBO_META_MARKER = "[微博元信息]"
REPOST_TOKEN = "转发 @"
MATCH_KEY_LEN = 80
THREAD_SEGMENT_SPLIT_RE = re.compile(r"(?m)^\s*---\s*$")

SYNTHETIC_SOURCE_ID_PREFIX = "src:"


def strip_csv_raw_fields(raw_text: str) -> str:
    text = str(raw_text or "")
    idx = text.find(CSV_RAW_FIELDS_MARKER)
    if idx < 0:
        return text.strip()
    return text[:idx].strip()


def _first_non_empty_line(text: str) -> str:
    raw = strip_csv_raw_fields(html_to_text(str(text or "")))
    for line in str(raw or "").splitlines():
        s = str(line).strip()
        if s:
            return s
    return str(raw or "").strip()


def _match_key(text: str) -> str:
    first_line = _first_non_empty_line(text)
    first_line = first_line.replace("\u00a0", " ")
    first_line = re.sub(r"\s+", " ", first_line).strip()
    if not first_line:
        return ""
    return first_line[:MATCH_KEY_LEN]


def _to_one_line_text(text: str) -> str:
    s = strip_csv_raw_fields(html_to_text(str(text or "")))
    s = strip_image_label_lines(s)
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _clean_thread_segment(text: str) -> str:
    """
    Clean a single thread-text segment.

    This is Weibo-specific, best-effort, and conservative:
    - Strip the noisy "[微博元信息]" tail if present
    - Handle "[转发原文]" by keeping comment or original text
    """
    s = str(text or "").strip()
    if not s:
        return ""

    meta_idx = s.find(WEIBO_META_MARKER)
    if meta_idx >= 0:
        s = s[:meta_idx].strip()

    forward_idx = s.find(FORWARD_ORIGINAL_MARKER)
    if forward_idx >= 0:
        before = s[:forward_idx].strip()
        after = s[forward_idx + len(FORWARD_ORIGINAL_MARKER) :].strip()
        s = before if before else after

    return strip_image_label_lines(s).strip()


def _parse_segmented_thread_text(thread_text: str) -> list[str]:
    text = html_to_text(str(thread_text or ""))
    if not text.strip():
        return []
    parts = THREAD_SEGMENT_SPLIT_RE.split(text)
    segments: list[str] = []
    for part in parts:
        seg = _to_one_line_text(str(part or ""))
        seg = _clean_thread_segment(seg)
        if seg:
            segments.append(seg)
    return segments


def _looks_like_compact_weibo_chain(text: str) -> bool:
    value = html_to_text(str(text or ""))
    if not value.strip():
        return False
    return (
        "//@" in value
        or "转发 @" in value
        or "转发@" in value
        or value.lstrip().startswith("回复@")
    )


def parse_thread_segments(
    thread_text: str,
    *,
    author: str = "",
    raw_text: str = "",
) -> list[str]:
    """
    Parse segmented thread text into tree-ready message segments.

    Prefer the stored '---' split format. If the content still looks like a
    compact Weibo reply/repost chain, rebuild it with the shared Weibo parser.
    """
    segments = _parse_segmented_thread_text(thread_text)
    candidate_text = str(raw_text or "").strip() or str(thread_text or "").strip()
    if not _looks_like_compact_weibo_chain(candidate_text):
        return segments
    rebuilt = build_weibo_display_lines(candidate_text, author=author)
    if len(rebuilt) > len(segments):
        return rebuilt
    return segments


def _strip_leading_speaker(text: str, *, author_hint: str = "") -> str:
    s = _to_one_line_text(text)
    author = str(author_hint or "").strip()
    if author:
        for sep in ["：", ":"]:
            prefix = author + sep
            if s.startswith(prefix):
                return s[len(prefix) :].lstrip()

    # Fallback: treat the first "xxx：" as speaker prefix.
    m = re.match(r"^[^：:]{1,20}[：:]\s*(.+)$", s)
    if m:
        return str(m.group(1) or "").strip()
    return s


def _content_key_for_compare(text: str, *, author_hint: str = "") -> str:
    content = _strip_leading_speaker(text, author_hint=author_hint)
    return _match_key(content)


def _extract_speaker_name(text: str) -> str:
    s = _to_one_line_text(text)
    m = re.match(r"^([^：:]{1,20})[：:]\s*", s)
    return str(m.group(1)).strip() if m else ""


def _make_synthetic_source_id(first_segment: str) -> str:
    speaker = _extract_speaker_name(first_segment)
    key = _content_key_for_compare(first_segment)
    base = f"{speaker}|{key}".encode("utf-8")
    digest = hashlib.md5(base).hexdigest()[:10]
    return f"{SYNTHETIC_SOURCE_ID_PREFIX}{digest}"


def _extract_forward_original_text(raw_text: str) -> str:
    text = str(raw_text or "")
    idx = text.find(FORWARD_ORIGINAL_MARKER)
    if idx < 0:
        return ""
    tail = text[idx + len(FORWARD_ORIGINAL_MARKER) :]
    stop = tail.find(CSV_RAW_FIELDS_MARKER)
    if stop >= 0:
        tail = tail[:stop]
    return tail.strip()


def _extract_repost_original_text(raw_text: str) -> tuple[str, str]:
    """
    Best-effort parse: "... 转发 @某人: 原文".
    Return (author_hint, original_text). If not found, return ("", "").
    """
    text = html_to_text(str(raw_text or ""))
    idx = text.rfind(REPOST_TOKEN)
    if idx < 0:
        return "", ""

    start = idx + len(REPOST_TOKEN)
    colon_idx = text.find(":", start)
    colon_cn_idx = text.find("：", start)
    candidates = [i for i in [colon_idx, colon_cn_idx] if i >= 0]
    if not candidates:
        return "", ""
    colon = min(candidates)

    author_hint = text[start:colon].strip().lstrip("@").strip()
    original_text = text[colon + 1 :].strip()
    return author_hint, original_text


def extract_platform_post_id(post_uid: object) -> str:
    value = str(post_uid or "").strip()
    if not value:
        return ""
    prefix, sep, suffix = value.partition(":")
    prefix = prefix.strip().lower()
    suffix = suffix.strip()
    if not sep:
        return value
    if prefix == "xueqiu":
        return suffix
    if suffix.startswith(("http://", "https://")):
        return suffix.strip()
    parts = value.split(":")
    if len(parts) >= 2:
        return parts[-1].strip()
    return value


def _is_escaped(text: str, idx: int) -> bool:
    if idx <= 0 or idx >= len(text):
        return False
    backslashes = 0
    i = idx - 1
    while i >= 0 and text[i] == "\\":
        backslashes += 1
        i -= 1
    return (backslashes % 2) == 1


def _extract_json_object_text(text: str, *, after_idx: int) -> str:
    start = text.find("{", max(0, int(after_idx)))
    if start < 0:
        return ""

    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == '"':
                in_string = False
            continue

        if ch == '"' and not _is_escaped(text, i):
            in_string = True
            continue
        if ch == "{":
            depth += 1
            continue
        if ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return ""


def parse_weibo_csv_raw_fields(raw_text: str) -> Dict[str, Any]:
    """
    Parse the JSON object after '[CSV原始字段]'.

    Best-effort:
    - Try json.loads directly (already valid JSON)
    - If failed, treat it as a JSON-string-escaped object and decode twice
    """
    text = str(raw_text or "")
    marker_idx = text.find(CSV_RAW_FIELDS_MARKER)
    if marker_idx < 0:
        return {}

    json_text = _extract_json_object_text(text, after_idx=marker_idx)
    if not json_text.strip():
        return {}

    candidate = json_text.strip()
    try:
        obj = json.loads(candidate)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass

    # Weibo CSV raw fields often escape every quote like: {\"id\": \"...\"}
    # That is not a valid JSON object, but it is valid JSON string content.
    try:
        wrapped = '"' + candidate.replace("\r", "\\r").replace("\n", "\\n") + '"'
        unescaped = json.loads(wrapped)
        obj = json.loads(unescaped)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _clean_id(value: object) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    s = s.replace("\t", "")
    s = re.sub(r"(?:\\+t)+$", "", s)
    return s.strip()


def extract_parent_post_id(*, csv_fields: Dict[str, Any]) -> str:
    return _clean_id(csv_fields.get("源微博id"))


clean_id = _clean_id
content_key_for_compare = _content_key_for_compare
extract_forward_original_text = _extract_forward_original_text
extract_repost_original_text = _extract_repost_original_text
extract_speaker_name = _extract_speaker_name
make_synthetic_source_id = _make_synthetic_source_id
match_key = _match_key
strip_leading_speaker = _strip_leading_speaker
to_one_line_text = _to_one_line_text


__all__ = [
    "CSV_RAW_FIELDS_MARKER",
    "FORWARD_ORIGINAL_MARKER",
    "MATCH_KEY_LEN",
    "REPOST_TOKEN",
    "SYNTHETIC_SOURCE_ID_PREFIX",
    "THREAD_SEGMENT_SPLIT_RE",
    "clean_id",
    "content_key_for_compare",
    "extract_forward_original_text",
    "extract_parent_post_id",
    "extract_repost_original_text",
    "extract_speaker_name",
    "extract_platform_post_id",
    "make_synthetic_source_id",
    "match_key",
    "parse_thread_segments",
    "parse_weibo_csv_raw_fields",
    "strip_leading_speaker",
    "strip_csv_raw_fields",
    "to_one_line_text",
]
