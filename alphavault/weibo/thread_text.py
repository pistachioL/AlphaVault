"""
Weibo thread-text normalization helpers.

This project stores the normalized root-to-current thread text in posts.raw_text.

Goal: convert noisy Weibo reply/repost chains into readable thread text:
- "speaker：text"
- segments separated by '---'
- optional "[图片] URL" lines for images
"""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any, Iterable, List, Optional


CSV_RAW_FIELDS_MARKER = "[CSV原始字段]"
WEIBO_META_MARKER = "[微博元信息]"
FORWARD_ORIGINAL_MARKER = "[转发原文]"

QUOTE_MARKER = "//@"
SEGMENT_SEPARATOR = "\n\n---\n\n"
DEFAULT_UNKNOWN_AUTHOR = "未知"
IMAGE_LABEL_PREFIX = "[图片]"
IMAGE_LINE_TEMPLATE = f"{IMAGE_LABEL_PREFIX} {{url}}"
REPOST_ONLY_TEXT = "转发"

_MAX_HTML_UNESCAPE_PASSES = 2

_RE_REPLY_PREFIX = re.compile(r"^\s*回复@[^:：\s]+[:：]\s*")
_RE_TRAILING_DASH = re.compile(r"[\s\-–—]+$")
_RE_REPOST_MARKER = re.compile(
    r"(?:^|[\s\n])(?:-\s*)?转发\s*@(?P<nick>[^:：\s]+)\s*[:：]\s*",
    re.MULTILINE,
)

_DUPLICATE_BLOCK_SPLIT_RE = re.compile(r"\n{2,}")
_DUPLICATE_KEY_MIN_RATIO = 0.85


def _unescape_html_entities(text: str) -> str:
    s = str(text or "")
    for _ in range(_MAX_HTML_UNESCAPE_PASSES):
        unescaped = html.unescape(s)
        if unescaped == s:
            break
        s = unescaped
    return s


def _strip_csv_raw_fields(raw_text: str) -> str:
    text = str(raw_text or "")
    idx = text.find(CSV_RAW_FIELDS_MARKER)
    if idx < 0:
        return text.strip()
    return text[:idx].strip()


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


def _is_escaped(text: str, idx: int) -> bool:
    if idx <= 0 or idx >= len(text):
        return False
    backslashes = 0
    cursor = idx - 1
    while cursor >= 0 and text[cursor] == "\\":
        backslashes += 1
        cursor -= 1
    return (backslashes % 2) == 1


def _extract_json_object_text(text: str, *, after_idx: int) -> str:
    start = text.find("{", max(0, int(after_idx)))
    if start < 0:
        return ""

    depth = 0
    in_string = False
    escape = False
    for idx in range(start, len(text)):
        ch = text[idx]
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

        if ch == '"' and not _is_escaped(text, idx):
            in_string = True
            continue
        if ch == "{":
            depth += 1
            continue
        if ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]
    return ""


def _parse_weibo_csv_raw_fields(raw_text: str) -> dict[str, Any]:
    text = str(raw_text or "")
    marker_idx = text.find(CSV_RAW_FIELDS_MARKER)
    if marker_idx < 0:
        return {}

    json_text = _extract_json_object_text(text, after_idx=marker_idx)
    candidate = json_text.strip()
    if not candidate:
        return {}

    try:
        obj = json.loads(candidate)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass

    try:
        wrapped = '"' + candidate.replace("\r", "\\r").replace("\n", "\\n") + '"'
        unescaped = json.loads(wrapped)
        obj = json.loads(unescaped)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _extract_forward_original_segment(raw_text: str) -> Optional["WeiboDisplaySegment"]:
    csv_fields = _parse_weibo_csv_raw_fields(raw_text)
    source_text = normalize_weibo_text(
        str(
            csv_fields.get("源微博正文")
            or _extract_forward_original_text(raw_text)
            or ""
        )
    )
    source_image_urls = _extract_csv_image_urls(csv_fields.get("源微博原始图片url"))
    if not source_text and not source_image_urls:
        return None

    speaker = normalize_weibo_text(str(csv_fields.get("源用户昵称") or "")).strip()
    speaker = speaker or DEFAULT_UNKNOWN_AUTHOR
    return WeiboDisplaySegment(
        speaker=speaker,
        text=source_text,
        is_current=False,
        image_urls=source_image_urls,
    )


def _strip_weibo_trailing_meta_sections(text: str) -> str:
    """
    Remove noisy trailer sections that should not join reply-chain segments.

    For many repost texts, "[微博元信息]" / "[转发原文]" appears after the compact
    "//@" chain. Keeping that trailer causes the oldest quoted segment to be
    polluted with metadata and original-content blocks.
    """
    value = str(text or "")
    if not value:
        return ""

    cut_positions: list[int] = []
    for marker in (WEIBO_META_MARKER, FORWARD_ORIGINAL_MARKER):
        idx = value.find(marker)
        if idx > 0:
            cut_positions.append(idx)

    if not cut_positions:
        return value
    return value[: min(cut_positions)].rstrip()


def strip_image_label_lines(text: str) -> str:
    lines: list[str] = []
    for raw_line in str(text or "").splitlines():
        line = str(raw_line or "").strip()
        if line.startswith(IMAGE_LABEL_PREFIX):
            continue
        lines.append(str(raw_line or ""))
    return "\n".join(lines).strip()


def normalize_weibo_text(text: str) -> str:
    value = _unescape_html_entities(strip_image_label_lines(text))
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    for ch in ("\u00a0", "\u2002", "\u2003", "\u2009", "\u202f", "\ufeff"):
        value = value.replace(ch, " ")
    value = re.sub(r"[ \t]{2,}", " ", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def _maybe_dedupe_repeated_blocks(text: str) -> str:
    """
    Some raw_text becomes: "title\\n\\ncontent" or even "content\\n\\ncontent".
    That can break reply-chain parsing and cause duplicated segments.

    Keep it conservative: only dedupe when there are exactly 2 blocks and they
    are near-identical (after removing whitespace).
    """
    s = str(text or "").strip()
    if not s:
        return ""

    parts = _DUPLICATE_BLOCK_SPLIT_RE.split(s)
    if len(parts) != 2:
        return s

    left = str(parts[0] or "").strip()
    right = str(parts[1] or "").strip()
    if not left or not right:
        return s

    left_key = re.sub(r"\s+", "", left)
    right_key = re.sub(r"\s+", "", right)
    if not left_key or not right_key:
        return s

    if left_key == right_key:
        return right

    if left_key in right_key:
        ratio = len(left_key) / max(1, len(right_key))
        if ratio >= _DUPLICATE_KEY_MIN_RATIO:
            return right

    if right_key in left_key:
        ratio = len(right_key) / max(1, len(left_key))
        if ratio >= _DUPLICATE_KEY_MIN_RATIO:
            return left

    return s


@dataclass(frozen=True)
class WeiboDisplaySegment:
    speaker: str
    text: str
    is_current: bool
    image_urls: tuple[str, ...] = ()


def _extract_csv_image_urls(value: Any) -> tuple[str, ...]:
    raw = str(value or "").strip()
    if not raw:
        return ()
    parts = [
        str(part or "").strip()
        for part in re.split(r"[\r\n,]+", raw)
        if str(part or "").strip()
    ]
    return tuple(_dedup_keep_order(parts))


def _strip_reply_prefix(text: str) -> str:
    return _RE_REPLY_PREFIX.sub("", (text or "").strip()).strip()


def _strip_leading_at_mention(text: str, *, target: str) -> str:
    """
    Remove a redundant leading "@target" mention at the start of a segment.

    Weibo comments often start with "@上一段说话人 ..." which is visually noisy when we
    already render segments as "speaker：text".
    Keep it conservative: only strip when it is an exact prefix, and only at start.
    """
    s = str(text or "")
    t = str(target or "").strip()
    if not s or not t:
        return s

    s2 = s.lstrip()
    prefix = "@" + t
    if not s2.startswith(prefix):
        return s

    rest = s2[len(prefix) :]
    if not rest:
        return s

    # Accept either whitespace or ":" / "：" after the mention.
    if rest[0] in (":", "："):
        rest = rest[1:]
    elif not rest[0].isspace():
        return s

    out = rest.lstrip()
    return out if out else s


def _split_nick_and_text(value: str) -> tuple[str, str]:
    """
    Parse "昵称:内容" or "昵称：内容".
    Returns ("", value) when separator not found.
    """
    raw = (value or "").strip()
    if not raw:
        return "", ""
    for sep in (":", "："):
        idx = raw.find(sep)
        if idx > 0:
            nick = raw[:idx].strip()
            content = raw[idx + 1 :].strip()
            return nick, content
    return "", raw


def parse_weibo_reply_chain(
    raw_text: str, *, default_author: str
) -> List[WeiboDisplaySegment]:
    """
    Convert a Weibo-style reply/repost chain to ordered segments (oldest -> newest).

    Example input:
      "回复@A:有点怕//@A:公公真不怕？//@B:原文"
    Output order:
      B -> A -> default_author
    """
    text = normalize_weibo_text(_strip_csv_raw_fields(raw_text))
    text = _strip_weibo_trailing_meta_sections(text)
    text = _maybe_dedupe_repeated_blocks(text)
    if not text:
        return []

    parts = text.split(QUOTE_MARKER)
    if not parts:
        return []

    current_text = normalize_weibo_text(_strip_reply_prefix(parts[0]))

    quoted_segments: List[WeiboDisplaySegment] = []
    for part in parts[1:]:
        item = (part or "").strip()
        if not item:
            continue
        nick, content = _split_nick_and_text(item)
        content = normalize_weibo_text(_strip_reply_prefix(content))
        speaker = (
            (nick or "").strip()
            or (default_author or "").strip()
            or DEFAULT_UNKNOWN_AUTHOR
        )
        if not content:
            continue
        quoted_segments.append(
            WeiboDisplaySegment(speaker=speaker, text=content, is_current=False)
        )

    ordered: List[WeiboDisplaySegment] = list(reversed(quoted_segments))
    if current_text:
        speaker = (default_author or "").strip() or DEFAULT_UNKNOWN_AUTHOR
        ordered.append(
            WeiboDisplaySegment(speaker=speaker, text=current_text, is_current=True)
        )
    elif quoted_segments:
        # For compact pure forwards like "//@A:内容", keep the current repost node.
        speaker = (default_author or "").strip() or DEFAULT_UNKNOWN_AUTHOR
        ordered.append(
            WeiboDisplaySegment(
                speaker=speaker,
                text=REPOST_ONLY_TEXT,
                is_current=True,
            )
        )

    if not ordered and text:
        speaker = (default_author or "").strip() or DEFAULT_UNKNOWN_AUTHOR
        ordered.append(WeiboDisplaySegment(speaker=speaker, text=text, is_current=True))

    return ordered


class _ImgSrcExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.urls: List[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if str(tag or "").lower() != "img":
            return
        attr_map = {str(k or "").lower(): (v or "") for k, v in attrs}
        url = (attr_map.get("src") or "").strip()
        if not url:
            url = (
                attr_map.get("data-src") or attr_map.get("data-original") or ""
            ).strip()
        if not url:
            return
        if url.startswith("//"):
            url = "https:" + url
        self.urls.append(url)


def extract_image_urls_from_html(content_html: str) -> List[str]:
    """
    Extract image urls from RSS HTML content.
    This is a best-effort helper (no heavy dependencies).
    """
    value = (content_html or "").strip()
    if not value:
        return []
    parser = _ImgSrcExtractor()
    try:
        parser.feed(value)
        parser.close()
    except Exception:
        return []
    return _dedup_keep_order(parser.urls)


def _dedup_keep_order(items: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for item in items:
        value = str(item or "").strip()
        if not value:
            continue
        if value.startswith("//"):
            value = "https:" + value
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _extract_repost(text: str) -> Optional[tuple[str, str, str]]:
    """
    Best-effort parse:
      "评论 ... - 转发 @A: 原文"
      "转发@A：原文"
    Return: (comment_text, nick, original_text)
    """
    value = normalize_weibo_text(text)
    if not value:
        return None

    match = _RE_REPOST_MARKER.search(value)
    if not match:
        return None

    marker_idx = match.start() + str(match.group(0) or "").find("转发")
    comment_text = value[:marker_idx].rstrip()
    comment_text = _RE_TRAILING_DASH.sub("", comment_text).strip()
    nick = str(match.group("nick") or "").strip()
    original_text = value[match.end() :].strip()
    if not nick or not original_text:
        return None
    return comment_text, nick, original_text


def _expand_repost_segments(
    seg: WeiboDisplaySegment, *, max_depth: int
) -> List[WeiboDisplaySegment]:
    if max_depth <= 0:
        return [seg]

    extracted = _extract_repost(seg.text)
    if not extracted:
        return [seg]

    comment_text, nick, original_text = extracted

    original_seg = WeiboDisplaySegment(
        speaker=nick, text=original_text, is_current=False
    )
    expanded_original = _expand_repost_segments(original_seg, max_depth=max_depth - 1)

    speaker = (seg.speaker or "").strip() or DEFAULT_UNKNOWN_AUTHOR
    retweeter_text = (comment_text or "").rstrip() or REPOST_ONLY_TEXT
    retweeter_seg = WeiboDisplaySegment(
        speaker=speaker, text=retweeter_text, is_current=seg.is_current
    )

    return expanded_original + [retweeter_seg]


def _sanitize_visible_text(text: str) -> str:
    s = normalize_weibo_text(text)
    return s.replace("<", "&lt;").replace(">", "&gt;")


def _build_normalized_segments(
    raw_text: str,
    *,
    author: str = "",
    max_depth: int = 3,
) -> List[WeiboDisplaySegment]:
    segments = parse_weibo_reply_chain(
        raw_text, default_author=str(author or "").strip()
    )
    if not segments:
        return []

    expanded_segments: List[WeiboDisplaySegment] = []
    for seg in segments:
        expanded_segments.extend(_expand_repost_segments(seg, max_depth=max_depth))

    source_segment = _extract_forward_original_segment(raw_text)
    if source_segment is not None:
        first_segment = expanded_segments[0] if expanded_segments else None
        source_text = normalize_weibo_text(source_segment.text or "")
        source_images = tuple(_dedup_keep_order(source_segment.image_urls))
        first_text = (
            normalize_weibo_text(first_segment.text or "") if first_segment else ""
        )
        first_images = (
            tuple(_dedup_keep_order(first_segment.image_urls))
            if first_segment is not None
            else ()
        )
        if (source_text or source_images) and (
            source_text != first_text or source_images != first_images
        ):
            expanded_segments = [source_segment] + expanded_segments

    normalized_segments: List[WeiboDisplaySegment] = []
    prev_speaker = ""
    for seg in expanded_segments:
        speaker = (seg.speaker or "").strip() or DEFAULT_UNKNOWN_AUTHOR
        text = normalize_weibo_text(seg.text or "")
        image_urls = tuple(_dedup_keep_order(seg.image_urls))
        if not text and not image_urls:
            continue
        if prev_speaker:
            text = _strip_leading_at_mention(text, target=prev_speaker)
        normalized_segments.append(
            WeiboDisplaySegment(
                speaker=speaker,
                text=text,
                is_current=seg.is_current,
                image_urls=image_urls,
            )
        )
        prev_speaker = speaker
    return normalized_segments


def format_weibo_thread_text(
    raw_text: str,
    *,
    author: str = "",
    image_urls: Optional[Iterable[str]] = None,
) -> str:
    images = _dedup_keep_order(image_urls or [])
    img_lines = [IMAGE_LINE_TEMPLATE.format(url=url) for url in images]
    normalized_segments = _build_normalized_segments(
        raw_text,
        author=str(author or "").strip(),
        max_depth=3,
    )
    if not normalized_segments:
        return "\n".join(img_lines).strip()

    blocks: List[str] = []
    for seg in normalized_segments:
        speaker = (seg.speaker or "").strip() or DEFAULT_UNKNOWN_AUTHOR
        text = normalize_weibo_text(seg.text or "")
        seg_img_lines = [
            IMAGE_LINE_TEMPLATE.format(url=url)
            for url in _dedup_keep_order(seg.image_urls)
        ]
        if not text and not seg_img_lines:
            continue

        if text:
            block = f"{_sanitize_visible_text(speaker)}：{_sanitize_visible_text(text)}"
            if seg_img_lines:
                block = block.rstrip() + "\n" + "\n".join(seg_img_lines)
        else:
            first_img = seg_img_lines[0]
            rest_imgs = seg_img_lines[1:]
            block = f"{_sanitize_visible_text(speaker)}：{first_img}"
            if rest_imgs:
                block = block.rstrip() + "\n" + "\n".join(rest_imgs)

        if seg.is_current and img_lines:
            block = block.rstrip() + "\n" + "\n".join(img_lines)

        blocks.append(block)

    if blocks and img_lines and not any(seg.is_current for seg in normalized_segments):
        blocks[-1] = blocks[-1].rstrip() + "\n" + "\n".join(img_lines)

    return SEGMENT_SEPARATOR.join(blocks).strip()
