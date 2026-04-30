from __future__ import annotations

from functools import lru_cache
import re
import unicodedata

from opencc import OpenCC

_WHITESPACE_RE = re.compile(r"\s+")
_SINGLE_CJK_RE = re.compile(r"^[\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]$")


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _raw_text(value: object) -> str:
    return str(value or "")


def _normalize_base_text(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = _WHITESPACE_RE.sub(" ", text).strip()
    if not text:
        return ""
    return text.casefold()


@lru_cache(maxsize=4)
def _opencc_converter(config: str) -> OpenCC:
    return OpenCC(config)


def to_simplified_text(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    return _opencc_converter("t2s").convert(text)


def to_traditional_text(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    return _opencc_converter("s2t").convert(text)


def normalize_search_text(value: object) -> str:
    base_text = _normalize_base_text(value)
    if not base_text:
        return ""
    return _normalize_base_text(to_simplified_text(base_text))


def normalize_literal_search_text(value: object) -> str:
    return _normalize_base_text(value)


def normalize_compact_search_text(value: object) -> str:
    return normalize_search_text(value).replace(" ", "")


def build_sparse_search_text(value: object) -> str:
    raw_text = _raw_text(value)
    if not raw_text.strip():
        return ""
    simplified_text = _opencc_converter("t2s").convert(raw_text)
    if simplified_text == raw_text:
        return ""
    return simplified_text


def build_exact_search_variants(value: object) -> list[str]:
    variants = [
        normalize_literal_search_text(value),
        normalize_search_text(value),
        _normalize_base_text(to_traditional_text(value)),
    ]
    out: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        if not variant or variant in seen:
            continue
        seen.add(variant)
        out.append(variant)
    return out


def is_single_cjk_query(value: object) -> bool:
    compact_text = _clean_text(value).replace(" ", "")
    if not compact_text:
        return False
    compact_text = unicodedata.normalize("NFKC", compact_text)
    return bool(_SINGLE_CJK_RE.fullmatch(compact_text))


__all__ = [
    "build_exact_search_variants",
    "build_sparse_search_text",
    "is_single_cjk_query",
    "normalize_compact_search_text",
    "normalize_literal_search_text",
    "normalize_search_text",
    "to_simplified_text",
    "to_traditional_text",
]
