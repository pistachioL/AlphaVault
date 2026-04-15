from __future__ import annotations

FULLWIDTH_SPACE_CODEPOINT = 0x3000
FULLWIDTH_HYPHEN_CODEPOINT = 0xFF0D
FULLWIDTH_DIGIT_START = 0xFF10
FULLWIDTH_DIGIT_END = 0xFF19
FULLWIDTH_UPPER_START = 0xFF21
FULLWIDTH_UPPER_END = 0xFF3A
FULLWIDTH_LOWER_START = 0xFF41
FULLWIDTH_LOWER_END = 0xFF5A
FULLWIDTH_TO_HALFWIDTH_SHIFT = 0xFEE0


def _is_missing_value(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(value != value)
    except Exception:
        return False


def clean_stock_name_text(value: object) -> str:
    if _is_missing_value(value):
        return ""
    return str(value or "").strip()


def _normalize_stock_name_chars(text: str) -> str:
    normalized_chars: list[str] = []
    for char in text:
        codepoint = ord(char)
        if codepoint == FULLWIDTH_SPACE_CODEPOINT:
            normalized_chars.append(" ")
            continue
        if codepoint == FULLWIDTH_HYPHEN_CODEPOINT:
            normalized_chars.append("-")
            continue
        if FULLWIDTH_DIGIT_START <= codepoint <= FULLWIDTH_DIGIT_END:
            normalized_chars.append(chr(codepoint - FULLWIDTH_TO_HALFWIDTH_SHIFT))
            continue
        if FULLWIDTH_UPPER_START <= codepoint <= FULLWIDTH_UPPER_END:
            normalized_chars.append(chr(codepoint - FULLWIDTH_TO_HALFWIDTH_SHIFT))
            continue
        if FULLWIDTH_LOWER_START <= codepoint <= FULLWIDTH_LOWER_END:
            normalized_chars.append(chr(codepoint - FULLWIDTH_TO_HALFWIDTH_SHIFT))
            continue
        normalized_chars.append(char)
    return "".join(normalized_chars)


def normalize_stock_official_name(value: object) -> str:
    return "".join(_normalize_stock_name_chars(clean_stock_name_text(value)).split())


def normalize_stock_official_name_norm(value: object) -> str:
    return normalize_stock_official_name(value).casefold()


__all__ = [
    "clean_stock_name_text",
    "normalize_stock_official_name",
    "normalize_stock_official_name_norm",
]
