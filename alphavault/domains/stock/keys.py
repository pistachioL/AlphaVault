from __future__ import annotations

import re

STOCK_KEY_PREFIX = "stock:"
_PREFIXED_CN_STOCK_VALUE_RE = re.compile(
    r"^(SH|SZ|BJ)(\d{6})(?:\.US)?$",
    re.IGNORECASE,
)


def _canonical_stock_value(stock_value: str) -> str:
    value = str(stock_value or "").strip()
    if not value:
        return ""
    matched = _PREFIXED_CN_STOCK_VALUE_RE.match(value)
    if matched is None:
        return value
    market, code = matched.groups()
    return f"{code}.{market.upper()}"


def canonical_stock_key(raw_key: str) -> str:
    value = str(raw_key or "").strip()
    if not value:
        return ""
    if not value.startswith(STOCK_KEY_PREFIX):
        return value
    stock_value = _canonical_stock_value(value[len(STOCK_KEY_PREFIX) :].strip())
    if not stock_value:
        return STOCK_KEY_PREFIX
    if "." in stock_value:
        code, market = stock_value.rsplit(".", 1)
        if code.isdigit() and market:
            return f"{STOCK_KEY_PREFIX}{code}.{market.upper()}"
    return f"{STOCK_KEY_PREFIX}{stock_value}"


def normalize_stock_key(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    key = text if text.startswith(STOCK_KEY_PREFIX) else f"{STOCK_KEY_PREFIX}{text}"
    return canonical_stock_key(key)


def stock_value(stock_key: str) -> str:
    key = str(stock_key or "").strip()
    if key.startswith(STOCK_KEY_PREFIX):
        return key[len(STOCK_KEY_PREFIX) :].strip()
    return key


def stock_key_lookup_candidates(value: str) -> list[str]:
    normalized = normalize_stock_key(value)
    if not normalized:
        return []
    out = [normalized]
    value_text = stock_value(normalized)
    if "." not in value_text:
        return out
    code, market = value_text.rsplit(".", 1)
    if not code.isdigit() or market not in {"SH", "SZ", "BJ"}:
        return out
    legacy_key = f"{STOCK_KEY_PREFIX}{market}{code}.US"
    if legacy_key not in out:
        out.append(legacy_key)
    return out


__all__ = [
    "STOCK_KEY_PREFIX",
    "canonical_stock_key",
    "normalize_stock_key",
    "stock_key_lookup_candidates",
    "stock_value",
]
