from __future__ import annotations


STOCK_KEY_PREFIX = "stock:"


def canonical_stock_key(raw_key: str) -> str:
    value = str(raw_key or "").strip()
    if not value:
        return ""
    if not value.startswith(STOCK_KEY_PREFIX):
        return value
    stock_value = value[len(STOCK_KEY_PREFIX) :].strip()
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


__all__ = [
    "STOCK_KEY_PREFIX",
    "canonical_stock_key",
    "normalize_stock_key",
    "stock_value",
]
