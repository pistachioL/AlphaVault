from __future__ import annotations

import json
import re

from alphavault.domains.stock.key_match import is_stock_code_value, normalize_stock_code

ASSERTION_ENTITY_TYPE_STOCK = "stock"
_ENTITY_PREFIX_BY_MENTION_TYPE = {
    "industry_name": "industry",
    "commodity_name": "commodity",
    "index_name": "index",
    "keyword": "keyword",
}
_RAW_CN_STOCK_CODE_RE = re.compile(r"^\d{6}$")
_RAW_HK_STOCK_CODE_RE = re.compile(r"^\d{4,5}$")
_RAW_US_STOCK_CODE_RE = re.compile(r"^[A-Z][A-Z0-9]{0,9}$")


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _clamp_confidence(value: object) -> float:
    if isinstance(value, bool):
        number = float(int(value))
        return max(0.0, min(number, 1.0))
    if isinstance(value, (int, float)):
        number = float(value)
        return max(0.0, min(number, 1.0))
    try:
        number = float(str(value or "").strip() or "0")
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(number, 1.0))


def _guess_cn_market(code: str) -> str:
    if not code:
        return ""
    first = code[0]
    if first in {"5", "6", "9"}:
        return "SH"
    if first in {"0", "1", "2", "3"}:
        return "SZ"
    if first in {"4", "8"}:
        return "BJ"
    return ""


def coerce_stock_code_entity_key(value: object) -> str:
    raw = _clean_text(value).upper()
    if not raw:
        return ""
    if is_stock_code_value(raw):
        return f"stock:{normalize_stock_code(raw)}"
    if _RAW_CN_STOCK_CODE_RE.match(raw):
        market = _guess_cn_market(raw)
        if market:
            return f"stock:{raw}.{market}"
    if _RAW_HK_STOCK_CODE_RE.match(raw):
        return f"stock:{raw}.HK"
    if _RAW_US_STOCK_CODE_RE.match(raw):
        return f"stock:{raw}.US"
    return ""


def _normalize_assertion_entities(value: object) -> list[dict[str, object]]:
    if isinstance(value, list):
        raw_items = value
    else:
        text = _clean_text(value)
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        raw_items = parsed

    out: list[dict[str, object]] = []
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            continue
        entity_key = _clean_text(raw_item.get("entity_key"))
        entity_type = _clean_text(raw_item.get("entity_type"))
        if not entity_key or not entity_type:
            continue
        out.append(
            {
                "entity_key": entity_key,
                "entity_type": entity_type,
                "source_mention_text": _clean_text(raw_item.get("source_mention_text")),
                "source_mention_type": _clean_text(raw_item.get("source_mention_type")),
                "confidence": _clamp_confidence(raw_item.get("confidence")),
            }
        )
    return out


def build_assertion_entities(
    assertion_mentions: list[dict[str, object]],
) -> list[dict[str, object]]:
    cleaned_mentions = [
        {
            "mention_text": _clean_text(item.get("mention_text")),
            "mention_type": _clean_text(item.get("mention_type")),
            "confidence": _clamp_confidence(item.get("confidence")),
        }
        for item in assertion_mentions
        if isinstance(item, dict)
    ]
    cleaned_mentions = [
        item
        for item in cleaned_mentions
        if str(item.get("mention_text") or "").strip()
        and str(item.get("mention_type") or "").strip()
    ]
    if not cleaned_mentions:
        return []

    stock_code_keys = [
        coerce_stock_code_entity_key(item.get("mention_text"))
        for item in cleaned_mentions
        if str(item.get("mention_type") or "").strip() == "stock_code"
    ]
    unique_stock_code_keys = [
        key for key in dict.fromkeys(stock_code_keys) if str(key or "").strip()
    ]
    single_stock_code_key = (
        unique_stock_code_keys[0] if len(unique_stock_code_keys) == 1 else ""
    )

    stock_name_values = [
        _clean_text(item.get("mention_text"))
        for item in cleaned_mentions
        if str(item.get("mention_type") or "").strip() == "stock_name"
    ]
    unique_stock_name_values = [
        value for value in dict.fromkeys(stock_name_values) if str(value or "").strip()
    ]
    single_stock_name_key = (
        f"stock:{unique_stock_name_values[0]}"
        if len(unique_stock_name_values) == 1
        else ""
    )

    out: list[dict[str, object]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for item in cleaned_mentions:
        mention_text = _clean_text(item.get("mention_text"))
        mention_type = _clean_text(item.get("mention_type"))
        confidence = _clamp_confidence(item.get("confidence"))
        if not mention_text or not mention_type:
            continue

        entity_key = ""
        entity_type = ""
        if mention_type == "stock_code":
            entity_key = coerce_stock_code_entity_key(mention_text)
            entity_type = ASSERTION_ENTITY_TYPE_STOCK if entity_key else ""
        elif mention_type == "stock_name":
            entity_key = single_stock_code_key or f"stock:{mention_text}"
            entity_type = ASSERTION_ENTITY_TYPE_STOCK
        elif mention_type == "stock_alias":
            entity_key = single_stock_code_key or single_stock_name_key
            entity_type = ASSERTION_ENTITY_TYPE_STOCK if entity_key else ""
        elif mention_type in _ENTITY_PREFIX_BY_MENTION_TYPE:
            entity_type = _ENTITY_PREFIX_BY_MENTION_TYPE[mention_type]
            entity_key = f"{entity_type}:{mention_text}"
        if not entity_key or not entity_type:
            continue

        dedupe_key = (entity_key, entity_type, mention_text, mention_type)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(
            {
                "entity_key": entity_key,
                "entity_type": entity_type,
                "source_mention_text": mention_text,
                "source_mention_type": mention_type,
                "confidence": confidence,
            }
        )
    return out


def extract_stock_entity_keys(assertions: list[dict[str, object]]) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for raw_assertion in assertions:
        if not isinstance(raw_assertion, dict):
            continue
        entities = _normalize_assertion_entities(
            raw_assertion.get("assertion_entities")
        )
        for entity in entities:
            entity_key = _clean_text(entity.get("entity_key"))
            entity_type = _clean_text(entity.get("entity_type"))
            if entity_type != ASSERTION_ENTITY_TYPE_STOCK:
                continue
            if not entity_key.startswith("stock:") or entity_key in seen:
                continue
            seen.add(entity_key)
            keys.append(entity_key)
    keys.sort()
    return keys


__all__ = [
    "ASSERTION_ENTITY_TYPE_STOCK",
    "build_assertion_entities",
    "coerce_stock_code_entity_key",
    "extract_stock_entity_keys",
]
