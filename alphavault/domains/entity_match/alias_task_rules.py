from __future__ import annotations

import re

ALIAS_KEY_PREFIX = "stock:"
RULE_BLOCK_REASON_PREFIX = "规则屏蔽："

_FUND_ALIAS_RE = re.compile(r"(ETF|LOF|REIT|基金)", re.IGNORECASE)
_PRODUCT_ALIAS_RE = re.compile(
    r"(策略|量化|轮动|增强|通宝|\([Pp]\d{5,}\)|\b[Pp]\d{5,}\b)",
    re.IGNORECASE,
)
_MARKET_BUCKET_ALIAS_RE = re.compile(
    r"(A股|港股|美股|韩股|日股|A/H|(?<![A-Za-z])AH(?![A-Za-z]))",
    re.IGNORECASE,
)

_BLOCK_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (_FUND_ALIAS_RE, "基金类条目不进入股票简称人工确认"),
    (_PRODUCT_ALIAS_RE, "策略产品类条目不进入股票简称人工确认"),
    (_MARKET_BUCKET_ALIAS_RE, "市场篮子类条目不进入股票简称人工确认"),
)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def alias_text_from_key(value: object) -> str:
    alias_key = _clean_text(value)
    if alias_key.startswith(ALIAS_KEY_PREFIX):
        return alias_key[len(ALIAS_KEY_PREFIX) :].strip()
    return alias_key


def blocked_alias_task_reason(value: object) -> str:
    alias_text = alias_text_from_key(value)
    if not alias_text:
        return ""
    for pattern, reason in _BLOCK_RULES:
        if pattern.search(alias_text):
            return f"{RULE_BLOCK_REASON_PREFIX}{reason}"
    return ""


__all__ = [
    "ALIAS_KEY_PREFIX",
    "RULE_BLOCK_REASON_PREFIX",
    "alias_text_from_key",
    "blocked_alias_task_reason",
]
