from __future__ import annotations

STOCK_VIEW_SCOPE_SECURITY = "security"
STOCK_VIEW_SCOPE_COMPANY = "company"
DEFAULT_STOCK_VIEW_SCOPE = STOCK_VIEW_SCOPE_SECURITY


def normalize_stock_view_scope(value: object) -> str:
    text = str(value or "").strip().lower()
    if text == STOCK_VIEW_SCOPE_COMPANY:
        return STOCK_VIEW_SCOPE_COMPANY
    return STOCK_VIEW_SCOPE_SECURITY


__all__ = [
    "DEFAULT_STOCK_VIEW_SCOPE",
    "STOCK_VIEW_SCOPE_COMPANY",
    "STOCK_VIEW_SCOPE_SECURITY",
    "normalize_stock_view_scope",
]
