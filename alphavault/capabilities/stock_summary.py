from __future__ import annotations


from alphavault.ai.stock_summary import (
    StockAiSummary,
    empty_stock_ai_summary,
    summarize_stock_evidence_pack,
)
from alphavault.capabilities.stock_analysis import (
    DEFAULT_STOCK_EVIDENCE_MAX_POSTS,
    DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
    StockEvidencePack,
    get_stock_evidence_pack,
)


class StockSummaryResult(StockEvidencePack):
    summary: StockAiSummary


def get_stock_summary(
    stock: str,
    *,
    window_days: int = DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
    max_posts: int = DEFAULT_STOCK_EVIDENCE_MAX_POSTS,
) -> StockSummaryResult:
    evidence_pack = get_stock_evidence_pack(
        stock,
        window_days=window_days,
        max_posts=max_posts,
    )
    if str(evidence_pack.get("load_error") or "").strip():
        return {
            **evidence_pack,
            "summary": empty_stock_ai_summary(),
        }
    return {
        **evidence_pack,
        "summary": summarize_stock_evidence_pack(evidence_pack),
    }


__all__ = ["StockAiSummary", "StockSummaryResult", "get_stock_summary"]
