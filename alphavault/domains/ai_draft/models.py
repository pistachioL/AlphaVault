from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AssertionDraft:
    topic_key: str
    action: str
    action_strength: int
    summary: str
    evidence: str
    confidence: float
    source_type: str
    stock_codes: tuple[str, ...]
    stock_names: tuple[str, ...]
    industries: tuple[str, ...]
    commodities: tuple[str, ...]
    indices: tuple[str, ...]


@dataclass(frozen=True)
class AnalyzeDecision:
    status: str
    invest_score: float


__all__ = ["AnalyzeDecision", "AssertionDraft"]
