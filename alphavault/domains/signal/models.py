from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Assertion:
    post_uid: str
    idx: int
    entity_key: str
    action: str
    action_strength: int
    summary: str
    evidence: str
    confidence: float
    stock_codes: tuple[str, ...]
    stock_names: tuple[str, ...]
    industries: tuple[str, ...]
    commodities: tuple[str, ...]
    indices: tuple[str, ...]
    cluster_keys: tuple[str, ...]
    keywords: tuple[str, ...]
    author: str
    created_at: datetime | str
    url: str
    raw_text: str
    source: str
    resolved_entity_key: str = ""
    stock_key: str = ""


@dataclass(frozen=True)
class Signal:
    post_uid: str
    entity_key: str
    resolved_entity_key: str
    stock_key: str
    action: str
    action_strength: int
    summary: str
    evidence: str
    confidence: float
    author: str
    created_at: datetime | str
    url: str
    raw_text: str
    cluster_keys: tuple[str, ...]
    tree_label: str = ""
    tree_text: str = ""


__all__ = ["Assertion", "Signal"]
