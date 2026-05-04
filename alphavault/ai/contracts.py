from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AiTopicPackage:
    topic_status_id: str
    focus_username: str
    message_tree: dict[str, Any]
    manual_feedback_hint: dict[str, object] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "topic_status_id": self.topic_status_id,
            "focus_username": self.focus_username,
            "message_tree": self.message_tree,
        }
        if self.manual_feedback_hint is not None:
            payload["manual_feedback_hint"] = self.manual_feedback_hint
        return payload


@dataclass(frozen=True)
class AiTopicRuntimeContext:
    root_key: str
    root_source_id: str
    focus_username: str
    message_tree: dict[str, Any]
    message_lookup: dict[tuple[str, str], dict[str, Any]]
    ai_topic_package: AiTopicPackage


@dataclass(frozen=True)
class AiAnalyzeInput:
    commentary_text: str
    quoted_text: str
    row: dict[str, str]


@dataclass(frozen=True)
class AiAssertionDraft:
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
class AiAnalyzeOutput:
    status: str
    invest_score: float
    assertions: tuple[AiAssertionDraft, ...]


__all__ = [
    "AiAnalyzeInput",
    "AiAnalyzeOutput",
    "AiAssertionDraft",
    "AiTopicPackage",
    "AiTopicRuntimeContext",
]
