from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Post:
    post_uid: str
    platform: str
    platform_post_id: str
    author: str
    created_at: datetime | str
    url: str
    raw_text: str
    status: str
    invest_score: float
    processed_at: datetime | str
    source: str = ""


__all__ = ["Post"]
