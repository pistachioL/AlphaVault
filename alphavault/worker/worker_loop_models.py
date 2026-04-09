from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import threading
from typing import Any, Callable, Optional

from alphavault.rss.utils import RateLimiter
from alphavault.worker.runtime_models import LLMConfig


@dataclass(frozen=True)
class SourceTickContext:
    args: Any
    config: LLMConfig
    limiter: RateLimiter
    redis_client: Any
    ai_cap: int
    limit_or_none: int | None
    stuck_seconds: int
    verbose: bool
    rss_active_hours: Optional[tuple[int, int]]
    rss_interval_seconds: float
    rss_feed_sleep_seconds: float
    worker_interval_seconds: float
    maintenance_next_at: float
    now: float
    do_maintenance: bool
    due_ai_pending_get: Callable[[], bool] | None


@dataclass(frozen=True)
class SourceTickExecutors:
    ai_executor: ThreadPoolExecutor
    stock_hot_executor: ThreadPoolExecutor
    rss_executor: ThreadPoolExecutor


@dataclass(frozen=True)
class SourceTickState:
    wakeup_event: threading.Event
    inflight_futures: set[Future]
    inflight_owner_by_future: dict[Future, str]


__all__ = [
    "SourceTickContext",
    "SourceTickExecutors",
    "SourceTickState",
]
