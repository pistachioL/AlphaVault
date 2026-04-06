from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import cast

from alphavault.worker import worker_loop_low_priority
from alphavault.worker.worker_loop_models import SourceTickContext, SourceTickExecutors


def test_schedule_low_priority_jobs_skips_alias_and_relation_tasks(
    monkeypatch,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        worker_loop_low_priority,
        "_build_should_continue_low_priority",
        lambda **_kwargs: (lambda: True),
    )
    monkeypatch.setattr(
        worker_loop_low_priority,
        "_schedule_backfill_cache",
        lambda **_kwargs: calls.append("backfill"),
    )
    monkeypatch.setattr(
        worker_loop_low_priority,
        "_schedule_stock_hot_cache",
        lambda **_kwargs: calls.append("stock_hot"),
    )
    monkeypatch.setattr(
        worker_loop_low_priority,
        "resolve_stock_hot_cache_interval_seconds",
        lambda: 300.0,
    )

    worker_loop_low_priority.schedule_low_priority_jobs(
        source=SimpleNamespace(),
        active_engine=object(),
        ctx=cast(SourceTickContext, SimpleNamespace(ai_cap=1)),
        execs=cast(SourceTickExecutors, SimpleNamespace()),
        wakeup_event=threading.Event(),
        inflight_futures=set(),
    )

    assert calls == ["backfill", "stock_hot"]
