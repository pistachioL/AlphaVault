from __future__ import annotations

from types import SimpleNamespace
from typing import cast

from alphavault.worker import worker_loop_source_tick
from alphavault.worker.worker_loop_models import SourceTickContext


def test_ensure_local_cache_ready_rebuilds_from_outbox_on_success(monkeypatch) -> None:
    source = SimpleNamespace(
        local_cache_ready=False,
        local_cache_rebuild_next_at=0.0,
    )
    ctx = cast(SourceTickContext, SimpleNamespace(now=100.0, verbose=False))
    calls: list[tuple[object, str]] = []

    def _fake_rebuild(
        engine: object, *, source_name: str, verbose: bool
    ) -> dict[str, int | bool]:
        del verbose
        calls.append((engine, source_name))
        return {"processed": 2, "inserted": 3, "cursor": 9, "has_error": False}

    monkeypatch.setattr(
        worker_loop_source_tick,
        "rebuild_local_cache_from_outbox",
        _fake_rebuild,
    )

    worker_loop_source_tick._ensure_local_cache_ready(
        source=source,
        active_engine=object(),
        source_name="weibo",
        ctx=ctx,
    )

    assert calls == [(calls[0][0], "weibo")]
    assert source.local_cache_ready is True
    assert source.local_cache_rebuild_next_at == 0.0


def test_ensure_local_cache_ready_sets_retry_when_rebuild_fails(monkeypatch) -> None:
    source = SimpleNamespace(
        local_cache_ready=False,
        local_cache_rebuild_next_at=0.0,
    )
    ctx = cast(SourceTickContext, SimpleNamespace(now=100.0, verbose=False))

    monkeypatch.setattr(
        worker_loop_source_tick,
        "rebuild_local_cache_from_outbox",
        lambda *_args, **_kwargs: {
            "processed": 0,
            "inserted": 0,
            "cursor": 0,
            "has_error": True,
        },
    )

    worker_loop_source_tick._ensure_local_cache_ready(
        source=source,
        active_engine=object(),
        source_name="weibo",
        ctx=ctx,
    )

    assert source.local_cache_ready is False
    assert source.local_cache_rebuild_next_at == 400.0
