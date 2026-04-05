from __future__ import annotations

import threading
from concurrent.futures import Future
from types import SimpleNamespace


from alphavault.worker import ai_processor as ai_processor_module
from alphavault.worker import runtime_cache as runtime_cache_module
from alphavault.worker import scheduler as scheduler_module
from alphavault.worker.runtime_cache import AuthorRecentLocalCache
from alphavault.worker.runtime_models import LLMConfig
from alphavault.worker.topic_prompt_v4 import build_topic_prompt_v4_llm_log_line


_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _config() -> LLMConfig:
    return LLMConfig(
        api_key="k",
        model="m",
        prompt_version="p",
        relevant_threshold=0.3,
        base_url="",
        api_mode="responses",
        ai_stream=True,
        ai_retries=0,
        ai_temperature=0.1,
        ai_reasoning_effort="low",
        ai_rpm=12.0,
        ai_timeout_seconds=30.0,
        trace_out=None,
        verbose=False,
    )


def test_compute_low_priority_budget_zero_when_rss_is_full() -> None:
    assert (
        scheduler_module.compute_low_priority_budget(ai_cap=6, rss_inflight_now=6) == 0
    )
    assert (
        scheduler_module.compute_low_priority_budget(ai_cap=6, rss_inflight_now=9) == 0
    )


def test_compute_low_priority_budget_uses_remaining_slots() -> None:
    assert (
        scheduler_module.compute_low_priority_budget(ai_cap=6, rss_inflight_now=2) == 4
    )


def test_compute_backfill_max_stocks_per_run_follows_low_budget() -> None:
    assert scheduler_module.compute_backfill_max_stocks_per_run(low_budget=0) == 1
    assert scheduler_module.compute_backfill_max_stocks_per_run(low_budget=3) == 3
    assert scheduler_module.compute_backfill_max_stocks_per_run(low_budget=100) == 32


def test_compute_rss_available_slots_subtracts_low_inflight() -> None:
    assert (
        scheduler_module.compute_rss_available_slots(
            ai_cap=6,
            rss_inflight_now=1,
            low_inflight_now=2,
        )
        == 3
    )


def test_compute_rss_available_slots_never_negative() -> None:
    assert (
        scheduler_module.compute_rss_available_slots(
            ai_cap=6,
            rss_inflight_now=6,
            low_inflight_now=1,
        )
        == 0
    )


def test_should_continue_turns_false_when_rss_becomes_busy() -> None:
    state = {"rss_inflight_now": 1}
    should_continue = scheduler_module.build_low_priority_should_continue(
        ai_cap=4,
        rss_inflight_now_get=lambda: int(state["rss_inflight_now"]),
    )

    assert should_continue() is True
    state["rss_inflight_now"] = 4
    assert should_continue() is False


def test_should_continue_turns_false_when_rss_due_and_no_available_slots() -> None:
    state = {"rss_inflight_now": 0, "low_inflight_now": 4, "rss_due_now": True}
    should_continue = scheduler_module.build_low_priority_should_continue(
        ai_cap=4,
        rss_inflight_now_get=lambda: int(state["rss_inflight_now"]),
        low_inflight_now_get=lambda: int(state["low_inflight_now"]),
        has_due_ai_pending_get=lambda: bool(state["rss_due_now"]),
    )

    assert should_continue() is False


def test_memoize_bool_with_ttl_reuses_recent_result(monkeypatch) -> None:
    calls: list[str] = []
    now_state = {"now": 100.0}

    monkeypatch.setattr(
        runtime_cache_module.time,
        "time",
        lambda: float(now_state["now"]),
    )

    def _resolve() -> bool:
        calls.append("call")
        return True

    cached = runtime_cache_module.memoize_bool_with_ttl(
        resolver=_resolve,
        ttl_seconds=1.0,
    )
    assert cached() is True
    assert cached() is True
    assert calls == ["call"]

    now_state["now"] = 101.2
    assert cached() is True
    assert calls == ["call", "call"]


def test_low_priority_slot_gate_respects_dynamic_cap() -> None:
    state = {"cap": 2}
    gate = scheduler_module.LowPriorityAiSlotGate(cap_getter=lambda: int(state["cap"]))

    assert gate.try_acquire() is True
    assert gate.try_acquire() is True
    assert gate.try_acquire() is False
    assert gate.inflight() == 2

    gate.release()
    assert gate.inflight() == 1
    state["cap"] = 1
    assert gate.try_acquire() is False

    gate.release()
    assert gate.inflight() == 0
    assert gate.try_acquire() is True


def test_build_topic_prompt_v4_llm_log_line_call_contains_id_and_author() -> None:
    line = build_topic_prompt_v4_llm_log_line(
        event="call",
        root_key="root:123",
        post_uid="weibo:1",
        author="博主A",
        locked_count=0,
    )

    assert "[llm] call topic_prompt_v4" in line
    assert "post_uid=weibo:1" in line
    assert "author=博主A" in line


def test_build_topic_prompt_v4_llm_log_line_done_contains_cost() -> None:
    line = build_topic_prompt_v4_llm_log_line(
        event="done",
        root_key="root:123",
        post_uid="weibo:1",
        author="博主A",
        locked_count=3,
        cost_seconds=12.3,
    )

    assert "[llm] done topic_prompt_v4" in line
    assert "post_uid=weibo:1" in line
    assert "author=博主A" in line
    assert "locked=3" in line
    assert "cost=12.3s" in line


def test_schedule_ai_requires_spool_dir() -> None:
    inflight_futures: set[Future] = set()
    inflight_owner_by_future: dict[Future, str] = {}
    scheduled, has_error = scheduler_module.schedule_ai(
        executor=object(),
        engine=object(),
        platform="weibo",
        ai_cap=4,
        low_inflight_now_get=lambda: 0,
        inflight_futures=inflight_futures,
        inflight_owner_by_future=inflight_owner_by_future,
        inflight_owner="weibo",
        wakeup_event=threading.Event(),
        config=_config(),
        limiter=object(),
        verbose=False,
        redis_client=object(),
        redis_queue_key="queue",
        spool_dir=None,
        schedule_ai_from_redis_fn=lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not use redis queue")
        ),
    )

    assert has_error is True
    assert scheduled == 0


def test_schedule_ai_prefers_redis_queue_when_available(tmp_path) -> None:
    called: dict[str, object] = {"ok": False, "spool_dir": None}

    def _schedule_from_redis(**kwargs):  # type: ignore[no-untyped-def]
        called["ok"] = True
        called["spool_dir"] = kwargs.get("spool_dir")
        return 1, False

    inflight_futures: set[Future] = set()
    inflight_owner_by_future: dict[Future, str] = {}
    scheduled, has_error = scheduler_module.schedule_ai(
        executor=object(),
        engine=object(),
        platform="weibo",
        ai_cap=4,
        low_inflight_now_get=lambda: 0,
        inflight_futures=inflight_futures,
        inflight_owner_by_future=inflight_owner_by_future,
        inflight_owner="weibo",
        wakeup_event=threading.Event(),
        config=_config(),
        limiter=object(),
        verbose=False,
        redis_client=object(),
        redis_queue_key="queue",
        spool_dir=tmp_path,
        schedule_ai_from_redis_fn=_schedule_from_redis,
    )

    assert has_error is False
    assert scheduled == 1
    assert called["ok"] is True
    assert called["spool_dir"] == tmp_path


def test_schedule_ai_requires_redis_queue(tmp_path) -> None:
    inflight_futures: set[Future] = set()
    inflight_owner_by_future: dict[Future, str] = {}

    scheduled, has_error = scheduler_module.schedule_ai(
        executor=object(),
        engine=object(),
        platform="weibo",
        ai_cap=4,
        low_inflight_now_get=lambda: 0,
        inflight_futures=inflight_futures,
        inflight_owner_by_future=inflight_owner_by_future,
        inflight_owner="weibo",
        wakeup_event=threading.Event(),
        config=_config(),
        limiter=object(),
        verbose=False,
        redis_client=None,
        redis_queue_key="",
        spool_dir=tmp_path,
        schedule_ai_from_redis_fn=lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("missing redis should fail before scheduling")
        ),
    )

    assert scheduled == 0
    assert has_error is True


def test_process_one_redis_payload_skips_turso_recent_load_on_local_cache_hit(
    tmp_path,
) -> None:
    load_calls: list[str] = []
    ack_calls: list[str] = []
    cache = AuthorRecentLocalCache()
    cache.set(
        queue_key="queue",
        author="作者A",
        rows=[{"post_uid": "weibo:hist"}],
        marked_empty=False,
    )

    def _should_not_load_recent_posts(*_args, **_kwargs) -> list[dict[str, object]]:
        load_calls.append("called")
        return []

    def _ack_and_cleanup(*_args, **_kwargs) -> bool:
        ack_calls.append("acked")
        return True

    ai_processor_module.process_one_redis_payload(
        engine=object(),
        payload={"post_uid": "weibo:1"},
        processing_msg="msg-1",
        redis_client=object(),
        redis_queue_key="queue",
        spool_dir=tmp_path,
        config=_config(),
        limiter=object(),
        verbose=False,
        payload_to_cloud_post_fn=lambda _payload: SimpleNamespace(
            post_uid="weibo:1",
            platform="weibo",
            platform_post_id="1",
            author="作者A",
            created_at="2026-03-28 10:00:00",
            url="https://example.com/post/1",
            raw_text="正文",
            display_md="正文",
            ai_retry_count=1,
        ),
        author_recent_local_cache_get_fn=cache.get,
        author_recent_local_cache_set_fn=cache.set,
        load_recent_posts_by_author_fn=_should_not_load_recent_posts,
        redis_ai_try_claim_lease_fn=lambda *_args, **_kwargs: "lease-1",
        process_one_post_uid_fn=lambda **_kwargs: True,
        redis_ai_release_lease_fn=lambda *_args, **_kwargs: True,
        redis_ai_ack_and_cleanup_fn=_ack_and_cleanup,
        redis_ai_push_delayed_fn=lambda *_args, **_kwargs: None,
        redis_ai_ack_processing_fn=lambda *_args, **_kwargs: None,
        payload_retry_count_fn=lambda _payload: 0,
        build_author_recent_payload_fn=ai_processor_module.build_author_recent_payload,
        backoff_seconds_fn=lambda _count: 1,
        now_epoch_fn=lambda: 100,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        author_recent_context_limit=200,
        lease_seconds=300,
    )

    assert load_calls == []
    assert ack_calls == ["acked"]


def test_process_one_redis_payload_acks_processing_when_lease_is_missing(
    tmp_path,
) -> None:
    load_calls: list[str] = []
    ack_calls: list[str] = []
    cache = AuthorRecentLocalCache()

    def _load_recent_posts(*_args, **kwargs) -> list[dict[str, object]]:
        load_calls.append(str(kwargs.get("author") or ""))
        return []

    ai_processor_module.process_one_redis_payload(
        engine=object(),
        payload={"post_uid": "weibo:2"},
        processing_msg="msg-2",
        redis_client=object(),
        redis_queue_key="queue",
        spool_dir=tmp_path,
        config=_config(),
        limiter=object(),
        verbose=False,
        payload_to_cloud_post_fn=lambda _payload: SimpleNamespace(
            post_uid="weibo:2",
            platform="weibo",
            platform_post_id="2",
            author="作者B",
            created_at="2026-03-28 10:00:00",
            url="https://example.com/post/2",
            raw_text="正文",
            display_md="正文",
            ai_retry_count=1,
        ),
        author_recent_local_cache_get_fn=cache.get,
        author_recent_local_cache_set_fn=cache.set,
        load_recent_posts_by_author_fn=_load_recent_posts,
        redis_ai_try_claim_lease_fn=lambda *_args, **_kwargs: "",
        process_one_post_uid_fn=lambda **_kwargs: True,
        redis_ai_release_lease_fn=lambda *_args, **_kwargs: True,
        redis_ai_ack_and_cleanup_fn=lambda *_args, **_kwargs: True,
        redis_ai_push_delayed_fn=lambda *_args, **_kwargs: None,
        redis_ai_ack_processing_fn=lambda *_args, **_kwargs: ack_calls.append("ack"),
        payload_retry_count_fn=lambda _payload: 0,
        build_author_recent_payload_fn=ai_processor_module.build_author_recent_payload,
        backoff_seconds_fn=lambda _count: 1,
        now_epoch_fn=lambda: 100,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        author_recent_context_limit=200,
        lease_seconds=300,
    )

    rows, marked_empty, hit = cache.get(queue_key="queue", author="作者B")
    assert load_calls == []
    assert ack_calls == ["ack"]
    assert rows == []
    assert marked_empty is False
    assert hit is False


def test_process_one_redis_payload_only_keeps_final_author_cache_status(
    tmp_path,
) -> None:
    cache = AuthorRecentLocalCache()

    ai_processor_module.process_one_redis_payload(
        engine=object(),
        payload={"post_uid": "weibo:3"},
        processing_msg="msg-3",
        redis_client=object(),
        redis_queue_key="queue",
        spool_dir=tmp_path,
        config=_config(),
        limiter=object(),
        verbose=False,
        payload_to_cloud_post_fn=lambda _payload: SimpleNamespace(
            post_uid="weibo:3",
            platform="weibo",
            platform_post_id="3",
            author="作者C",
            created_at="2026-03-28 10:00:00",
            url="https://example.com/post/3",
            raw_text="正文",
            display_md="正文",
            ai_retry_count=1,
        ),
        author_recent_local_cache_get_fn=cache.get,
        author_recent_local_cache_set_fn=cache.set,
        load_recent_posts_by_author_fn=lambda *_args, **_kwargs: [
            {"post_uid": "weibo:hist", "author": "作者C"}
        ],
        redis_ai_try_claim_lease_fn=lambda *_args, **_kwargs: "lease-3",
        process_one_post_uid_fn=lambda **_kwargs: True,
        redis_ai_release_lease_fn=lambda *_args, **_kwargs: True,
        redis_ai_ack_and_cleanup_fn=lambda *_args, **_kwargs: True,
        redis_ai_push_delayed_fn=lambda *_args, **_kwargs: None,
        redis_ai_ack_processing_fn=lambda *_args, **_kwargs: None,
        payload_retry_count_fn=lambda _payload: 0,
        build_author_recent_payload_fn=ai_processor_module.build_author_recent_payload,
        backoff_seconds_fn=lambda _count: 1,
        now_epoch_fn=lambda: 100,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        author_recent_context_limit=200,
        lease_seconds=300,
    )

    rows, marked_empty, hit = cache.get(queue_key="queue", author="作者C")
    assert hit is True
    assert marked_empty is False
    assert len(rows) == 1
    assert rows[0]["post_uid"] == "weibo:3"
    assert rows[0]["ai_status"] == "done"


def test_process_one_redis_payload_reuses_local_author_cache_to_skip_turso_load(
    tmp_path,
) -> None:
    load_recent_calls: list[str] = []
    ack_calls: list[str] = []
    cache = AuthorRecentLocalCache()

    def _load_recent_posts(*_args, **_kwargs) -> list[dict[str, object]]:
        load_recent_calls.append("load")
        return [{"post_uid": "weibo:hist"}]

    def _ack_and_cleanup(*_args, **_kwargs) -> bool:  # type: ignore[no-untyped-def]
        ack_calls.append("acked")
        return True

    def _run_once(*, msg: str) -> None:
        ai_processor_module.process_one_redis_payload(
            engine=object(),
            payload={"post_uid": "weibo:4"},
            processing_msg=msg,
            redis_client=object(),
            redis_queue_key="queue",
            spool_dir=tmp_path,
            config=_config(),
            limiter=object(),
            verbose=False,
            payload_to_cloud_post_fn=lambda _payload: SimpleNamespace(
                post_uid="weibo:4",
                platform="weibo",
                platform_post_id="4",
                author="作者D",
                created_at="2026-03-28 10:00:00",
                url="https://example.com/post/4",
                raw_text="正文",
                display_md="正文",
                ai_retry_count=1,
            ),
            author_recent_local_cache_get_fn=cache.get,
            author_recent_local_cache_set_fn=cache.set,
            load_recent_posts_by_author_fn=_load_recent_posts,
            redis_ai_try_claim_lease_fn=lambda *_args, **_kwargs: "lease-4",
            process_one_post_uid_fn=lambda **_kwargs: True,
            redis_ai_release_lease_fn=lambda *_args, **_kwargs: True,
            redis_ai_ack_and_cleanup_fn=_ack_and_cleanup,
            redis_ai_push_delayed_fn=lambda *_args, **_kwargs: None,
            redis_ai_ack_processing_fn=lambda *_args, **_kwargs: None,
            payload_retry_count_fn=lambda _payload: 0,
            build_author_recent_payload_fn=ai_processor_module.build_author_recent_payload,
            backoff_seconds_fn=lambda _count: 1,
            now_epoch_fn=lambda: 100,
            fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
            author_recent_context_limit=200,
            lease_seconds=300,
        )

    _run_once(msg="msg-4a")
    _run_once(msg="msg-4b")

    assert load_recent_calls == ["load"]
    assert ack_calls == ["acked", "acked"]


def test_process_one_redis_payload_claims_and_releases_lease_on_success(
    tmp_path,
) -> None:
    events: list[tuple[object, ...]] = []

    def _claim_lease(
        _client, _queue_key: str, *, post_uid: str, lease_seconds: int
    ) -> str:
        events.append(("claim", post_uid, lease_seconds))
        return "lease-token-1"

    def _release_lease(
        _client,
        _queue_key: str,
        *,
        post_uid: str,
        lease_token: str,
        verbose: bool,
    ) -> bool:
        events.append(("release", post_uid, lease_token, verbose))
        return True

    def _ack_and_cleanup(*_args, **_kwargs) -> bool:
        events.append(("ack", _kwargs["msg"]))
        return True

    ai_processor_module.process_one_redis_payload(
        engine=object(),
        payload={"post_uid": "weibo:ok", "retry_count": 2},
        processing_msg="msg-ok",
        redis_client=object(),
        redis_queue_key="queue",
        spool_dir=tmp_path,
        config=_config(),
        limiter=object(),
        verbose=False,
        payload_to_cloud_post_fn=lambda _payload: SimpleNamespace(
            post_uid="weibo:ok",
            platform="weibo",
            platform_post_id="ok",
            author="作者Lease",
            created_at="2026-03-28 10:00:00",
            url="https://example.com/post/ok",
            raw_text="正文",
            display_md="正文",
            ai_retry_count=2,
        ),
        author_recent_local_cache_get_fn=lambda **_kwargs: ([], False, True),
        author_recent_local_cache_set_fn=lambda **_kwargs: None,
        load_recent_posts_by_author_fn=lambda *_args, **_kwargs: [],
        redis_ai_try_claim_lease_fn=_claim_lease,
        process_one_post_uid_fn=lambda **_kwargs: True,
        redis_ai_release_lease_fn=_release_lease,
        redis_ai_ack_and_cleanup_fn=_ack_and_cleanup,
        redis_ai_push_delayed_fn=lambda *_args, **_kwargs: None,
        redis_ai_ack_processing_fn=lambda *_args, **_kwargs: None,
        payload_retry_count_fn=lambda _payload: 2,
        build_author_recent_payload_fn=ai_processor_module.build_author_recent_payload,
        backoff_seconds_fn=lambda _count: 1,
        now_epoch_fn=lambda: 100,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        author_recent_context_limit=200,
        lease_seconds=300,
    )

    assert events == [
        ("claim", "weibo:ok", 300),
        ("ack", "msg-ok"),
        ("release", "weibo:ok", "lease-token-1", False),
    ]


def test_process_one_redis_payload_does_not_ack_when_lease_claim_errors(
    tmp_path,
) -> None:
    events: list[tuple[object, ...]] = []

    def _claim_lease(*_args, **_kwargs) -> str:
        raise RuntimeError("redis unavailable")

    def _ack_processing(*_args, **_kwargs) -> None:
        events.append(("ack_processing",))

    def _release_lease(*_args, **_kwargs) -> bool:
        events.append(("release",))
        return True

    ai_processor_module.process_one_redis_payload(
        engine=object(),
        payload={"post_uid": "weibo:claim-error"},
        processing_msg="msg-claim-error",
        redis_client=object(),
        redis_queue_key="queue",
        spool_dir=tmp_path,
        config=_config(),
        limiter=object(),
        verbose=False,
        payload_to_cloud_post_fn=lambda _payload: SimpleNamespace(
            post_uid="weibo:claim-error",
            platform="weibo",
            platform_post_id="claim-error",
            author="作者Lease",
            created_at="2026-03-28 10:00:00",
            url="https://example.com/post/claim-error",
            raw_text="正文",
            display_md="正文",
            ai_retry_count=0,
        ),
        author_recent_local_cache_get_fn=lambda **_kwargs: ([], False, False),
        author_recent_local_cache_set_fn=lambda **_kwargs: None,
        load_recent_posts_by_author_fn=lambda *_args, **_kwargs: [],
        redis_ai_try_claim_lease_fn=_claim_lease,
        process_one_post_uid_fn=lambda **_kwargs: True,
        redis_ai_release_lease_fn=_release_lease,
        redis_ai_ack_and_cleanup_fn=lambda *_args, **_kwargs: True,
        redis_ai_push_delayed_fn=lambda *_args, **_kwargs: None,
        redis_ai_ack_processing_fn=_ack_processing,
        payload_retry_count_fn=lambda _payload: 0,
        build_author_recent_payload_fn=ai_processor_module.build_author_recent_payload,
        backoff_seconds_fn=lambda _count: 1,
        now_epoch_fn=lambda: 100,
        fatal_exceptions=_FATAL_BASE_EXCEPTIONS,
        author_recent_context_limit=200,
        lease_seconds=300,
    )

    assert events == []
