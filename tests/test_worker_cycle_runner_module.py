from __future__ import annotations

from concurrent.futures import Future

from alphavault.worker import cycle_runner as cycle_runner_module


def test_collect_periodic_job_result_reads_stats_from_finished_future() -> None:
    future: Future = Future()
    future.set_result({"processed": 3, "has_more": False})

    next_future, stats, finished, has_error = (
        cycle_runner_module.collect_periodic_job_result(
            job_name="unit",
            future=future,
            engine=object(),
            maybe_dispose_source_db_engine_on_transient_error_fn=lambda **_kwargs: None,
            fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        )
    )

    assert next_future is None
    assert stats == {"processed": 3, "has_more": False}
    assert finished is True
    assert has_error is False


def test_collect_rss_ingest_result_nonfatal_exception_marks_error() -> None:
    future: Future = Future()
    future.set_exception(RuntimeError("boom"))
    disposed = {"called": False}

    next_future, accepted, finished, has_error = (
        cycle_runner_module.collect_rss_ingest_result(
            source_name="weibo",
            future=future,
            engine=object(),
            maybe_dispose_source_db_engine_on_transient_error_fn=lambda **_kwargs: disposed.__setitem__(
                "called", True
            ),
            fatal_exceptions=(KeyboardInterrupt, SystemExit, GeneratorExit),
        )
    )

    assert next_future is None
    assert accepted == 0
    assert finished is True
    assert has_error is True
    assert disposed["called"] is True


def test_cycle_runner_helper_functions_keep_boolean_logic() -> None:
    assert cycle_runner_module.should_fast_retry_for_periodic_job(has_more=True) is True
    assert (
        cycle_runner_module.should_fast_retry_for_periodic_job(
            has_more=True,
            attempted=0,
        )
        is False
    )
    assert (
        cycle_runner_module.build_source_db_error(
            maintenance_error=False,
            spool_flush_error=False,
            schedule_error=True,
            stock_hot_error=False,
        )
        is True
    )
    assert (
        cycle_runner_module.should_wait_with_event(
            ai_inflight=False,
            any_stock_hot_inflight=False,
            any_redis_enqueue_inflight=False,
            any_rss_inflight=False,
            any_spool_flush_inflight=False,
        )
        is False
    )
    assert (
        cycle_runner_module.should_wait_with_event(
            ai_inflight=False,
            any_stock_hot_inflight=False,
            any_redis_enqueue_inflight=True,
            any_rss_inflight=False,
            any_spool_flush_inflight=False,
        )
        is True
    )
