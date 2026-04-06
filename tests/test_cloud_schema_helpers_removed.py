from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize(
    ("module_name", "attr_name"),
    [
        ("alphavault.db.turso_queue", "ensure_cloud_queue_schema"),
        ("alphavault.db.turso_schema", "init_cloud_schema"),
        ("alphavault.research_workbench", "ensure_research_workbench_schema"),
        ("alphavault.research_workbench.schema", "ensure_research_workbench_schema"),
        ("alphavault.research_stock_cache", "ensure_research_stock_cache_schema"),
        ("alphavault.homework_trade_feed", "ensure_homework_trade_feed_schema"),
        ("alphavault.follow_pages", "ensure_follow_pages_schema"),
        ("alphavault.follow_pages", "init_follow_pages_schema"),
        ("alphavault.worker.job_state", "ensure_worker_job_state_schema"),
        ("alphavault.worker.job_state", "_ensure_schema_once"),
    ],
)
def test_cloud_schema_bootstrap_helpers_removed(
    module_name: str,
    attr_name: str,
) -> None:
    module = importlib.import_module(module_name)
    assert not hasattr(module, attr_name)
