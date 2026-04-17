from __future__ import annotations

import json
import subprocess
import sys


ANALYSIS_FEEDBACK_HEAVY_MODULES = (
    "alphavault.db.analysis_feedback",
    "alphavault.db.postgres_db",
    "alphavault.db.source_queue",
)

RESEARCH_PAGE_LOADER_HEAVY_MODULES = (
    "alphavault_reflex.services.source_read",
    "alphavault_reflex.services.stock_hot_read",
    "alphavault_reflex.services.sector_hot_read",
    "alphavault_reflex.services.homework_board",
    "alphavault_reflex.services.research_data",
)

RESEARCH_STATE_HEAVY_MODULES = (
    "alphavault.db.analysis_feedback",
    "alphavault.db.postgres_db",
    "alphavault_reflex.services.source_read",
    "alphavault_reflex.services.stock_hot_read",
    "alphavault_reflex.services.sector_hot_read",
)

HOMEWORK_STATE_HEAVY_MODULES = (
    "alphavault.db.analysis_feedback",
    "alphavault.db.postgres_db",
    "alphavault_reflex.services.source_read",
    "alphavault.domains.stock.object_index",
)

APP_HEAVY_MODULES = (
    "alphavault.db.analysis_feedback",
    "alphavault.db.postgres_db",
    "alphavault_reflex.services.source_read",
    "alphavault_reflex.services.stock_hot_read",
    "alphavault_reflex.services.sector_hot_read",
    "alphavault.domains.stock.object_index",
)

SOURCE_READ_HEAVY_MODULES = (
    "alphavault.homework_trade_feed",
    "alphavault.research_workbench.service",
    "alphavault_reflex.services.stock_fast_loader",
    "alphavault_reflex.services.trade_board_loader",
    "alphavault_reflex.services.tree_loader",
    "alphavault_reflex.services.url_loader",
)


def _import_payload(
    module_name: str, *, symbol_name: str, heavy_modules: tuple[str, ...]
):
    script = f"""
import importlib
import json
import sys

module_name = {module_name!r}
symbol_name = {symbol_name!r}
heavy_modules = {heavy_modules!r}
module = importlib.import_module(module_name)
symbol = getattr(module, symbol_name)
print("__RESULT__" + json.dumps({{
    "symbol_name": getattr(symbol, "__name__", ""),
    "loaded_heavy_modules": [
        item for item in heavy_modules if item in sys.modules
    ],
}}))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        check=True,
        text=True,
    )
    marker_line = next(
        line for line in result.stdout.splitlines() if line.startswith("__RESULT__")
    )
    return json.loads(marker_line.removeprefix("__RESULT__"))


def test_import_analysis_feedback_keeps_db_modules_lazy() -> None:
    payload = _import_payload(
        "alphavault_reflex.services.analysis_feedback",
        symbol_name="submit_post_analysis_feedback",
        heavy_modules=ANALYSIS_FEEDBACK_HEAVY_MODULES,
    )

    assert payload["symbol_name"] == "submit_post_analysis_feedback"
    assert payload["loaded_heavy_modules"] == []


def test_import_research_page_loader_keeps_data_modules_lazy() -> None:
    payload = _import_payload(
        "alphavault_reflex.services.research_page_loader",
        symbol_name="load_stock_page_cached_view",
        heavy_modules=RESEARCH_PAGE_LOADER_HEAVY_MODULES,
    )

    assert payload["symbol_name"] == "load_stock_page_cached_view"
    assert payload["loaded_heavy_modules"] == []


def test_import_research_state_keeps_heavy_modules_lazy() -> None:
    payload = _import_payload(
        "alphavault_reflex.research_state",
        symbol_name="ResearchState",
        heavy_modules=RESEARCH_STATE_HEAVY_MODULES,
    )

    assert payload["symbol_name"] == "ResearchState"
    assert payload["loaded_heavy_modules"] == []


def test_import_homework_state_keeps_heavy_modules_lazy() -> None:
    payload = _import_payload(
        "alphavault_reflex.homework_state",
        symbol_name="HomeworkState",
        heavy_modules=HOMEWORK_STATE_HEAVY_MODULES,
    )

    assert payload["symbol_name"] == "HomeworkState"
    assert payload["loaded_heavy_modules"] == []


def test_import_app_keeps_runtime_modules_lazy() -> None:
    payload = _import_payload(
        "alphavault_reflex.alphavault_reflex",
        symbol_name="app",
        heavy_modules=APP_HEAVY_MODULES,
    )

    assert payload["loaded_heavy_modules"] == []


def test_import_source_read_keeps_runtime_submodules_lazy() -> None:
    payload = _import_payload(
        "alphavault_reflex.services.source_read",
        symbol_name="load_homework_board_payload_from_env",
        heavy_modules=SOURCE_READ_HEAVY_MODULES,
    )

    assert payload["symbol_name"] == "load_homework_board_payload_from_env"
    assert payload["loaded_heavy_modules"] == []


def test_source_read___all___only_lists_bound_names() -> None:
    script = """
import json

from alphavault_reflex.services import source_read

missing = [
    name for name in source_read.__all__ if name not in source_read.__dict__
]
print("__RESULT__" + json.dumps({"missing": missing}))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        check=True,
        text=True,
    )
    marker_line = next(
        line for line in result.stdout.splitlines() if line.startswith("__RESULT__")
    )
    payload = json.loads(marker_line.removeprefix("__RESULT__"))

    assert payload["missing"] == []
