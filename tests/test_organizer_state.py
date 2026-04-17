from __future__ import annotations

import json
import subprocess
import sys

from alphavault_reflex import organizer_state


HEAVY_MODULES = (
    "alphavault.research_workbench",
    "alphavault_reflex.services.source_read",
    "alphavault_reflex.services.relation_actions",
    "alphavault_reflex.services.research_data",
    "alphavault.infra.ai.relation_candidate_ranker",
    "alphavault.infra.ai.alias_resolve_predictor",
    "alphavault.domains.stock.object_index",
)


def test_import_organizer_state_keeps_heavy_modules_lazy() -> None:
    script = f"""
import importlib
import json
import sys

heavy_modules = {HEAVY_MODULES!r}
organizer_state = importlib.import_module("alphavault_reflex.organizer_state")
print("__RESULT__" + json.dumps({{
    "state_name": organizer_state.OrganizerState.__name__,
    "loaded_heavy_modules": [
        module_name for module_name in heavy_modules if module_name in sys.modules
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
    payload = json.loads(marker_line.removeprefix("__RESULT__"))

    assert payload["state_name"] == "OrganizerState"
    assert payload["loaded_heavy_modules"] == []


def test_normalized_stock_alias_limit_handles_invalid_value() -> None:
    original_loader = organizer_state._load_relation_candidate_ranker_module

    class _FakeRankerModule:
        AI_RANK_BATCH_CAP = 17

    organizer_state._load_relation_candidate_ranker_module = lambda: _FakeRankerModule()  # type: ignore[assignment]
    try:
        assert organizer_state._normalized_stock_alias_limit(object()) == 17
        assert organizer_state._normalized_stock_alias_limit("9") == 9
    finally:
        organizer_state._load_relation_candidate_ranker_module = original_loader
