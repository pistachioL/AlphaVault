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


def test_load_pending_rows_for_alias_manual_adds_sample_post_url() -> None:
    original_research_loader = organizer_state._load_research_workbench_module
    original_source_loader = organizer_state._load_source_read_module
    seen_post_uids: list[str] = []

    class _FakeResearchWorkbenchModule:
        @staticmethod
        def get_research_workbench_engine_from_env() -> str:
            return "engine"

        @staticmethod
        def list_pending_alias_resolve_tasks(
            engine: str,
            *,
            limit: int,
        ) -> list[dict[str, str]]:
            assert engine == "engine"
            assert limit == 3
            return [
                {
                    "alias_key": "stock_alias:紫金",
                    "attempt_count": "2",
                    "sample_post_uid": "xueqiu:status:373998047",
                    "sample_evidence": "提到紫金",
                    "sample_raw_text_excerpt": "紫金还能追吗",
                }
            ]

        @staticmethod
        def cleanup_known_pending_alias_resolve_tasks(
            engine: str,
            alias_keys: list[str],
        ) -> list[str]:
            assert engine == "engine"
            assert alias_keys == ["stock_alias:紫金"]
            return []

    class _FakeSourceReadModule:
        @staticmethod
        def load_post_urls_from_env(
            post_uids: list[str],
        ) -> tuple[dict[str, str], str]:
            seen_post_uids.extend(post_uids)
            return {
                "xueqiu:status:373998047": "https://xueqiu.com/S/SH601899/373998047"
            }, ""

    def _fake_research_loader() -> _FakeResearchWorkbenchModule:
        return _FakeResearchWorkbenchModule()

    def _fake_source_loader() -> _FakeSourceReadModule:
        return _FakeSourceReadModule()

    organizer_state._load_research_workbench_module = _fake_research_loader  # type: ignore[assignment]
    organizer_state._load_source_read_module = _fake_source_loader  # type: ignore[assignment]
    try:
        rows, err = organizer_state.load_pending_rows(
            organizer_state.SECTION_ALIAS_MANUAL,
            alias_task_limit=3,
        )
    finally:
        organizer_state._load_research_workbench_module = original_research_loader
        organizer_state._load_source_read_module = original_source_loader

    assert err == ""
    assert seen_post_uids == ["xueqiu:status:373998047"]
    assert len(rows) == 1
    assert rows[0]["sample_post_uid"] == "xueqiu:status:373998047"
    assert rows[0]["sample_post_url"] == "https://xueqiu.com/S/SH601899/373998047"
    assert rows[0]["ai_status_display"] == "未跑"


def test_load_pending_rows_for_alias_manual_ignores_post_url_lookup_error() -> None:
    original_research_loader = organizer_state._load_research_workbench_module
    original_source_loader = organizer_state._load_source_read_module

    class _FakeResearchWorkbenchModule:
        @staticmethod
        def get_research_workbench_engine_from_env() -> str:
            return "engine"

        @staticmethod
        def list_pending_alias_resolve_tasks(
            engine: str,
            *,
            limit: int,
        ) -> list[dict[str, str]]:
            assert engine == "engine"
            assert limit == 1
            return [
                {
                    "alias_key": "stock_alias:中芯",
                    "sample_post_uid": "xueqiu:status:123",
                }
            ]

        @staticmethod
        def cleanup_known_pending_alias_resolve_tasks(
            engine: str,
            alias_keys: list[str],
        ) -> list[str]:
            assert engine == "engine"
            assert alias_keys == ["stock_alias:中芯"]
            return []

    class _FakeSourceReadModule:
        @staticmethod
        def load_post_urls_from_env(
            post_uids: list[str],
        ) -> tuple[dict[str, str], str]:
            assert post_uids == ["xueqiu:status:123"]
            return {}, "postgres_connect_error:RuntimeError"

    def _fake_research_loader() -> _FakeResearchWorkbenchModule:
        return _FakeResearchWorkbenchModule()

    def _fake_source_loader() -> _FakeSourceReadModule:
        return _FakeSourceReadModule()

    organizer_state._load_research_workbench_module = _fake_research_loader  # type: ignore[assignment]
    organizer_state._load_source_read_module = _fake_source_loader  # type: ignore[assignment]
    try:
        rows, err = organizer_state.load_pending_rows(
            organizer_state.SECTION_ALIAS_MANUAL,
            alias_task_limit=1,
        )
    finally:
        organizer_state._load_research_workbench_module = original_research_loader
        organizer_state._load_source_read_module = original_source_loader

    assert err == ""
    assert len(rows) == 1
    assert rows[0]["sample_post_uid"] == "xueqiu:status:123"
    assert rows[0]["sample_post_url"] == ""
