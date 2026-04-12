from __future__ import annotations

from alphavault_reflex.organizer_state import OrganizerState
from alphavault_reflex.organizer_state import ALIAS_TASK_PAGE_LIMIT
from alphavault_reflex.organizer_state import SECTION_ALIAS_MANUAL
from alphavault_reflex.organizer_state import load_search_results
from alphavault_reflex.organizer_state import load_pending_rows


def test_organizer_state_starts_in_loading_state() -> None:
    state = OrganizerState()

    assert hasattr(state, "loading")
    assert hasattr(state, "loaded_once")
    assert hasattr(state, "show_loading")
    assert hasattr(state, "show_pending_empty")
    assert state.show_loading is True
    assert state.show_pending_empty is False


def test_load_pending_marks_loaded_before_showing_empty(monkeypatch) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.load_pending_rows",
        lambda section, alias_task_limit=0: ([], ""),
    )

    state = OrganizerState()
    assert list(state.load_pending()) == [None]

    assert getattr(state, "loaded_once", False) is True
    assert getattr(state, "loading", True) is False
    assert hasattr(state, "show_pending_empty")
    assert state.show_pending_empty is True


def test_load_pending_rows_alias_manual_returns_pending_alias_tasks(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.get_research_workbench_engine_from_env",
        lambda: object(),
    )
    seen_limits: list[int] = []

    def _fake_list_pending_alias_resolve_tasks(_engine, limit=0):  # type: ignore[no-untyped-def]
        seen_limits.append(int(limit))
        return [
            {
                "alias_key": "stock:长电",
                "attempt_count": 2,
                "status": "pending",
                "sample_post_uid": "weibo:9",
                "sample_evidence": "提到长电和封测",
                "sample_raw_text_excerpt": "今天继续看好长电科技。",
                "updated_at": "2026-04-06 10:00:00",
            }
        ]

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.list_pending_alias_resolve_tasks",
        _fake_list_pending_alias_resolve_tasks,
    )

    rows, err = load_pending_rows(SECTION_ALIAS_MANUAL)

    assert err == ""
    assert seen_limits == [30]
    assert rows == [
        {
            "alias_key": "stock:长电",
            "attempt_count": "2",
            "status": "pending",
            "sample_post_uid": "weibo:9",
            "sample_evidence": "提到长电和封测",
            "sample_raw_text_excerpt": "今天继续看好长电科技。",
            "updated_at": "2026-04-06 10:00:00",
            "ai_status": "",
            "ai_stock_code": "",
            "ai_official_name": "",
            "ai_confidence": "",
            "ai_reason": "",
            "ai_uncertain": "",
        }
    ]


def test_preview_alias_ai_batch_updates_pending_rows(monkeypatch) -> None:
    state = OrganizerState()
    state.active_section = SECTION_ALIAS_MANUAL
    state.pending_rows = [
        {
            "alias_key": "stock:茅台",
            "attempt_count": "0",
            "status": "pending",
            "sample_post_uid": "weibo:1",
            "sample_evidence": "白酒龙头继续走强",
            "sample_raw_text_excerpt": "原文提到高端白酒和提价预期。",
            "updated_at": "2026-04-06 10:00:00",
            "ai_status": "",
            "ai_stock_code": "",
            "ai_official_name": "",
            "ai_confidence": "",
            "ai_reason": "",
            "ai_uncertain": "",
        }
    ]

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.enrich_alias_tasks_with_ai",
        lambda rows, **_kwargs: [
            {
                **rows[0],
                "ai_status": "ranked",
                "ai_stock_code": "600519.SH",
                "ai_official_name": "贵州茅台",
                "ai_confidence": "0.92",
                "ai_reason": "样例提到高端白酒和提价，最像贵州茅台。",
                "ai_uncertain": "",
            }
        ],
    )

    assert list(state.preview_alias_ai_batch()) == [None]

    assert state.load_error == ""
    assert state.pending_rows[0]["ai_status"] == "ranked"
    assert state.pending_rows[0]["ai_stock_code"] == "600519.SH"
    assert state.pending_rows[0]["ai_official_name"] == "贵州茅台"


def test_preview_alias_ai_batch_skips_rows_with_existing_ai_status(
    monkeypatch,
) -> None:
    state = OrganizerState()
    state.active_section = SECTION_ALIAS_MANUAL
    state.pending_rows = [
        {
            "alias_key": "stock:茅台",
            "attempt_count": "0",
            "status": "pending",
            "sample_post_uid": "weibo:1",
            "sample_evidence": "白酒龙头继续走强",
            "sample_raw_text_excerpt": "原文提到高端白酒和提价预期。",
            "updated_at": "2026-04-06 10:00:00",
            "ai_status": "ranked",
            "ai_stock_code": "600519.SH",
            "ai_official_name": "贵州茅台",
            "ai_confidence": "0.92",
            "ai_reason": "之前已经预判过",
            "ai_uncertain": "",
        },
        {
            "alias_key": "stock:长电",
            "attempt_count": "0",
            "status": "pending",
            "sample_post_uid": "weibo:2",
            "sample_evidence": "长电继续走强",
            "sample_raw_text_excerpt": "原文提到封测景气度。",
            "updated_at": "2026-04-06 10:01:00",
            "ai_status": "",
            "ai_stock_code": "",
            "ai_official_name": "",
            "ai_confidence": "",
            "ai_reason": "",
            "ai_uncertain": "",
        },
    ]

    seen_alias_keys: list[str] = []

    def _fake_enrich(rows, **_kwargs):  # type: ignore[no-untyped-def]
        seen_alias_keys.extend(str(row.get("alias_key") or "") for row in rows)
        return [
            {
                **rows[0],
                "ai_status": "ranked",
                "ai_stock_code": "600584.SH",
                "ai_official_name": "长电科技",
                "ai_confidence": "0.88",
                "ai_reason": "样例提到封测景气度，最像长电科技。",
                "ai_uncertain": "",
            }
        ]

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.enrich_alias_tasks_with_ai",
        _fake_enrich,
    )

    assert list(state.preview_alias_ai_batch()) == [None]

    assert seen_alias_keys == ["stock:长电"]
    assert state.pending_rows[0]["ai_stock_code"] == "600519.SH"
    assert state.pending_rows[1]["ai_stock_code"] == "600584.SH"


def test_load_more_alias_tasks_increases_limit_and_reloads(monkeypatch) -> None:
    state = OrganizerState()
    state.active_section = SECTION_ALIAS_MANUAL
    state.alias_task_limit = ALIAS_TASK_PAGE_LIMIT

    seen_limits: list[int] = []

    def _fake_load_pending_rows(section, alias_task_limit=0):  # type: ignore[no-untyped-def]
        seen_limits.append(int(alias_task_limit))
        return ([{"alias_key": f"stock:{alias_task_limit}"}], "")

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.load_pending_rows",
        _fake_load_pending_rows,
    )

    assert list(state.load_more_alias_tasks()) == [None]

    assert state.alias_task_limit == ALIAS_TASK_PAGE_LIMIT * 2
    assert seen_limits == [ALIAS_TASK_PAGE_LIMIT * 2]
    assert state.pending_rows == [{"alias_key": f"stock:{ALIAS_TASK_PAGE_LIMIT * 2}"}]


def test_load_more_alias_tasks_keeps_existing_ai_preview(monkeypatch) -> None:
    state = OrganizerState()
    state.active_section = SECTION_ALIAS_MANUAL
    state.alias_task_limit = ALIAS_TASK_PAGE_LIMIT
    state.pending_rows = [
        {
            "alias_key": "stock:茅台",
            "attempt_count": "0",
            "status": "pending",
            "sample_post_uid": "weibo:1",
            "sample_evidence": "白酒龙头继续走强",
            "sample_raw_text_excerpt": "原文提到高端白酒和提价预期。",
            "updated_at": "2026-04-06 10:00:00",
            "ai_status": "ranked",
            "ai_stock_code": "600519.SH",
            "ai_official_name": "贵州茅台",
            "ai_confidence": "0.92",
            "ai_reason": "之前已经预判过",
            "ai_uncertain": "",
        }
    ]

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.load_pending_rows",
        lambda section, alias_task_limit=0: (
            [
                {
                    "alias_key": "stock:茅台",
                    "attempt_count": "0",
                    "status": "pending",
                    "sample_post_uid": "weibo:1",
                    "sample_evidence": "白酒龙头继续走强",
                    "sample_raw_text_excerpt": "原文提到高端白酒和提价预期。",
                    "updated_at": "2026-04-06 10:00:00",
                    "ai_status": "",
                    "ai_stock_code": "",
                    "ai_official_name": "",
                    "ai_confidence": "",
                    "ai_reason": "",
                    "ai_uncertain": "",
                },
                {
                    "alias_key": f"stock:{alias_task_limit}",
                    "attempt_count": "0",
                    "status": "pending",
                    "sample_post_uid": "weibo:2",
                    "sample_evidence": "",
                    "sample_raw_text_excerpt": "",
                    "updated_at": "2026-04-06 10:01:00",
                    "ai_status": "",
                    "ai_stock_code": "",
                    "ai_official_name": "",
                    "ai_confidence": "",
                    "ai_reason": "",
                    "ai_uncertain": "",
                },
            ],
            "",
        ),
    )

    assert list(state.load_more_alias_tasks()) == [None]

    assert state.pending_rows[0]["ai_stock_code"] == "600519.SH"
    assert state.pending_rows[0]["ai_reason"] == "之前已经预判过"


def test_confirm_alias_manual_merge_keeps_limit_and_ai_preview(monkeypatch) -> None:
    state = OrganizerState()
    state.active_section = SECTION_ALIAS_MANUAL
    state.alias_task_limit = ALIAS_TASK_PAGE_LIMIT * 2
    state.alias_manual_alias_key = "stock:长电"
    state.alias_manual_target_input = "600584.SH"
    state.pending_rows = [
        {
            "alias_key": "stock:茅台",
            "attempt_count": "0",
            "status": "pending",
            "sample_post_uid": "weibo:1",
            "sample_evidence": "白酒龙头继续走强",
            "sample_raw_text_excerpt": "原文提到高端白酒和提价预期。",
            "updated_at": "2026-04-06 10:00:00",
            "ai_status": "ranked",
            "ai_stock_code": "600519.SH",
            "ai_official_name": "贵州茅台",
            "ai_confidence": "0.92",
            "ai_reason": "之前已经预判过",
            "ai_uncertain": "",
        }
    ]

    seen_limits: list[int] = []
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.get_research_workbench_engine_from_env",
        lambda: object(),
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.record_stock_alias_relation",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.set_alias_resolve_task_status",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.clear_reflex_source_caches",
        lambda: None,
    )

    def _fake_load_pending_rows(section, alias_task_limit=0):  # type: ignore[no-untyped-def]
        seen_limits.append(int(alias_task_limit))
        return (
            [
                {
                    "alias_key": "stock:茅台",
                    "attempt_count": "0",
                    "status": "pending",
                    "sample_post_uid": "weibo:1",
                    "sample_evidence": "白酒龙头继续走强",
                    "sample_raw_text_excerpt": "原文提到高端白酒和提价预期。",
                    "updated_at": "2026-04-06 10:00:00",
                    "ai_status": "",
                    "ai_stock_code": "",
                    "ai_official_name": "",
                    "ai_confidence": "",
                    "ai_reason": "",
                    "ai_uncertain": "",
                }
            ],
            "",
        )

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.load_pending_rows",
        _fake_load_pending_rows,
    )

    assert list(state.confirm_alias_manual_merge()) == [None]

    assert seen_limits == [ALIAS_TASK_PAGE_LIMIT * 2]
    assert state.pending_rows[0]["ai_stock_code"] == "600519.SH"
    assert state.pending_rows[0]["ai_reason"] == "之前已经预判过"


def test_load_search_results_returns_relation_error_when_standard_alias_fails(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.load_sources_from_env",
        lambda: (None, None, ""),
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.load_stock_alias_relations_from_env",
        lambda: (None, "postgres_connect_error:standard:RuntimeError"),
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.build_search_index",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should_not_build_search_index")
        ),
    )

    rows, err = load_search_results("茅台")

    assert rows == []
    assert err == "postgres_connect_error:standard:RuntimeError"
