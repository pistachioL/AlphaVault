from __future__ import annotations

import pytest

from alphavault_reflex.organizer_state import OrganizerState
from alphavault_reflex.organizer_state import ALIAS_TASK_PAGE_LIMIT
from alphavault_reflex.organizer_state import SECTION_ALIAS_MANUAL
from alphavault_reflex.organizer_state import SECTION_STOCK_ALIAS
from alphavault_reflex.organizer_state import SECTION_STOCK_SECTOR
from alphavault_reflex.organizer_state import load_search_results
from alphavault_reflex.organizer_state import load_pending_rows


def test_organizer_state_starts_in_loading_state() -> None:
    state = OrganizerState()

    assert hasattr(state, "loading")
    assert hasattr(state, "loaded_once")
    assert hasattr(state, "show_loading")
    assert hasattr(state, "show_pending_empty")
    assert state.active_section == SECTION_STOCK_ALIAS
    assert state.show_loading is True
    assert state.show_pending_empty is False


def test_load_pending_marks_loaded_before_showing_empty(monkeypatch) -> None:
    seen_sections: list[str] = []

    def _fake_load_pending_rows(section, alias_task_limit=0):  # type: ignore[no-untyped-def]
        seen_sections.append(str(section))
        return ([], "")

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.load_pending_rows",
        _fake_load_pending_rows,
    )

    state = OrganizerState()
    assert list(state.load_pending()) == [None]

    assert seen_sections == [SECTION_STOCK_ALIAS]
    assert getattr(state, "loaded_once", False) is True
    assert getattr(state, "loading", True) is False
    assert hasattr(state, "show_pending_empty")
    assert state.show_pending_empty is True


def test_set_active_section_empty_value_falls_back_to_stock_alias(monkeypatch) -> None:
    seen_sections: list[str] = []

    def _fake_load_pending_rows(section, alias_task_limit=0):  # type: ignore[no-untyped-def]
        seen_sections.append(str(section))
        return ([], "")

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.load_pending_rows",
        _fake_load_pending_rows,
    )

    state = OrganizerState()
    state.active_section = SECTION_STOCK_ALIAS

    assert list(state.set_active_section("")) == [None]

    assert state.active_section == SECTION_STOCK_ALIAS
    assert seen_sections == [SECTION_STOCK_ALIAS]


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


def test_stock_alias_candidate_actions_remove_current_row_without_reloading(
    monkeypatch,
) -> None:
    action_calls: list[tuple[str, str]] = []
    clear_calls: list[str] = []

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.clear_reflex_source_caches",
        lambda: clear_calls.append("cleared"),
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.load_pending_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("should_not_reload_stock_alias")
        ),
    )

    def _fake_apply_candidate_action_by_id(*, rows, candidate_id, action):  # type: ignore[no-untyped-def]
        action_calls.append((str(candidate_id), str(action)))
        return True

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.apply_candidate_action_by_id",
        _fake_apply_candidate_action_by_id,
    )

    methods = (
        ("accept_candidate", "accept"),
        ("ignore_candidate", "ignore"),
        ("block_candidate", "block"),
    )
    for method_name, action in methods:
        state = OrganizerState()
        state.active_section = SECTION_STOCK_ALIAS
        state.pending_rows = [
            {"candidate_id": "candidate-1", "candidate_key": "stock:紫金"},
            {"candidate_id": "candidate-2", "candidate_key": "stock:阿里"},
        ]

        event = getattr(state, method_name)("candidate-1")

        assert next(event) is None
        assert state.candidate_action_pending_id == "candidate-1"

        with pytest.raises(StopIteration):
            next(event)

        assert state.pending_rows == [
            {
                "candidate_id": "candidate-2",
                "candidate_key": "stock:阿里",
                "selected": False,
            }
        ]
        assert state.candidate_action_pending_id == ""
        assert state.loading is False
        assert state.loaded_once is False

    assert action_calls == [
        ("candidate-1", "accept"),
        ("candidate-1", "ignore"),
        ("candidate-1", "block"),
    ]
    assert clear_calls == ["cleared", "cleared", "cleared"]


def test_stock_alias_selection_toggle_select_all_and_clear() -> None:
    state = OrganizerState()
    state.active_section = SECTION_STOCK_ALIAS
    state.pending_rows = [
        {"candidate_id": "candidate-1", "candidate_key": "stock:紫金"},
        {"candidate_id": "candidate-2", "candidate_key": "stock:阿里"},
    ]

    state.toggle_stock_alias_candidate("candidate-1", True)
    assert state.selected_candidate_ids == ["candidate-1"]
    assert state.pending_rows == [
        {
            "candidate_id": "candidate-1",
            "candidate_key": "stock:紫金",
            "selected": True,
        },
        {
            "candidate_id": "candidate-2",
            "candidate_key": "stock:阿里",
            "selected": False,
        },
    ]

    state.toggle_stock_alias_candidate("candidate-1", False)
    assert state.selected_candidate_ids == []
    assert state.pending_rows == [
        {
            "candidate_id": "candidate-1",
            "candidate_key": "stock:紫金",
            "selected": False,
        },
        {
            "candidate_id": "candidate-2",
            "candidate_key": "stock:阿里",
            "selected": False,
        },
    ]

    state.select_all_stock_alias_candidates()
    assert state.selected_candidate_ids == ["candidate-1", "candidate-2"]
    assert state.pending_rows == [
        {
            "candidate_id": "candidate-1",
            "candidate_key": "stock:紫金",
            "selected": True,
        },
        {
            "candidate_id": "candidate-2",
            "candidate_key": "stock:阿里",
            "selected": True,
        },
    ]

    state.clear_selected_stock_alias_candidates()
    assert state.selected_candidate_ids == []
    assert state.pending_rows == [
        {
            "candidate_id": "candidate-1",
            "candidate_key": "stock:紫金",
            "selected": False,
        },
        {
            "candidate_id": "candidate-2",
            "candidate_key": "stock:阿里",
            "selected": False,
        },
    ]


def test_stock_alias_batch_actions_remove_selected_rows_without_reloading(
    monkeypatch,
) -> None:
    action_calls: list[tuple[str, str]] = []
    clear_calls: list[str] = []

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.clear_reflex_source_caches",
        lambda: clear_calls.append("cleared"),
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.load_pending_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("should_not_reload_stock_alias")
        ),
    )

    def _fake_apply_candidate_action_by_id(*, rows, candidate_id, action):  # type: ignore[no-untyped-def]
        action_calls.append((str(candidate_id), str(action)))
        return True

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.apply_candidate_action_by_id",
        _fake_apply_candidate_action_by_id,
    )

    state = OrganizerState()
    state.active_section = SECTION_STOCK_ALIAS
    state.pending_rows = [
        {"candidate_id": "candidate-1", "candidate_key": "stock:紫金"},
        {"candidate_id": "candidate-2", "candidate_key": "stock:阿里"},
        {"candidate_id": "candidate-3", "candidate_key": "stock:腾讯"},
    ]
    state.selected_candidate_ids = ["candidate-1", "candidate-3"]

    event = state.batch_accept_selected_candidates()

    assert next(event) is None
    assert state.candidate_action_pending_id == "candidate-1"

    assert next(event) is None
    assert state.candidate_action_pending_id == "candidate-3"
    assert state.pending_rows == [
        {"candidate_id": "candidate-2", "candidate_key": "stock:阿里"},
        {"candidate_id": "candidate-3", "candidate_key": "stock:腾讯"},
    ]

    with pytest.raises(StopIteration):
        next(event)

    assert state.pending_rows == [
        {
            "candidate_id": "candidate-2",
            "candidate_key": "stock:阿里",
            "selected": False,
        }
    ]
    assert state.selected_candidate_ids == []
    assert state.candidate_action_pending_id == ""
    assert state.loading is False
    assert state.loaded_once is False
    assert state.load_error == ""
    assert action_calls == [
        ("candidate-1", "accept"),
        ("candidate-3", "accept"),
    ]
    assert clear_calls == ["cleared"]


def test_non_stock_alias_candidate_action_keeps_reload_behavior(monkeypatch) -> None:
    state = OrganizerState()
    state.active_section = SECTION_STOCK_SECTOR
    state.pending_rows = [
        {"candidate_id": "candidate-1", "candidate_key": "cluster:白酒"}
    ]

    seen_loads: list[str] = []
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.apply_candidate_action_by_id",
        lambda *, rows, candidate_id, action: True,
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.clear_reflex_source_caches",
        lambda: None,
    )

    def _fake_load_pending_rows(section, alias_task_limit=0):  # type: ignore[no-untyped-def]
        del alias_task_limit
        seen_loads.append(str(section))
        return ([{"candidate_id": "reloaded-row"}], "")

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.load_pending_rows",
        _fake_load_pending_rows,
    )

    assert list(state.accept_candidate("candidate-1")) == [None]

    assert seen_loads == [SECTION_STOCK_SECTOR]
    assert state.pending_rows == [{"candidate_id": "reloaded-row"}]
    assert state.loaded_once is True
    assert state.loading is False
    assert state.candidate_action_pending_id == ""
    assert state.selected_candidate_ids == []


def test_apply_candidate_action_marks_all_source_engines_dirty(
    monkeypatch,
) -> None:
    from alphavault_reflex.services import relation_actions

    standard_engine = object()
    weibo_engine = object()
    xueqiu_engine = object()
    upsert_calls: list[object] = []
    accept_calls: list[object] = []
    dirty_calls: list[tuple[object, str, str]] = []

    monkeypatch.setattr(
        relation_actions,
        "get_research_workbench_engine_from_env",
        lambda: standard_engine,
    )
    monkeypatch.setattr(
        relation_actions,
        "load_source_engines_from_env",
        lambda: [weibo_engine, xueqiu_engine],
        raising=False,
    )
    monkeypatch.setattr(
        relation_actions,
        "upsert_relation_candidate",
        lambda engine, **kwargs: upsert_calls.append(
            (engine, kwargs["candidate_id"], kwargs["left_key"])
        ),
    )
    monkeypatch.setattr(
        relation_actions,
        "accept_relation_candidate",
        lambda engine, *, candidate_id, source: accept_calls.append(
            (engine, candidate_id, source)
        ),
    )
    monkeypatch.setattr(
        relation_actions,
        "mark_entity_page_dirty",
        lambda engine, *, stock_key, reason: dirty_calls.append(
            (engine, stock_key, reason)
        ),
    )

    relation_actions.apply_candidate_action(
        {
            "candidate_id": "cand-1",
            "relation_type": "stock_alias",
            "left_key": "stock:601899.SH",
            "right_key": "stock:紫金",
            "relation_label": "alias_of",
            "suggestion_reason": "人工确认",
            "evidence_summary": "同票简称",
            "score": "2",
            "ai_status": "",
        },
        "accept",
    )

    assert upsert_calls == [(standard_engine, "cand-1", "stock:601899.SH")]
    assert accept_calls == [(standard_engine, "cand-1", "manual")]
    assert dirty_calls == [
        (weibo_engine, "stock:601899.SH", "candidate_action"),
        (xueqiu_engine, "stock:601899.SH", "candidate_action"),
    ]


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


def test_load_pending_rows_stock_alias_reads_pending_relation_candidates(
    monkeypatch,
) -> None:
    fake_engine = object()

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.get_research_workbench_engine_from_env",
        lambda: fake_engine,
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.load_stock_alias_candidates_from_env",
        lambda: (_ for _ in ()).throw(
            AssertionError("should_not_use_source_fast_path")
        ),
        raising=False,
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.load_sources_from_env",
        lambda: (_ for _ in ()).throw(AssertionError("should_not_load_sources")),
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state._filter_known_candidate_statuses",
        lambda _rows: (_ for _ in ()).throw(
            AssertionError("should_not_refilter_pending_candidates")
        ),
    )

    def _fake_list_pending_candidates(engine):  # type: ignore[no-untyped-def]
        assert engine is fake_engine
        return [
            {
                "relation_type": "stock_alias",
                "left_key": "stock:600519.SH",
                "right_key": "stock:茅台",
                "relation_label": "alias_of",
                "candidate_id": "candidate-1",
                "candidate_key": "stock:茅台",
                "score": "8",
                "suggestion_reason": "同条观点里代码和简称一起出现",
                "evidence_summary": "同条观点里代码和简称一起出现",
                "status": "pending",
            },
            {
                "relation_type": "stock_sector",
                "left_key": "stock:600519.SH",
                "right_key": "cluster:白酒",
                "relation_label": "member_of",
                "candidate_id": "candidate-2",
                "candidate_key": "白酒",
                "score": "5",
                "suggestion_reason": "近期高频共现",
                "evidence_summary": "近30天共现 5 次",
                "status": "pending",
            },
        ]

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.list_pending_candidates",
        _fake_list_pending_candidates,
    )

    rows, err = load_pending_rows(SECTION_STOCK_ALIAS)

    assert err == ""
    assert rows == [
        {
            "relation_type": "stock_alias",
            "left_key": "stock:600519.SH",
            "right_key": "stock:茅台",
            "relation_label": "alias_of",
            "candidate_id": "candidate-1",
            "candidate_key": "stock:茅台",
            "score": "8",
            "suggestion_reason": "同条观点里代码和简称一起出现",
            "evidence_summary": "同条观点里代码和简称一起出现",
        }
    ]


def test_stock_alias_candidates_builds_stock_index_once_and_only_requests_alias(
    monkeypatch,
) -> None:
    assertions = [{"entity_key": "stock:601899.SH"}]
    stock_index = object()
    seen_builds: list[int] = []
    seen_calls: list[tuple[str, object]] = []

    def _fake_build_stock_object_index(rows):  # type: ignore[no-untyped-def]
        seen_builds.append(len(rows))
        return stock_index

    def _fake_build_stock_pending_candidates(  # type: ignore[no-untyped-def]
        rows,
        *,
        stock_key,
        ai_enabled,
        relation_type,
        stock_index,
        should_continue=None,
    ):
        del rows, ai_enabled, should_continue
        seen_calls.append((str(relation_type), stock_index))
        return [
            {
                "relation_type": "stock_alias",
                "candidate_key": f"{stock_key}:alias",
            }
        ]

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state._unique_stock_keys",
        lambda rows: ["stock:601899.SH"],
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.build_stock_object_index",
        _fake_build_stock_object_index,
        raising=False,
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.build_stock_pending_candidates",
        _fake_build_stock_pending_candidates,
    )

    from alphavault_reflex.organizer_state import _stock_alias_candidates

    rows = _stock_alias_candidates(assertions)

    assert seen_builds == [1]
    assert seen_calls == [("stock_alias", stock_index)]
    assert rows == [
        {
            "relation_type": "stock_alias",
            "candidate_key": "stock:601899.SH:alias",
        }
    ]


def test_stock_sector_candidates_builds_stock_index_once_and_only_requests_sector(
    monkeypatch,
) -> None:
    assertions = [{"entity_key": "stock:601899.SH"}]
    stock_index = object()
    seen_builds: list[int] = []
    seen_calls: list[tuple[str, object]] = []

    def _fake_build_stock_object_index(rows):  # type: ignore[no-untyped-def]
        seen_builds.append(len(rows))
        return stock_index

    def _fake_build_stock_pending_candidates(  # type: ignore[no-untyped-def]
        rows,
        *,
        stock_key,
        ai_enabled,
        relation_type,
        stock_index,
        should_continue=None,
    ):
        del rows, ai_enabled, should_continue
        seen_calls.append((str(relation_type), stock_index))
        return [
            {
                "relation_type": "stock_sector",
                "candidate_key": f"{stock_key}:sector",
            }
        ]

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state._unique_stock_keys",
        lambda rows: ["stock:601899.SH"],
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.build_stock_object_index",
        _fake_build_stock_object_index,
        raising=False,
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.build_stock_pending_candidates",
        _fake_build_stock_pending_candidates,
    )

    from alphavault_reflex.organizer_state import _stock_sector_candidates

    rows = _stock_sector_candidates(assertions)

    assert seen_builds == [1]
    assert seen_calls == [("stock_sector", stock_index)]
    assert rows == [
        {
            "relation_type": "stock_sector",
            "candidate_key": "stock:601899.SH:sector",
        }
    ]


def test_stock_alias_candidates_stops_after_enough_rows(monkeypatch) -> None:
    assertions = [{"entity_key": "stock:601899.SH"}]
    seen_stock_keys: list[str] = []

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state._unique_stock_keys",
        lambda rows: ["stock:1", "stock:2", "stock:3"],
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.build_stock_object_index",
        lambda rows: object(),
        raising=False,
    )

    def _fake_build_stock_pending_candidates(  # type: ignore[no-untyped-def]
        rows,
        *,
        stock_key,
        ai_enabled,
        relation_type,
        stock_index,
        should_continue=None,
    ):
        del rows, ai_enabled, relation_type, stock_index, should_continue
        seen_stock_keys.append(str(stock_key))
        return [
            {
                "relation_type": "stock_alias",
                "candidate_key": f"{stock_key}:{index}",
            }
            for index in range(20)
        ]

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.build_stock_pending_candidates",
        _fake_build_stock_pending_candidates,
    )

    from alphavault_reflex.organizer_state import _stock_alias_candidates

    rows = _stock_alias_candidates(assertions)

    assert seen_stock_keys == ["stock:1", "stock:2"]
    assert len(rows) == 30


def test_stock_sector_candidates_stops_after_enough_rows(monkeypatch) -> None:
    assertions = [{"entity_key": "stock:601899.SH"}]
    seen_stock_keys: list[str] = []

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state._unique_stock_keys",
        lambda rows: ["stock:1", "stock:2", "stock:3"],
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.build_stock_object_index",
        lambda rows: object(),
        raising=False,
    )

    def _fake_build_stock_pending_candidates(  # type: ignore[no-untyped-def]
        rows,
        *,
        stock_key,
        ai_enabled,
        relation_type,
        stock_index,
        should_continue=None,
    ):
        del rows, ai_enabled, relation_type, stock_index, should_continue
        seen_stock_keys.append(str(stock_key))
        return [
            {
                "relation_type": "stock_sector",
                "candidate_key": f"{stock_key}:{index}",
            }
            for index in range(20)
        ]

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.build_stock_pending_candidates",
        _fake_build_stock_pending_candidates,
    )

    from alphavault_reflex.organizer_state import _stock_sector_candidates

    rows = _stock_sector_candidates(assertions)

    assert seen_stock_keys == ["stock:1", "stock:2"]
    assert len(rows) == 30


def test_stock_alias_candidates_disables_ai_enrich(monkeypatch) -> None:
    assertions = [{"entity_key": "stock:601899.SH"}]
    seen_ai_enabled: list[bool] = []

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state._unique_stock_keys",
        lambda rows: ["stock:1"],
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.build_stock_object_index",
        lambda rows: object(),
        raising=False,
    )

    def _fake_build_stock_pending_candidates(  # type: ignore[no-untyped-def]
        rows,
        *,
        stock_key,
        ai_enabled,
        relation_type,
        stock_index,
        should_continue=None,
    ):
        del rows, stock_key, relation_type, stock_index, should_continue
        seen_ai_enabled.append(bool(ai_enabled))
        return []

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.build_stock_pending_candidates",
        _fake_build_stock_pending_candidates,
    )

    from alphavault_reflex.organizer_state import _stock_alias_candidates

    _stock_alias_candidates(assertions)

    assert seen_ai_enabled == [False]


def test_stock_sector_candidates_disables_ai_enrich(monkeypatch) -> None:
    assertions = [{"entity_key": "stock:601899.SH"}]
    seen_ai_enabled: list[bool] = []

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state._unique_stock_keys",
        lambda rows: ["stock:1"],
    )
    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.build_stock_object_index",
        lambda rows: object(),
        raising=False,
    )

    def _fake_build_stock_pending_candidates(  # type: ignore[no-untyped-def]
        rows,
        *,
        stock_key,
        ai_enabled,
        relation_type,
        stock_index,
        should_continue=None,
    ):
        del rows, stock_key, relation_type, stock_index, should_continue
        seen_ai_enabled.append(bool(ai_enabled))
        return []

    monkeypatch.setattr(
        "alphavault_reflex.organizer_state.build_stock_pending_candidates",
        _fake_build_stock_pending_candidates,
    )

    from alphavault_reflex.organizer_state import _stock_sector_candidates

    _stock_sector_candidates(assertions)

    assert seen_ai_enabled == [False]
