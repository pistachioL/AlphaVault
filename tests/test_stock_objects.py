from __future__ import annotations

import threading
import time

import pandas as pd

from alphavault_reflex.services.stock_objects import (
    build_ai_stock_alias_map,
    build_stock_object_index,
    filter_assertions_for_stock_object,
    resolve_stock_object_key,
)


def test_build_stock_object_index_merges_code_and_full_name_but_not_short_alias() -> (
    None
):
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:601899.SH",
                "stock_codes_json": '["601899.SH"]',
                "stock_names_json": '["紫金矿业"]',
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:紫金矿业",
                "stock_codes_json": "[]",
                "stock_names_json": '["紫金矿业"]',
            },
            {
                "post_uid": "p3",
                "topic_key": "stock:紫金",
                "stock_codes_json": "[]",
                "stock_names_json": '["紫金"]',
            },
        ]
    )

    index = build_stock_object_index(assertions)

    assert index.display_name_by_object_key["stock:601899.SH"] == "紫金矿业"
    assert index.member_keys_by_object_key["stock:601899.SH"] == {
        "stock:601899.SH",
        "stock:紫金矿业",
    }
    assert resolve_stock_object_key(assertions, stock_key="stock:紫金") == "stock:紫金"


def test_build_ai_stock_alias_map_resolves_short_alias_with_ai(monkeypatch) -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:601899.SH",
                "summary": "先建一点仓",
                "author": "alice",
                "created_at": "2026-03-25 10:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": '["601899.SH"]',
                "stock_names_json": '["紫金矿业"]',
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:紫金",
                "summary": "继续拿着",
                "author": "alice",
                "created_at": "2026-03-26 10:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["紫金"]',
            },
        ]
    )

    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects.ai_is_configured",
        lambda: (True, ""),
    )
    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects._call_ai_with_litellm",
        lambda **kwargs: {
            "target_object_key": "stock:601899.SH",
            "ai_reason": "同作者同板块，判断是紫金矿业简称",
        },
    )

    ai_alias_map = build_ai_stock_alias_map(assertions, alias_keys=["stock:紫金"])

    assert ai_alias_map == {"stock:紫金": "stock:601899.SH"}


def test_filter_assertions_for_stock_object_uses_accepted_alias_relations() -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:601899.SH",
                "stock_codes_json": '["601899.SH"]',
                "stock_names_json": '["紫金矿业"]',
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:阿紫",
                "stock_codes_json": "[]",
                "stock_names_json": '["阿紫"]',
            },
        ]
    )
    relations = pd.DataFrame(
        [
            {
                "relation_type": "stock_alias",
                "left_key": "stock:601899.SH",
                "right_key": "stock:阿紫",
                "relation_label": "alias_of",
            }
        ]
    )

    filtered = filter_assertions_for_stock_object(
        assertions,
        stock_key="stock:601899.SH",
        stock_relations=relations,
    )

    assert filtered["post_uid"].tolist() == ["p1", "p2"]


def test_build_ai_stock_alias_map_ignores_non_stock_like_topic_keys(
    monkeypatch,
) -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:601899.SH",
                "summary": "先建仓",
                "author": "alice",
                "created_at": "2026-03-25 10:00:00",
                "stock_codes_json": '["601899.SH"]',
                "stock_names_json": '["紫金矿业"]',
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:commodity:黄金",
                "summary": "看黄金波动",
                "author": "alice",
                "created_at": "2026-03-26 10:00:00",
                "stock_codes_json": "[]",
                "stock_names_json": "[]",
            },
        ]
    )
    calls: list[str] = []

    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects.ai_is_configured",
        lambda: (True, ""),
    )

    def _fake_ai(**kwargs):
        calls.append(str(kwargs.get("trace_label") or ""))
        return {"target_object_key": "", "ai_reason": ""}

    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects._call_ai_with_litellm",
        _fake_ai,
    )

    ai_alias_map = build_ai_stock_alias_map(assertions)

    assert ai_alias_map == {}
    assert calls == []


def test_build_ai_stock_alias_map_respects_max_alias_keys_and_stats(
    monkeypatch,
) -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:601899.SH",
                "summary": "先建仓",
                "author": "alice",
                "created_at": "2026-03-25 10:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": '["601899.SH"]',
                "stock_names_json": '["紫金矿业"]',
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:紫金",
                "summary": "看多",
                "author": "alice",
                "created_at": "2026-03-26 10:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["紫金"]',
            },
            {
                "post_uid": "p3",
                "topic_key": "stock:阿紫",
                "summary": "继续看",
                "author": "alice",
                "created_at": "2026-03-26 11:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["阿紫"]',
            },
            {
                "post_uid": "p4",
                "topic_key": "stock:小紫",
                "summary": "再观察",
                "author": "alice",
                "created_at": "2026-03-26 12:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["小紫"]',
            },
        ]
    )
    calls: list[str] = []
    stats: dict[str, int] = {}

    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects.ai_is_configured",
        lambda: (True, ""),
    )

    def _fake_ai(**kwargs):
        calls.append(str(kwargs.get("trace_label") or ""))
        return {"target_object_key": "", "ai_reason": ""}

    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects._call_ai_with_litellm",
        _fake_ai,
    )

    ai_alias_map = build_ai_stock_alias_map(
        assertions,
        max_alias_keys=2,
        stats_out=stats,
    )

    assert ai_alias_map == {}
    assert len(calls) == 2
    assert stats["unresolved_total"] == 3
    assert stats["processed_aliases"] == 2
    assert stats["remaining_aliases"] == 1


def test_build_ai_stock_alias_map_respects_ai_max_inflight(monkeypatch) -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:601899.SH",
                "summary": "先建仓",
                "author": "alice",
                "created_at": "2026-03-25 10:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": '["601899.SH"]',
                "stock_names_json": '["紫金矿业"]',
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:紫金",
                "summary": "看多",
                "author": "alice",
                "created_at": "2026-03-26 10:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["紫金"]',
            },
            {
                "post_uid": "p3",
                "topic_key": "stock:阿紫",
                "summary": "继续看",
                "author": "alice",
                "created_at": "2026-03-26 11:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["阿紫"]',
            },
            {
                "post_uid": "p4",
                "topic_key": "stock:小紫",
                "summary": "再观察",
                "author": "alice",
                "created_at": "2026-03-26 12:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["小紫"]',
            },
        ]
    )
    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects.ai_is_configured",
        lambda: (True, ""),
    )

    lock = threading.Lock()
    active = 0
    max_active = 0
    calls: list[str] = []

    def _fake_resolve(*_args, **kwargs) -> str:
        nonlocal active, max_active
        alias_key = str(kwargs.get("alias_key") or "").strip()
        with lock:
            active += 1
            max_active = max(max_active, active)
        calls.append(alias_key)
        time.sleep(0.03)
        with lock:
            active -= 1
        return ""

    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects._resolve_single_alias_with_ai",
        _fake_resolve,
    )

    build_ai_stock_alias_map(
        assertions,
        alias_keys=["stock:紫金", "stock:阿紫", "stock:小紫"],
        ai_max_inflight=2,
    )

    assert len(calls) == 3
    assert max_active <= 2


def test_build_ai_stock_alias_map_respects_shared_slot_gate(monkeypatch) -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:601899.SH",
                "summary": "先建仓",
                "author": "alice",
                "created_at": "2026-03-25 10:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": '["601899.SH"]',
                "stock_names_json": '["紫金矿业"]',
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:紫金",
                "summary": "看多",
                "author": "alice",
                "created_at": "2026-03-26 10:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["紫金"]',
            },
            {
                "post_uid": "p3",
                "topic_key": "stock:阿紫",
                "summary": "继续看",
                "author": "alice",
                "created_at": "2026-03-26 11:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["阿紫"]',
            },
            {
                "post_uid": "p4",
                "topic_key": "stock:小紫",
                "summary": "再观察",
                "author": "alice",
                "created_at": "2026-03-26 12:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["小紫"]',
            },
        ]
    )
    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects.ai_is_configured",
        lambda: (True, ""),
    )

    lock = threading.Lock()
    active = 0
    max_active = 0
    gated_inflight = 0
    calls: list[str] = []

    def _acquire_slot() -> bool:
        nonlocal gated_inflight
        with lock:
            if gated_inflight >= 1:
                return False
            gated_inflight += 1
            return True

    def _release_slot() -> None:
        nonlocal gated_inflight
        with lock:
            if gated_inflight > 0:
                gated_inflight -= 1

    def _fake_resolve(*_args, **kwargs) -> str:
        nonlocal active, max_active
        alias_key = str(kwargs.get("alias_key") or "").strip()
        with lock:
            active += 1
            max_active = max(max_active, active)
        calls.append(alias_key)
        time.sleep(0.03)
        with lock:
            active -= 1
        return ""

    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects._resolve_single_alias_with_ai",
        _fake_resolve,
    )

    build_ai_stock_alias_map(
        assertions,
        alias_keys=["stock:紫金", "stock:阿紫", "stock:小紫"],
        ai_max_inflight=3,
        acquire_low_priority_slot=_acquire_slot,
        release_low_priority_slot=_release_slot,
    )

    assert len(calls) == 3
    assert max_active <= 1
    assert gated_inflight == 0


def test_build_ai_stock_alias_map_stops_when_should_continue_false(
    monkeypatch,
) -> None:
    assertions = pd.DataFrame(
        [
            {
                "post_uid": "p1",
                "topic_key": "stock:601899.SH",
                "summary": "先建仓",
                "author": "alice",
                "created_at": "2026-03-25 10:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": '["601899.SH"]',
                "stock_names_json": '["紫金矿业"]',
            },
            {
                "post_uid": "p2",
                "topic_key": "stock:紫金",
                "summary": "看多",
                "author": "alice",
                "created_at": "2026-03-26 10:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["紫金"]',
            },
            {
                "post_uid": "p3",
                "topic_key": "stock:阿紫",
                "summary": "继续看",
                "author": "alice",
                "created_at": "2026-03-26 11:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["阿紫"]',
            },
            {
                "post_uid": "p4",
                "topic_key": "stock:小紫",
                "summary": "再观察",
                "author": "alice",
                "created_at": "2026-03-26 12:00:00",
                "cluster_keys": ["gold"],
                "stock_codes_json": "[]",
                "stock_names_json": '["小紫"]',
            },
        ]
    )
    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects.ai_is_configured",
        lambda: (True, ""),
    )

    stop = {"value": False}
    calls: list[str] = []
    stats: dict[str, int] = {}

    def _fake_resolve(*_args, **kwargs) -> str:
        alias_key = str(kwargs.get("alias_key") or "").strip()
        calls.append(alias_key)
        stop["value"] = True
        return ""

    monkeypatch.setattr(
        "alphavault_reflex.services.stock_objects._resolve_single_alias_with_ai",
        _fake_resolve,
    )

    build_ai_stock_alias_map(
        assertions,
        alias_keys=["stock:紫金", "stock:阿紫", "stock:小紫"],
        ai_max_inflight=1,
        should_continue=lambda: not bool(stop["value"]),
        stats_out=stats,
    )

    assert len(calls) == 1
    assert stats["processed_aliases"] == 1
    assert stats["remaining_aliases"] == 2
