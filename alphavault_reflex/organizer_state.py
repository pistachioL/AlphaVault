from __future__ import annotations

import reflex as rx
import pandas as pd

from alphavault.research_workbench import (
    ALIAS_TASK_STATUS_BLOCKED,
    ALIAS_TASK_STATUS_RESOLVED,
    ensure_research_workbench_schema,
    get_research_workbench_engine_from_env,
    list_candidate_status_map,
    list_manual_alias_resolve_tasks,
    record_stock_alias_relation,
    set_alias_resolve_task_status,
)
from alphavault_reflex.research_state import apply_candidate_action
from alphavault_reflex.services.research_data import (
    build_search_index,
    build_sector_pending_candidates,
    build_stock_pending_candidates,
)
from alphavault_reflex.services.turso_read import (
    clear_reflex_source_caches,
    load_stock_alias_relations_from_env,
    load_sources_from_env,
)
from alphavault.ui.follow_pages_key_match import (
    is_stock_code_value,
    normalize_stock_code,
)


SECTION_STOCK_ALIAS = "stock_alias"
SECTION_STOCK_SECTOR = "stock_sector"
SECTION_SECTOR_SECTOR = "sector_sector"
SECTION_ALIAS_MANUAL = "alias_manual"


def _parse_manual_alias_target_stock_key(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("stock:"):
        raw = raw[len("stock:") :].strip()
    code = normalize_stock_code(raw)
    if not is_stock_code_value(code):
        return ""
    return f"stock:{code}"


class OrganizerState(rx.State):
    search_query: str = ""
    search_results: list[dict[str, str]] = []
    active_section: str = SECTION_STOCK_ALIAS
    pending_rows: list[dict[str, str]] = []
    load_error: str = ""

    alias_manual_dialog_open: bool = False
    alias_manual_alias_key: str = ""
    alias_manual_target_input: str = ""
    alias_manual_error: str = ""

    @rx.var
    def has_search_results(self) -> bool:
        return bool(self.search_results)

    @rx.var
    def has_pending_rows(self) -> bool:
        return bool(self.pending_rows)

    @rx.event
    def set_search_query(self, value: str) -> None:
        self.search_query = str(value or "")

    @rx.event
    def run_search(self) -> None:
        self.search_results, self.load_error = load_search_results(self.search_query)

    @rx.event
    def load_pending(self) -> None:
        self.pending_rows, self.load_error = load_pending_rows(self.active_section)

    @rx.event
    def set_active_section(self, value: str) -> None:
        self.active_section = str(value or SECTION_STOCK_ALIAS)
        self.pending_rows, self.load_error = load_pending_rows(self.active_section)

    @rx.event
    def accept_candidate(self, candidate_id: str) -> None:
        self._mutate_candidate(candidate_id, action="accept")

    @rx.event
    def ignore_candidate(self, candidate_id: str) -> None:
        self._mutate_candidate(candidate_id, action="ignore")

    @rx.event
    def block_candidate(self, candidate_id: str) -> None:
        self._mutate_candidate(candidate_id, action="block")

    @rx.event
    def set_alias_manual_dialog_open(self, value: bool) -> None:
        if value:
            self.alias_manual_dialog_open = True
            return
        self.close_alias_manual_dialog()

    @rx.event
    def close_alias_manual_dialog(self) -> None:
        self.alias_manual_dialog_open = False
        self.alias_manual_alias_key = ""
        self.alias_manual_target_input = ""
        self.alias_manual_error = ""

    @rx.event
    def open_alias_manual_dialog(self, alias_key: str) -> None:
        key = str(alias_key or "").strip()
        self.alias_manual_dialog_open = True
        self.alias_manual_alias_key = key
        self.alias_manual_target_input = ""
        self.alias_manual_error = ""

    @rx.event
    def set_alias_manual_target_input(self, value: str) -> None:
        self.alias_manual_target_input = str(value or "")

    @rx.event
    def confirm_alias_manual_merge(self) -> None:
        alias_key = str(self.alias_manual_alias_key or "").strip()
        target_key = _parse_manual_alias_target_stock_key(
            str(self.alias_manual_target_input or "")
        )
        if not alias_key:
            return
        if not target_key:
            self.alias_manual_error = "请输入股票代码，比如 601899.SH"
            return
        try:
            engine = get_research_workbench_engine_from_env()
            ensure_research_workbench_schema(engine)
            record_stock_alias_relation(
                engine,
                stock_key=target_key,
                alias_key=alias_key,
                source="manual",
            )
            set_alias_resolve_task_status(
                engine,
                alias_key=alias_key,
                status=ALIAS_TASK_STATUS_RESOLVED,
            )
        except BaseException as err:
            if isinstance(err, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                raise
            self.alias_manual_error = f"失败：{err}"
            return
        clear_reflex_source_caches()
        self.close_alias_manual_dialog()
        self.pending_rows, self.load_error = load_pending_rows(self.active_section)

    @rx.event
    def block_alias_manual(self) -> None:
        alias_key = str(self.alias_manual_alias_key or "").strip()
        if not alias_key:
            return
        try:
            engine = get_research_workbench_engine_from_env()
            ensure_research_workbench_schema(engine)
            set_alias_resolve_task_status(
                engine,
                alias_key=alias_key,
                status=ALIAS_TASK_STATUS_BLOCKED,
            )
        except BaseException as err:
            if isinstance(err, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                raise
            self.alias_manual_error = f"失败：{err}"
            return
        clear_reflex_source_caches()
        self.close_alias_manual_dialog()
        self.pending_rows, self.load_error = load_pending_rows(self.active_section)

    def _mutate_candidate(self, candidate_id: str, *, action: str) -> None:
        target = str(candidate_id or "").strip()
        if not target:
            return
        row = next(
            (
                item
                for item in self.pending_rows
                if str(item.get("candidate_id") or "").strip() == target
            ),
            None,
        )
        if row is None:
            return
        apply_candidate_action(row, action)
        clear_reflex_source_caches()
        self.pending_rows, self.load_error = load_pending_rows(self.active_section)


def load_search_results(query: str) -> tuple[list[dict[str, str]], str]:
    posts, assertions, err = load_sources_from_env()
    if err:
        return [], err
    needle = str(query or "").strip()
    stock_relations, relation_err = load_stock_alias_relations_from_env()
    if relation_err:
        stock_relations = None
    rows = build_search_index(
        posts,
        assertions,
        stock_relations=stock_relations,
    )
    needle = needle.lower()
    if not needle:
        return rows[:20], ""
    filtered = [
        row
        for row in rows
        if needle in str(row.get("label") or "").lower()
        or needle in str(row.get("entity_key") or "").lower()
        or needle in str(row.get("search_text") or "").lower()
    ]
    return filtered[:20], ""


def load_pending_rows(section: str) -> tuple[list[dict[str, str]], str]:
    section_key = str(section or SECTION_STOCK_ALIAS).strip()
    if section_key == SECTION_ALIAS_MANUAL:
        try:
            engine = get_research_workbench_engine_from_env()
            ensure_research_workbench_schema(engine)
            task_rows = list_manual_alias_resolve_tasks(engine)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                raise
            return [], str(exc)
        out: list[dict[str, str]] = []
        for row in task_rows:
            alias_key = str(row.get("alias_key") or "").strip()
            if not alias_key:
                continue
            out.append(
                {
                    "alias_key": alias_key,
                    "attempt_count": str(row.get("attempt_count") or "0").strip(),
                    "status": str(row.get("status") or "").strip(),
                    "updated_at": str(row.get("updated_at") or "").strip(),
                }
            )
        return out, ""

    posts, assertions, err = load_sources_from_env()
    del posts
    if err:
        return [], err
    candidate_rows = _build_section_candidates(assertions, section_key)
    return _filter_known_candidate_statuses(candidate_rows), ""


def _build_section_candidates(
    assertions,
    section: str,
) -> list[dict[str, str]]:
    if section == SECTION_STOCK_ALIAS:
        return _stock_alias_candidates(assertions)
    if section == SECTION_STOCK_SECTOR:
        return _stock_sector_candidates(assertions)
    return _sector_relation_candidates(assertions)


def _stock_alias_candidates(assertions) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for stock_key in _unique_stock_keys(assertions):
        out.extend(
            build_stock_pending_candidates(
                assertions, stock_key=stock_key, ai_enabled=True
            )
        )
    return [row for row in out if row.get("relation_type") == SECTION_STOCK_ALIAS][:30]


def _stock_sector_candidates(assertions) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for stock_key in _unique_stock_keys(assertions):
        out.extend(
            build_stock_pending_candidates(
                assertions, stock_key=stock_key, ai_enabled=True
            )
        )
    return [row for row in out if row.get("relation_type") == SECTION_STOCK_SECTOR][:30]


def _sector_relation_candidates(assertions) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for sector_key in _unique_sector_keys(assertions):
        out.extend(
            build_sector_pending_candidates(
                assertions, sector_key=sector_key, ai_enabled=True
            )
        )
    return out[:30]


def _unique_stock_keys(assertions) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw_key in assertions.get("topic_key", pd.Series(dtype=str)).tolist():
        stock_key = str(raw_key or "").strip()
        if not stock_key.startswith("stock:") or stock_key in seen:
            continue
        seen.add(stock_key)
        out.append(stock_key)
    return out


def _unique_sector_keys(assertions) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in assertions.get("cluster_keys", pd.Series(dtype=object)).tolist():
        if not isinstance(item, list):
            continue
        for raw_key in item:
            sector_key = str(raw_key or "").strip()
            if not sector_key or sector_key in seen:
                continue
            seen.add(sector_key)
            out.append(sector_key)
    return out


def _filter_known_candidate_statuses(
    rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    if not rows:
        return []
    try:
        engine = get_research_workbench_engine_from_env()
        ensure_research_workbench_schema(engine)
        status_map = list_candidate_status_map(
            engine,
            [str(row.get("candidate_id") or "").strip() for row in rows],
        )
    except Exception:
        status_map = {}
    return [
        row
        for row in rows
        if str(
            status_map.get(str(row.get("candidate_id") or "").strip(), "") or ""
        ).strip()
        not in {"accepted", "ignored", "blocked"}
    ]
