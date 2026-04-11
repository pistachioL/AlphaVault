from __future__ import annotations

import reflex as rx
import pandas as pd

from alphavault.infra.ai.alias_resolve_predictor import enrich_alias_tasks_with_ai
from alphavault.research_workbench import (
    ALIAS_TASK_STATUS_BLOCKED,
    ALIAS_TASK_STATUS_RESOLVED,
    get_research_workbench_engine_from_env,
    list_candidate_status_map,
    list_pending_alias_resolve_tasks,
    record_stock_alias_relation,
    set_alias_resolve_task_status,
)
from alphavault_reflex.services.relation_actions import apply_candidate_action_by_id
from alphavault.app.relation.candidate_builders import (
    build_sector_pending_candidates,
    build_stock_pending_candidates,
)
from alphavault_reflex.services.research_data import (
    build_search_index,
)
from alphavault_reflex.services.source_read import (
    clear_reflex_source_caches,
    load_stock_alias_relations_from_env,
    load_sources_from_env,
)
from alphavault.domains.stock.key_match import (
    is_stock_code_value,
    normalize_stock_code,
)


SECTION_STOCK_ALIAS = "stock_alias"
SECTION_STOCK_SECTOR = "stock_sector"
SECTION_SECTOR_SECTOR = "sector_sector"
SECTION_ALIAS_MANUAL = "alias_manual"
ALIAS_TASK_PAGE_LIMIT = 30
ALIAS_TASK_PAGE_STEP = 30
ALIAS_AI_PREVIEW_KEYS = (
    "ai_status",
    "ai_stock_code",
    "ai_official_name",
    "ai_confidence",
    "ai_reason",
    "ai_uncertain",
)


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


def _merge_alias_ai_preview_rows(
    old_rows: list[dict[str, str]],
    new_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    if not old_rows or not new_rows:
        return new_rows
    preview_by_alias_key: dict[str, dict[str, str]] = {}
    for row in old_rows:
        alias_key = str(row.get("alias_key") or "").strip()
        if not alias_key:
            continue
        preview_fields = {
            key: str(row.get(key) or "").strip()
            for key in ALIAS_AI_PREVIEW_KEYS
            if str(row.get(key) or "").strip()
        }
        if preview_fields:
            preview_by_alias_key[alias_key] = preview_fields
    if not preview_by_alias_key:
        return new_rows

    merged_rows: list[dict[str, str]] = []
    for row in new_rows:
        merged_row = dict(row)
        alias_key = str(merged_row.get("alias_key") or "").strip()
        matched_preview_fields = preview_by_alias_key.get(alias_key)
        if matched_preview_fields:
            for key, value in matched_preview_fields.items():
                if not str(merged_row.get(key) or "").strip():
                    merged_row[key] = value
        merged_rows.append(merged_row)
    return merged_rows


class OrganizerState(rx.State):
    search_query: str = ""
    search_results: list[dict[str, str]] = []
    active_section: str = SECTION_STOCK_ALIAS
    pending_rows: list[dict[str, str]] = []
    load_error: str = ""
    alias_task_limit: int = ALIAS_TASK_PAGE_LIMIT

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
        self._reload_pending_rows()

    @rx.event
    def set_active_section(self, value: str) -> None:
        self.active_section = str(value or SECTION_STOCK_ALIAS)
        if self.active_section == SECTION_ALIAS_MANUAL:
            self.alias_task_limit = ALIAS_TASK_PAGE_LIMIT
        self._reload_pending_rows()

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
    def open_alias_manual_dialog(
        self,
        alias_key: str,
        suggested_stock_code: str = "",
    ) -> None:
        key = str(alias_key or "").strip()
        self.alias_manual_dialog_open = True
        self.alias_manual_alias_key = key
        self.alias_manual_target_input = str(suggested_stock_code or "").strip()
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
        self._reload_pending_rows()

    @rx.event
    def block_alias_manual(self) -> None:
        alias_key = str(self.alias_manual_alias_key or "").strip()
        if not alias_key:
            return
        try:
            engine = get_research_workbench_engine_from_env()
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
        self._reload_pending_rows()

    @rx.event
    def preview_alias_ai_batch(self) -> None:
        if self.active_section != SECTION_ALIAS_MANUAL:
            return
        target_indexes = [
            index
            for index, row in enumerate(self.pending_rows)
            if not str(row.get("ai_status") or "").strip()
        ]
        if not target_indexes:
            return
        target_rows = [dict(self.pending_rows[index]) for index in target_indexes]
        try:
            enriched_rows = enrich_alias_tasks_with_ai(
                target_rows,
                ai_enabled=True,
                limit=10,
            )
            merged_rows = [dict(row) for row in self.pending_rows]
            for index, row in zip(target_indexes, enriched_rows, strict=False):
                merged_rows[index] = row
            self.pending_rows = merged_rows
            self.load_error = ""
        except BaseException as err:
            if isinstance(err, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                raise
            self.load_error = str(err)

    @rx.event
    def load_more_alias_tasks(self) -> None:
        if self.active_section != SECTION_ALIAS_MANUAL:
            return
        self.alias_task_limit = (
            max(0, int(self.alias_task_limit)) + ALIAS_TASK_PAGE_STEP
        )
        self._reload_pending_rows()

    def _reload_pending_rows(self) -> None:
        pending_rows, load_error = load_pending_rows(
            self.active_section,
            alias_task_limit=self.alias_task_limit,
        )
        if self.active_section == SECTION_ALIAS_MANUAL:
            pending_rows = _merge_alias_ai_preview_rows(self.pending_rows, pending_rows)
        self.pending_rows = pending_rows
        self.load_error = load_error

    def _mutate_candidate(self, candidate_id: str, *, action: str) -> None:
        applied = apply_candidate_action_by_id(
            rows=self.pending_rows,
            candidate_id=candidate_id,
            action=action,
        )
        if not applied:
            return
        clear_reflex_source_caches()
        self.pending_rows, self.load_error = load_pending_rows(self.active_section)


def load_search_results(query: str) -> tuple[list[dict[str, str]], str]:
    posts, assertions, err = load_sources_from_env()
    if err:
        return [], err
    needle = str(query or "").strip()
    stock_relations, relation_err = load_stock_alias_relations_from_env()
    if relation_err:
        return [], relation_err
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


def load_pending_rows(
    section: str,
    *,
    alias_task_limit: int = ALIAS_TASK_PAGE_LIMIT,
) -> tuple[list[dict[str, str]], str]:
    section_key = str(section or SECTION_STOCK_ALIAS).strip()
    if section_key == SECTION_ALIAS_MANUAL:
        try:
            engine = get_research_workbench_engine_from_env()
            task_rows = list_pending_alias_resolve_tasks(
                engine,
                limit=max(1, int(alias_task_limit)),
            )
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
                    "sample_post_uid": str(row.get("sample_post_uid") or "").strip(),
                    "sample_evidence": str(row.get("sample_evidence") or "").strip(),
                    "sample_raw_text_excerpt": str(
                        row.get("sample_raw_text_excerpt") or ""
                    ).strip(),
                    "updated_at": str(row.get("updated_at") or "").strip(),
                    "ai_status": "",
                    "ai_stock_code": "",
                    "ai_official_name": "",
                    "ai_confidence": "",
                    "ai_reason": "",
                    "ai_uncertain": "",
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
    for raw_key in assertions.get("entity_key", pd.Series(dtype=str)).tolist():
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
