from __future__ import annotations

import reflex as rx

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
    load_stock_alias_candidates_from_env,
    load_stock_alias_relations_from_env,
    load_sources_from_env,
)
from alphavault.domains.stock.key_match import (
    is_stock_code_value,
    normalize_stock_code,
)
from alphavault.domains.stock.object_index import build_stock_object_index


SECTION_STOCK_ALIAS = "stock_alias"
SECTION_STOCK_SECTOR = "stock_sector"
SECTION_SECTOR_SECTOR = "sector_sector"
SECTION_ALIAS_MANUAL = "alias_manual"
SECTION_CANDIDATE_LIMIT = 30
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


def _remove_candidate_row_by_id(
    rows: list[dict[str, str]],
    *,
    candidate_id: str,
) -> list[dict[str, str]]:
    target = str(candidate_id or "").strip()
    if not target:
        return [dict(row) for row in rows]
    return [
        dict(row)
        for row in rows
        if str(row.get("candidate_id") or "").strip() != target
    ]


def _visible_stock_alias_candidate_ids(rows: list[dict[str, str]]) -> list[str]:
    out: list[str] = []
    for row in rows:
        candidate_id = str(row.get("candidate_id") or "").strip()
        if not candidate_id:
            continue
        out.append(candidate_id)
    return out


def _sync_selected_candidate_ids(
    rows: list[dict[str, str]],
    selected_candidate_ids: list[str],
) -> list[str]:
    selected_set = {
        str(candidate_id or "").strip()
        for candidate_id in selected_candidate_ids
        if str(candidate_id or "").strip()
    }
    if not selected_set:
        return []
    visible_ids = _visible_stock_alias_candidate_ids(rows)
    return [
        candidate_id for candidate_id in visible_ids if candidate_id in selected_set
    ]


def _apply_selected_candidate_flags(
    rows: list[dict[str, str]],
    selected_candidate_ids: list[str],
) -> list[dict[str, str]]:
    selected_set = {
        str(candidate_id or "").strip()
        for candidate_id in selected_candidate_ids
        if str(candidate_id or "").strip()
    }
    out: list[dict[str, str]] = []
    for row in rows:
        next_row = dict(row)
        next_row["selected"] = (
            str(next_row.get("candidate_id") or "").strip() in selected_set
        )
        out.append(next_row)
    return out


class OrganizerState(rx.State):
    search_query: str = ""
    search_results: list[dict[str, str]] = []
    active_section: str = SECTION_STOCK_ALIAS
    pending_rows: list[dict[str, str]] = []
    candidate_action_pending_id: str = ""
    selected_candidate_ids: list[str] = []
    loading: bool = False
    loaded_once: bool = False
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

    @rx.var
    def has_candidate_action_pending(self) -> bool:
        return bool(str(self.candidate_action_pending_id or "").strip())

    @rx.var
    def selected_stock_alias_candidate_count(self) -> int:
        return len(self.selected_candidate_ids)

    @rx.var
    def has_selected_stock_alias_candidates(self) -> bool:
        return bool(self.selected_candidate_ids)

    @rx.var
    def show_loading(self) -> bool:
        return bool(self.loading or not self.loaded_once)

    @rx.var
    def show_pending_empty(self) -> bool:
        return bool(
            self.loaded_once
            and (not self.loading)
            and (str(self.load_error or "").strip() == "")
            and (not self.pending_rows)
        )

    @rx.event
    def set_search_query(self, value: str) -> None:
        self.search_query = str(value or "")

    @rx.event
    def run_search(self) -> None:
        self.search_results, self.load_error = load_search_results(self.search_query)

    @rx.event
    def load_pending(self):
        yield from self._reload_pending_rows_with_loading()

    @rx.event
    def set_active_section(self, value: str):
        self.active_section = str(value or SECTION_STOCK_ALIAS)
        self.candidate_action_pending_id = ""
        self.selected_candidate_ids = []
        if self.active_section == SECTION_ALIAS_MANUAL:
            self.alias_task_limit = ALIAS_TASK_PAGE_LIMIT
        yield from self._reload_pending_rows_with_loading()

    @rx.event
    def accept_candidate(self, candidate_id: str):
        yield from self._mutate_candidate_with_loading(candidate_id, action="accept")

    @rx.event
    def ignore_candidate(self, candidate_id: str):
        yield from self._mutate_candidate_with_loading(candidate_id, action="ignore")

    @rx.event
    def block_candidate(self, candidate_id: str):
        yield from self._mutate_candidate_with_loading(candidate_id, action="block")

    @rx.event
    def toggle_stock_alias_candidate(self, candidate_id: str, checked: bool) -> None:
        if self.active_section != SECTION_STOCK_ALIAS:
            return
        target = str(candidate_id or "").strip()
        if not target:
            return
        selected = list(self.selected_candidate_ids)
        if checked:
            if target not in selected:
                selected.append(target)
        else:
            selected = [item for item in selected if item != target]
        self.selected_candidate_ids = _sync_selected_candidate_ids(
            self.pending_rows,
            selected,
        )
        self.pending_rows = _apply_selected_candidate_flags(
            self.pending_rows,
            self.selected_candidate_ids,
        )

    @rx.event
    def select_all_stock_alias_candidates(self) -> None:
        if self.active_section != SECTION_STOCK_ALIAS:
            return
        self.selected_candidate_ids = _visible_stock_alias_candidate_ids(
            self.pending_rows
        )
        self.pending_rows = _apply_selected_candidate_flags(
            self.pending_rows,
            self.selected_candidate_ids,
        )

    @rx.event
    def clear_selected_stock_alias_candidates(self) -> None:
        self.selected_candidate_ids = []
        self.pending_rows = _apply_selected_candidate_flags(self.pending_rows, [])

    @rx.event
    def batch_accept_selected_candidates(self):
        yield from self._mutate_selected_candidates_with_loading(action="accept")

    @rx.event
    def batch_ignore_selected_candidates(self):
        yield from self._mutate_selected_candidates_with_loading(action="ignore")

    @rx.event
    def batch_block_selected_candidates(self):
        yield from self._mutate_selected_candidates_with_loading(action="block")

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
    def confirm_alias_manual_merge(self):
        alias_key = str(self.alias_manual_alias_key or "").strip()
        target_key = _parse_manual_alias_target_stock_key(
            str(self.alias_manual_target_input or "")
        )
        if not alias_key:
            return
        if not target_key:
            self.alias_manual_error = "请输入股票代码，比如 601899.SH"
            return
        self._start_loading()
        yield
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
        finally:
            self._finish_loading()
        clear_reflex_source_caches()
        self.close_alias_manual_dialog()
        self._reload_pending_rows()

    @rx.event
    def block_alias_manual(self):
        alias_key = str(self.alias_manual_alias_key or "").strip()
        if not alias_key:
            return
        self._start_loading()
        yield
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
        finally:
            self._finish_loading()
        clear_reflex_source_caches()
        self.close_alias_manual_dialog()
        self._reload_pending_rows()

    @rx.event
    def preview_alias_ai_batch(self):
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
        self._start_loading()
        yield
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
        finally:
            self._finish_loading()

    @rx.event
    def load_more_alias_tasks(self):
        if self.active_section != SECTION_ALIAS_MANUAL:
            return
        self.alias_task_limit = (
            max(0, int(self.alias_task_limit)) + ALIAS_TASK_PAGE_STEP
        )
        yield from self._reload_pending_rows_with_loading()

    def _reload_pending_rows(self) -> None:
        pending_rows, load_error = load_pending_rows(
            self.active_section,
            alias_task_limit=self.alias_task_limit,
        )
        if self.active_section == SECTION_ALIAS_MANUAL:
            pending_rows = _merge_alias_ai_preview_rows(self.pending_rows, pending_rows)
            self.selected_candidate_ids = []
        elif self.active_section == SECTION_STOCK_ALIAS:
            self.selected_candidate_ids = _sync_selected_candidate_ids(
                pending_rows,
                self.selected_candidate_ids,
            )
            pending_rows = _apply_selected_candidate_flags(
                pending_rows,
                self.selected_candidate_ids,
            )
        else:
            self.selected_candidate_ids = []
        self.pending_rows = pending_rows
        self.load_error = load_error

    def _start_loading(self) -> None:
        self.loading = True
        self.load_error = ""

    def _finish_loading(self) -> None:
        self.loaded_once = True
        self.loading = False

    def _reload_pending_rows_with_loading(self):
        self._start_loading()
        yield
        try:
            self._reload_pending_rows()
        finally:
            self._finish_loading()

    def _mutate_candidate_with_loading(self, candidate_id: str, *, action: str):
        if self.active_section == SECTION_STOCK_ALIAS:
            self.candidate_action_pending_id = str(candidate_id or "").strip()
            yield
            try:
                applied = apply_candidate_action_by_id(
                    rows=self.pending_rows,
                    candidate_id=candidate_id,
                    action=action,
                )
                if not applied:
                    return
                clear_reflex_source_caches()
                self.pending_rows = _remove_candidate_row_by_id(
                    self.pending_rows,
                    candidate_id=candidate_id,
                )
                self.selected_candidate_ids = _sync_selected_candidate_ids(
                    self.pending_rows,
                    self.selected_candidate_ids,
                )
                self.pending_rows = _apply_selected_candidate_flags(
                    self.pending_rows,
                    self.selected_candidate_ids,
                )
                self.load_error = ""
            finally:
                self.candidate_action_pending_id = ""
            return
        self._start_loading()
        yield
        try:
            applied = apply_candidate_action_by_id(
                rows=self.pending_rows,
                candidate_id=candidate_id,
                action=action,
            )
            if not applied:
                return
            clear_reflex_source_caches()
            self.pending_rows, self.load_error = load_pending_rows(self.active_section)
        finally:
            self._finish_loading()

    def _mutate_selected_candidates_with_loading(self, *, action: str):
        if self.active_section != SECTION_STOCK_ALIAS:
            return
        selected_candidate_ids = list(self.selected_candidate_ids)
        if not selected_candidate_ids:
            return
        succeeded_candidate_ids: list[str] = []
        first_error = ""
        for candidate_id in selected_candidate_ids:
            self.candidate_action_pending_id = str(candidate_id or "").strip()
            yield
            try:
                applied = apply_candidate_action_by_id(
                    rows=self.pending_rows,
                    candidate_id=candidate_id,
                    action=action,
                )
            except BaseException as err:
                if isinstance(err, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                    raise
                if not first_error:
                    first_error = str(err)
                continue
            if not applied:
                continue
            succeeded_candidate_ids.append(str(candidate_id or "").strip())
            self.pending_rows = _remove_candidate_row_by_id(
                self.pending_rows,
                candidate_id=candidate_id,
            )
        if succeeded_candidate_ids:
            clear_reflex_source_caches()
        self.selected_candidate_ids = _sync_selected_candidate_ids(
            self.pending_rows,
            self.selected_candidate_ids,
        )
        self.pending_rows = _apply_selected_candidate_flags(
            self.pending_rows,
            self.selected_candidate_ids,
        )
        self.load_error = first_error
        self.candidate_action_pending_id = ""


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
    section_key = str(section or SECTION_ALIAS_MANUAL).strip()
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

    if section_key == SECTION_STOCK_ALIAS:
        candidate_rows, candidate_err = load_stock_alias_candidates_from_env()
        if candidate_err:
            return [], candidate_err
        return _filter_known_candidate_statuses(candidate_rows), ""

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
    return _stock_candidates_for_relation_type(
        assertions,
        relation_type=SECTION_STOCK_ALIAS,
    )


def _stock_sector_candidates(assertions) -> list[dict[str, str]]:
    return _stock_candidates_for_relation_type(
        assertions,
        relation_type=SECTION_STOCK_SECTOR,
    )


def _stock_candidates_for_relation_type(
    assertions,
    *,
    relation_type: str,
) -> list[dict[str, str]]:
    stock_index = build_stock_object_index(assertions)
    out: list[dict[str, str]] = []
    for stock_key in _unique_stock_keys(assertions):
        out.extend(
            build_stock_pending_candidates(
                assertions,
                stock_key=stock_key,
                ai_enabled=False,
                relation_type=relation_type,
                stock_index=stock_index,
            )
        )
        if len(out) >= SECTION_CANDIDATE_LIMIT:
            break
    return out[:SECTION_CANDIDATE_LIMIT]


def _sector_relation_candidates(assertions) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for sector_key in _unique_sector_keys(assertions):
        out.extend(
            build_sector_pending_candidates(
                assertions, sector_key=sector_key, ai_enabled=False
            )
        )
    return out[:SECTION_CANDIDATE_LIMIT]


def _unique_stock_keys(assertions) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for row in assertions:
        raw_key = row.get("entity_key")
        stock_key = str(raw_key or "").strip()
        if not stock_key.startswith("stock:") or stock_key in seen:
            continue
        seen.add(stock_key)
        out.append(stock_key)
    return out


def _unique_sector_keys(assertions) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for row in assertions:
        item = row.get("cluster_keys")
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
