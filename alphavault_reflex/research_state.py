from __future__ import annotations

from dataclasses import asdict

import reflex as rx

from alphavault.research_workbench import (
    accept_relation_candidate,
    ensure_research_workbench_schema,
    get_research_workbench_engine_from_env,
    ignore_relation_candidate,
    list_candidate_status_map,
    upsert_relation_candidate,
    block_relation_candidate,
)
from alphavault_reflex.services.research_data import (
    build_sector_pending_candidates,
    build_sector_research_view,
    build_stock_pending_candidates,
    build_stock_research_view,
)
from alphavault_reflex.services.research_models import (
    build_sector_route,
    build_stock_route,
)
from alphavault_reflex.services.turso_read import (
    clear_reflex_source_caches,
    load_sources_from_env,
)


def load_stock_page_view(stock_slug: str) -> dict[str, object]:
    stock_key = _normalize_stock_key(stock_slug)
    posts, assertions, err = load_sources_from_env()
    if err:
        return {
            "header_title": stock_key.removeprefix("stock:"),
            "signals": [],
            "related_sectors": [],
            "pending_candidates": [],
            "load_error": err,
        }
    view = build_stock_research_view(posts, assertions, stock_key=stock_key)
    result = asdict(view)
    result["pending_candidates"] = _filter_pending_candidates(
        build_stock_pending_candidates(
            assertions,
            stock_key=stock_key,
            ai_enabled=True,
        )
    )
    result["load_error"] = ""
    return result


def load_sector_page_view(sector_slug: str) -> dict[str, object]:
    sector_key = str(sector_slug or "").strip()
    posts, assertions, err = load_sources_from_env()
    if err:
        return {
            "header_title": sector_key,
            "signals": [],
            "related_stocks": [],
            "pending_candidates": [],
            "load_error": err,
        }
    view = build_sector_research_view(posts, assertions, sector_key=sector_key)
    result = asdict(view)
    result["pending_candidates"] = _filter_pending_candidates(
        build_sector_pending_candidates(
            assertions,
            sector_key=sector_key,
            ai_enabled=True,
        )
    )
    result["load_error"] = ""
    return result


def apply_candidate_action(candidate_row: dict[str, str], action: str) -> None:
    engine = get_research_workbench_engine_from_env()
    ensure_research_workbench_schema(engine)
    upsert_relation_candidate(
        engine,
        candidate_id=str(candidate_row.get("candidate_id") or "").strip(),
        relation_type=str(candidate_row.get("relation_type") or "").strip(),
        left_key=str(candidate_row.get("left_key") or "").strip(),
        right_key=str(candidate_row.get("right_key") or "").strip(),
        relation_label=str(candidate_row.get("relation_label") or "").strip(),
        suggestion_reason=str(candidate_row.get("suggestion_reason") or "").strip(),
        evidence_summary=str(candidate_row.get("evidence_summary") or "").strip(),
        score=float(str(candidate_row.get("score") or "0") or 0),
        ai_status=str(candidate_row.get("ai_status") or "").strip(),
    )
    action_name = str(action or "").strip()
    candidate_id = str(candidate_row.get("candidate_id") or "").strip()
    if action_name == "accept":
        accept_relation_candidate(engine, candidate_id=candidate_id, source="manual")
        return
    if action_name == "ignore":
        ignore_relation_candidate(engine, candidate_id=candidate_id)
        return
    if action_name == "block":
        block_relation_candidate(engine, candidate_id=candidate_id)


class ResearchState(rx.State):
    page_title: str = ""
    entity_key: str = ""
    entity_type: str = ""
    load_error: str = ""
    primary_signals: list[dict[str, str]] = []
    related_items: list[dict[str, str]] = []
    pending_candidates: list[dict[str, str]] = []

    @rx.var
    def has_signals(self) -> bool:
        return bool(self.primary_signals)

    @rx.var
    def has_pending_candidates(self) -> bool:
        return bool(self.pending_candidates)

    @rx.var
    def has_related_items(self) -> bool:
        return bool(self.related_items)

    @rx.event
    def load_stock_page(self, stock_slug: str | None = None) -> None:
        slug = _resolve_route_slug(
            self, explicit_slug=stock_slug, route_key="stock_slug"
        )
        stock_key = _normalize_stock_key(slug)
        view = load_stock_page_view(slug)
        self.page_title = str(view.get("header_title") or "").strip()
        self.entity_key = stock_key
        self.entity_type = "stock"
        self.load_error = str(view.get("load_error") or "").strip()
        self.primary_signals = _coerce_rows(view.get("signals"))
        self.related_items = _prepare_sector_links(view.get("related_sectors"))
        self.pending_candidates = _coerce_rows(view.get("pending_candidates"))

    @rx.event
    def load_sector_page(self, sector_slug: str | None = None) -> None:
        slug = _resolve_route_slug(
            self,
            explicit_slug=sector_slug,
            route_key="sector_slug",
        )
        sector_key = str(slug or "").strip()
        view = load_sector_page_view(sector_key)
        self.page_title = str(view.get("header_title") or "").strip()
        self.entity_key = f"cluster:{sector_key}" if sector_key else ""
        self.entity_type = "sector"
        self.load_error = str(view.get("load_error") or "").strip()
        self.primary_signals = _coerce_rows(view.get("signals"))
        self.related_items = _prepare_stock_links(view.get("related_stocks"))
        self.pending_candidates = _coerce_rows(view.get("pending_candidates"))

    @rx.event
    def accept_candidate(self, candidate_id: str) -> None:
        self._mutate_candidate(candidate_id, action="accept")

    @rx.event
    def ignore_candidate(self, candidate_id: str) -> None:
        self._mutate_candidate(candidate_id, action="ignore")

    @rx.event
    def block_candidate(self, candidate_id: str) -> None:
        self._mutate_candidate(candidate_id, action="block")

    def _mutate_candidate(self, candidate_id: str, *, action: str) -> None:
        target = str(candidate_id or "").strip()
        if not target:
            return
        row = next(
            (
                item
                for item in self.pending_candidates
                if str(item.get("candidate_id") or "").strip() == target
            ),
            None,
        )
        if row is None:
            return
        apply_candidate_action(row, action)
        clear_reflex_source_caches()
        if self.entity_type == "stock":
            self.load_stock_page(self.entity_key.removeprefix("stock:"))
            return
        if self.entity_type == "sector":
            self.load_sector_page(self.entity_key.removeprefix("cluster:"))
            return
        self.pending_candidates = [
            item
            for item in self.pending_candidates
            if str(item.get("candidate_id") or "").strip() != target
        ]


def _resolve_route_slug(
    state: ResearchState,
    *,
    explicit_slug: str | None,
    route_key: str,
) -> str:
    slug = str(explicit_slug or "").strip()
    if slug:
        return slug
    params = getattr(getattr(state.router, "page", None), "params", None)
    if params is not None:
        getter = getattr(params, "get", None)
        if callable(getter):
            slug = str(getter(route_key, "") or "").strip()
            if slug:
                return slug
        route_value = getattr(params, route_key, "")
        slug = str(route_value or "").strip()
        if slug:
            return slug
    route_value = getattr(state, route_key, "")
    return str(route_value or "").strip()


def _normalize_stock_key(stock_slug: str) -> str:
    slug = str(stock_slug or "").strip()
    if not slug:
        return ""
    if slug.startswith("stock:"):
        return slug
    return f"stock:{slug}"


def _coerce_rows(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                str(key): str(raw or "").strip()
                for key, raw in item.items()
                if str(key).strip()
            }
        )
    return out


def _prepare_sector_links(value: object) -> list[dict[str, str]]:
    rows = _coerce_rows(value)
    out: list[dict[str, str]] = []
    for row in rows:
        sector_key = str(row.get("sector_key") or "").strip()
        if not sector_key:
            continue
        out.append(
            {
                **row,
                "label": sector_key,
                "href": build_sector_route(f"cluster:{sector_key}"),
            }
        )
    return out


def _prepare_stock_links(value: object) -> list[dict[str, str]]:
    rows = _coerce_rows(value)
    out: list[dict[str, str]] = []
    for row in rows:
        stock_key = str(row.get("stock_key") or "").strip()
        if not stock_key:
            continue
        out.append(
            {
                **row,
                "label": stock_key.removeprefix("stock:"),
                "href": build_stock_route(stock_key),
            }
        )
    return out


def _filter_pending_candidates(rows: list[dict[str, str]]) -> list[dict[str, str]]:
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
