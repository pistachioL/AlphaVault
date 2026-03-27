from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import os
from urllib.parse import unquote

import reflex as rx
from reflex import constants as rx_constants

from alphavault.db.turso_db import (
    ensure_turso_engine,
    get_turso_engine_from_env,
    turso_connect_autocommit,
)
from alphavault.db.turso_env import (
    infer_platform_from_post_uid,
    require_turso_source_from_env,
)
from alphavault.db.turso_queue import (
    ensure_cloud_queue_schema,
    reset_ai_results_for_post_uids,
    write_assertions_and_mark_done,
)
from alphavault.research_workbench import (
    accept_relation_candidate,
    ensure_research_workbench_schema,
    get_research_workbench_engine_from_env,
    ignore_relation_candidate,
    list_pending_candidates_for_left_key,
    upsert_relation_candidate,
    block_relation_candidate,
)
from alphavault.research_backfill_cache import list_stock_backfill_posts
from alphavault_reflex.services.research_data import (
    build_sector_research_view,
    build_stock_research_view,
)
from alphavault_reflex.services.research_models import (
    build_sector_route,
    build_stock_route,
)
from alphavault_reflex.services.turso_read import (
    clear_reflex_source_caches,
    load_stock_alias_relations_from_env,
    load_sources_from_env,
)
from alphavault.env import load_dotenv_if_present
from alphavault_reflex.services.stock_backfill import (
    BACKFILL_PROMPT_VERSION,
    merge_post_assertions,
    run_targeted_stock_backfill,
)


def load_stock_page_view(stock_slug: str) -> dict[str, object]:
    stock_key = _normalize_stock_key(stock_slug)
    posts, assertions, err = load_sources_from_env()
    if err:
        return {
            "entity_key": stock_key,
            "header_title": stock_key.removeprefix("stock:"),
            "signals": [],
            "related_sectors": [],
            "pending_candidates": [],
            "backfill_posts": [],
            "load_error": err,
        }
    stock_relations, relation_err = load_stock_alias_relations_from_env()
    if relation_err:
        stock_relations = None
    view = build_stock_research_view(
        posts,
        assertions,
        stock_key=stock_key,
        stock_relations=stock_relations,
    )
    result = asdict(view)
    entity_key = str(result.get("entity_key") or stock_key).strip()
    try:
        workbench = get_research_workbench_engine_from_env()
        ensure_research_workbench_schema(workbench)
        result["pending_candidates"] = list_pending_candidates_for_left_key(
            workbench,
            left_key=entity_key,
            limit=12,
        )
    except Exception:
        result["pending_candidates"] = []
    try:
        engine = get_turso_engine_from_env()
        result["backfill_posts"] = list_stock_backfill_posts(
            engine,
            stock_key=entity_key,
            limit=12,
        )
    except Exception:
        result["backfill_posts"] = []
    result["load_error"] = ""
    return result


def _get_turso_engine_for_post_uid(post_uid: str):
    load_dotenv_if_present()
    platform = infer_platform_from_post_uid(post_uid)
    if not platform:
        raise RuntimeError("unknown_post_platform")
    source = require_turso_source_from_env(platform)
    return ensure_turso_engine(source.url, source.token)


def queue_post_for_ai_backfill(post_uid: str) -> None:
    target = str(post_uid or "").strip()
    if not target:
        return
    engine = _get_turso_engine_for_post_uid(target)
    ensure_cloud_queue_schema(engine, verbose=False)
    archived_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    reset_ai_results_for_post_uids(
        engine,
        post_uids=[target],
        archived_at=archived_at,
        chunk_size=1,
    )


def run_direct_stock_backfill(post_uid: str, stock_key: str, display_name: str) -> int:
    target_post_uid = str(post_uid or "").strip()
    target_stock_key = str(stock_key or "").strip()
    if not target_post_uid or not target_stock_key:
        return 0
    posts, _assertions, err = load_sources_from_env()
    if err:
        raise RuntimeError(err)
    if posts.empty:
        raise RuntimeError("posts_empty")
    matched = posts[posts["post_uid"].astype(str).str.strip() == target_post_uid]
    if matched.empty:
        raise RuntimeError("post_not_found")
    post_row = {
        str(key): str(value or "").strip()
        for key, value in matched.iloc[0].to_dict().items()
    }
    new_assertions = run_targeted_stock_backfill(
        post_row,
        stock_key=target_stock_key,
        display_name=display_name,
    )
    if not new_assertions:
        return 0
    engine = _get_turso_engine_for_post_uid(target_post_uid)
    ensure_cloud_queue_schema(engine, verbose=False)
    existing_assertions = _load_assertions_for_post(engine, post_uid=target_post_uid)
    merged = merge_post_assertions(existing_assertions, new_assertions)
    archived_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    write_assertions_and_mark_done(
        engine,
        post_uid=target_post_uid,
        final_status="relevant",
        invest_score=1.0,
        processed_at=archived_at,
        model=os.getenv("AI_MODEL", "").strip() or "targeted-stock-backfill",
        prompt_version=BACKFILL_PROMPT_VERSION,
        archived_at=archived_at,
        ai_result_json=None,
        assertions=merged,
    )
    return max(0, len(merged) - len(existing_assertions))


def _load_assertions_for_post(engine, *, post_uid: str) -> list[dict[str, object]]:
    target = str(post_uid or "").strip()
    if not target:
        return []
    sql = """
SELECT topic_key, action, action_strength, summary, evidence, confidence,
       stock_codes_json, stock_names_json, industries_json, commodities_json, indices_json
FROM assertions
WHERE post_uid = :post_uid
ORDER BY idx ASC
"""
    with turso_connect_autocommit(engine) as conn:
        rows = conn.execute(sql, {"post_uid": target}).mappings().all()
    return [
        {
            "topic_key": str(row.get("topic_key") or "").strip(),
            "action": str(row.get("action") or "").strip(),
            "action_strength": int(row.get("action_strength") or 0),
            "summary": str(row.get("summary") or "").strip(),
            "evidence": str(row.get("evidence") or "").strip(),
            "confidence": float(row.get("confidence") or 0),
            "stock_codes_json": str(row.get("stock_codes_json") or "[]"),
            "stock_names_json": str(row.get("stock_names_json") or "[]"),
            "industries_json": str(row.get("industries_json") or "[]"),
            "commodities_json": str(row.get("commodities_json") or "[]"),
            "indices_json": str(row.get("indices_json") or "[]"),
        }
        for row in rows
    ]


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
    left_key = f"cluster:{sector_key}" if sector_key else ""
    try:
        engine = get_research_workbench_engine_from_env()
        ensure_research_workbench_schema(engine)
        result["pending_candidates"] = list_pending_candidates_for_left_key(
            engine,
            left_key=left_key,
            limit=12,
        )
    except Exception:
        result["pending_candidates"] = []
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
    loading: bool = False
    loaded_once: bool = False
    page_title: str = ""
    entity_key: str = ""
    entity_type: str = ""
    load_error: str = ""
    primary_signals: list[dict[str, str]] = []
    related_items: list[dict[str, str]] = []
    pending_candidates: list[dict[str, str]] = []
    backfill_posts: list[dict[str, str]] = []
    backfill_notice: str = ""

    @rx.var
    def has_signals(self) -> bool:
        return bool(self.primary_signals)

    @rx.var
    def show_loading(self) -> bool:
        return bool(self.loading or not self.loaded_once)

    @rx.var
    def show_signal_empty(self) -> bool:
        return bool(
            self.loaded_once
            and (not self.loading)
            and (str(self.load_error or "").strip() == "")
            and (not self.primary_signals)
        )

    @rx.var
    def show_related_empty(self) -> bool:
        return bool(
            self.loaded_once
            and (not self.loading)
            and (str(self.load_error or "").strip() == "")
            and (not self.related_items)
        )

    @rx.var
    def show_pending_empty(self) -> bool:
        return bool(
            self.loaded_once
            and (not self.loading)
            and (str(self.load_error or "").strip() == "")
            and (not self.pending_candidates)
        )

    @rx.var
    def show_backfill_empty(self) -> bool:
        return bool(
            self.loaded_once
            and (not self.loading)
            and (str(self.load_error or "").strip() == "")
            and (not self.backfill_posts)
        )

    @rx.var
    def has_pending_candidates(self) -> bool:
        return bool(self.pending_candidates)

    @rx.var
    def has_related_items(self) -> bool:
        return bool(self.related_items)

    @rx.var
    def has_backfill_posts(self) -> bool:
        return bool(self.backfill_posts)

    @rx.event
    def load_stock_page(self, stock_slug: str | None = None) -> None:
        self.loading = True
        slug = _resolve_route_slug(
            self, explicit_slug=stock_slug, route_key="stock_slug"
        )
        stock_key = _normalize_stock_key(slug)
        view = load_stock_page_view(slug)
        self.page_title = str(view.get("header_title") or "").strip()
        self.entity_key = str(view.get("entity_key") or stock_key).strip()
        self.entity_type = "stock"
        self.load_error = str(view.get("load_error") or "").strip()
        self.primary_signals = _coerce_rows(view.get("signals"))
        self.related_items = _prepare_sector_links(view.get("related_sectors"))
        self.pending_candidates = _coerce_rows(view.get("pending_candidates"))
        self.backfill_posts = _coerce_rows(view.get("backfill_posts"))
        self.loaded_once = True
        self.loading = False

    @rx.event
    def load_sector_page(self, sector_slug: str | None = None) -> None:
        self.loading = True
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
        self.loaded_once = True
        self.loading = False

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
    def queue_backfill_post(self, post_uid: str) -> None:
        target = str(post_uid or "").strip()
        if not target:
            return
        try:
            queue_post_for_ai_backfill(target)
        except BaseException as err:
            if isinstance(err, (KeyboardInterrupt, SystemExit)):
                raise
            self.backfill_notice = f"排队失败：{type(err).__name__}"
            return
        clear_reflex_source_caches()
        self.backfill_notice = f"已排队：{target}（等一会再刷新）"

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


_RESEARCH_ROUTE_ARGS = {
    "stock_slug": rx_constants.RouteArgType.SINGLE,
    "sector_slug": rx_constants.RouteArgType.SINGLE,
}
rx.State.setup_dynamic_args(_RESEARCH_ROUTE_ARGS)


def research_page_loading_var() -> rx.Var[bool]:
    return ResearchState.show_loading | (~rx.State.is_hydrated)


def stock_page_title_var() -> rx.Var[str]:
    return rx.cond(
        ResearchState.stock_slug != "",
        ResearchState.stock_slug,
        "",
    )


def stock_browser_title_var() -> rx.Var[str]:
    page_title = stock_page_title_var()
    return rx.cond(
        page_title != "",
        page_title + " | 个股研究",
        "个股研究",
    )


def sector_page_title_var() -> rx.Var[str]:
    loading = research_page_loading_var()
    return rx.cond(
        loading & (ResearchState.sector_slug != ""),
        ResearchState.sector_slug,
        ResearchState.page_title,
    )


def sector_browser_title_var() -> rx.Var[str]:
    page_title = sector_page_title_var()
    return rx.cond(
        page_title != "",
        page_title + " | 板块研究",
        "板块研究",
    )


def _resolve_route_slug(
    state: object,
    *,
    explicit_slug: str | None,
    route_key: str,
) -> str:
    slug = str(explicit_slug or "").strip()
    if slug:
        return slug
    router = getattr(state, "router", None)
    slug = _resolve_route_slug_from_url(router, route_key=route_key)
    if slug:
        return slug
    route_value = getattr(state, route_key, "")
    slug = str(route_value or "").strip()
    if slug:
        return slug
    return ""


def _resolve_route_slug_from_url(router: object, *, route_key: str) -> str:
    url = getattr(router, "url", None)
    query_parameters = getattr(url, "query_parameters", None)
    if query_parameters is not None:
        getter = getattr(query_parameters, "get", None)
        if callable(getter):
            slug = str(getter(route_key, "") or "").strip()
            if slug:
                return slug

    route_parts = _split_route_parts(getattr(router, "route_id", ""))
    path_parts = _split_route_parts(getattr(url, "path", ""))
    if len(route_parts) != len(path_parts):
        return ""

    target_part = f"[{route_key}]"
    for route_part, path_part in zip(route_parts, path_parts):
        if route_part == target_part:
            return unquote(path_part).strip()
    return ""


def _split_route_parts(value: object) -> tuple[str, ...]:
    return tuple(
        part for part in str(value or "").strip().strip("/").split("/") if part
    )


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
