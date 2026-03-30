from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import os
from urllib.parse import unquote

import reflex as rx
from reflex import constants as rx_constants

from alphavault.db.turso_db import (
    ensure_turso_engine,
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
from alphavault.research_stock_cache import mark_stock_dirty
from alphavault.research_backfill_cache import (
    mark_stock_backfill_dirty_from_assertions,
)
from alphavault_reflex.services.research_data import (
    build_sector_research_view,
)
from alphavault_reflex.services.research_models import (
    build_sector_route,
    build_stock_route,
)
from alphavault_reflex.services.turso_read import (
    clear_reflex_source_caches,
    load_sources_from_env,
)
from alphavault_reflex.services.stock_hot_read import (
    clear_stock_hot_read_caches,
    load_stock_cached_view_from_env,
)
from alphavault_reflex.services.research_status_text import (
    STOCK_CACHE_PREPARING_HINTS,
)
from alphavault.env import load_dotenv_if_present
from alphavault_reflex.services.stock_backfill import (
    BACKFILL_PROMPT_VERSION,
    merge_post_assertions,
    run_targeted_stock_backfill,
)

DEFAULT_SIGNAL_PAGE_SIZE = 5
SIGNAL_PAGE_SIZE_OPTIONS = (5, 10, 20)
_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _empty_stock_page_view(
    stock_key: str,
    *,
    signal_page_size: int,
    load_error: str = "",
) -> dict[str, object]:
    return {
        "entity_key": stock_key,
        "header_title": stock_key.removeprefix("stock:"),
        "signals": [],
        "signal_total": 0,
        "signal_page": 1,
        "signal_page_size": _normalize_signal_page_size(signal_page_size),
        "related_sectors": [],
        "pending_candidates": [],
        "backfill_posts": [],
        "load_error": str(load_error or "").strip(),
    }


def load_stock_page_cached_view(
    stock_slug: str,
    *,
    signal_page: int,
    signal_page_size: int,
) -> dict[str, object]:
    stock_key = _normalize_stock_key(stock_slug)
    view = load_stock_cached_view_from_env(
        stock_key,
        signal_page=_normalize_signal_page(signal_page),
        signal_page_size=_normalize_signal_page_size(signal_page_size),
    )
    if str(view.get("entity_key") or "").strip() == "":
        return _empty_stock_page_view(
            stock_key,
            signal_page_size=signal_page_size,
            load_error=str(view.get("load_error") or "").strip(),
        )
    return view


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
    mark_stock_dirty(
        engine,
        stock_key=target_stock_key,
        reason="direct_backfill",
    )
    mark_stock_backfill_dirty_from_assertions(
        engine,
        assertions=merged,
        reason="direct_backfill",
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


def _is_stock_cache_preparing_warning(warning: str) -> bool:
    text = str(warning or "").strip()
    if not text:
        return False
    return any(hint in text for hint in STOCK_CACHE_PREPARING_HINTS)


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
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        result["pending_candidates"] = []
    result["load_error"] = ""
    return result


def apply_candidate_action(candidate_row: dict[str, str], action: str) -> None:
    engine = get_research_workbench_engine_from_env()
    ensure_research_workbench_schema(engine)
    left_key = str(candidate_row.get("left_key") or "").strip()
    upsert_relation_candidate(
        engine,
        candidate_id=str(candidate_row.get("candidate_id") or "").strip(),
        relation_type=str(candidate_row.get("relation_type") or "").strip(),
        left_key=left_key,
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
    elif action_name == "ignore":
        ignore_relation_candidate(engine, candidate_id=candidate_id)
    elif action_name == "block":
        block_relation_candidate(engine, candidate_id=candidate_id)
    if left_key.startswith("stock:"):
        mark_stock_dirty(engine, stock_key=left_key, reason="candidate_action")


class ResearchState(rx.State):
    loading: bool = False
    extras_loading: bool = False
    signals_ready: bool = False
    extras_ready: bool = False
    extras_updated_at: str = ""
    worker_status_text: str = ""
    worker_next_run_at: str = ""
    worker_cycle_updated_at: str = ""
    worker_running: bool = False
    loaded_once: bool = False
    page_title: str = ""
    entity_key: str = ""
    entity_type: str = ""
    load_error: str = ""
    stock_load_warning: str = ""
    stock_load_request_id: int = 0
    primary_signals: list[dict[str, str]] = []
    related_items: list[dict[str, str]] = []
    pending_candidates: list[dict[str, str]] = []
    backfill_posts: list[dict[str, str]] = []
    backfill_notice: str = ""
    signal_page: int = 1
    signal_page_size: int = DEFAULT_SIGNAL_PAGE_SIZE
    signal_total: int = 0

    @rx.var
    def has_signals(self) -> bool:
        return bool(self.primary_signals)

    @rx.var
    def show_loading(self) -> bool:
        return bool(self.loading or not self.loaded_once)

    @rx.var
    def show_extras_loading(self) -> bool:
        return bool(self.extras_loading)

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
            and (not self.extras_loading)
            and (str(self.load_error or "").strip() == "")
            and (not self.pending_candidates)
        )

    @rx.var
    def show_backfill_empty(self) -> bool:
        return bool(
            self.loaded_once
            and (not self.loading)
            and (not self.extras_loading)
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

    @rx.var
    def signal_page_size_options(self) -> list[str]:
        return [str(size) for size in SIGNAL_PAGE_SIZE_OPTIONS]

    @rx.var
    def signal_page_size_text(self) -> str:
        return str(_normalize_signal_page_size(self.signal_page_size))

    @rx.var
    def signal_total_pages(self) -> int:
        total = max(int(self.signal_total or 0), 0)
        size = max(int(self.signal_page_size or DEFAULT_SIGNAL_PAGE_SIZE), 1)
        if total <= 0:
            return 1
        return max(1, (total + size - 1) // size)

    @rx.var
    def signal_page_caption(self) -> str:
        total = max(int(self.signal_total or 0), 0)
        if total <= 0:
            return ""
        page = max(int(self.signal_page or 1), 1)
        pages = max(int(self.signal_total_pages or 1), 1)
        return f"第{page}页 / 共{pages}页（{total}条）"

    @rx.event
    def load_stock_page(self, stock_slug: str | None = None):
        self.loading = True
        slug = _resolve_route_slug(
            self, explicit_slug=stock_slug, route_key="stock_slug"
        )
        if self.entity_type != "stock" or _normalize_stock_key(slug) != self.entity_key:
            self.signal_page = 1
        stock_key = _normalize_stock_key(slug)
        self.stock_load_request_id = max(int(self.stock_load_request_id), 0) + 1
        self.entity_type = "stock"
        self.entity_key = stock_key
        self.load_error = ""
        self.stock_load_warning = ""
        self.extras_loading = False
        self.signals_ready = False
        self.extras_ready = False
        self.extras_updated_at = ""
        self.worker_status_text = ""
        self.worker_next_run_at = ""
        self.worker_cycle_updated_at = ""
        self.worker_running = False
        self.pending_candidates = []
        self.backfill_posts = []

        view = load_stock_page_cached_view(
            slug,
            signal_page=_normalize_signal_page(self.signal_page),
            signal_page_size=_normalize_signal_page_size(self.signal_page_size),
        )
        self._apply_stock_primary_view(view, fallback_stock_key=stock_key)
        self.pending_candidates = _coerce_rows(view.get("pending_candidates"))
        self.backfill_posts = _coerce_rows(view.get("backfill_posts"))
        self.extras_updated_at = str(view.get("extras_updated_at") or "").strip()
        self.worker_status_text = str(view.get("worker_status_text") or "").strip()
        self.worker_next_run_at = str(view.get("worker_next_run_at") or "").strip()
        self.worker_cycle_updated_at = str(
            view.get("worker_cycle_updated_at") or ""
        ).strip()
        self.worker_running = bool(view.get("worker_running"))
        self.loaded_once = True
        self.loading = False
        cache_preparing = _is_stock_cache_preparing_warning(self.stock_load_warning)
        self.signals_ready = bool(
            str(self.load_error or "").strip() == ""
            and self.loaded_once
            and (not cache_preparing)
        )
        self.extras_ready = bool(
            self.loaded_once
            and (not self.extras_loading)
            and (
                self.extras_updated_at != ""
                or bool(self.pending_candidates)
                or bool(self.backfill_posts)
            )
        )

    def _apply_stock_primary_view(
        self, view: dict[str, object], *, fallback_stock_key: str
    ) -> None:
        self.page_title = str(view.get("header_title") or "").strip()
        self.entity_key = str(view.get("entity_key") or fallback_stock_key).strip()
        self.entity_type = "stock"
        self.load_error = str(view.get("load_error") or "").strip()
        self.primary_signals = _coerce_rows(view.get("signals"))
        self.signal_total = _normalize_signal_total(
            view.get("signal_total"),
            fallback=len(self.primary_signals),
        )
        self.signal_page = _normalize_signal_page(view.get("signal_page") or 1)
        self.signal_page_size = _normalize_signal_page_size(
            view.get("signal_page_size") or self.signal_page_size
        )
        self.related_items = _prepare_sector_links(view.get("related_sectors"))
        warning = str(view.get("load_warning") or "").strip()
        if warning:
            self.stock_load_warning = warning

    @rx.event
    def prev_signal_page(self):
        if int(self.signal_page or 1) <= 1:
            return
        self.signal_page = max(int(self.signal_page) - 1, 1)
        return self.load_stock_page(self.entity_key.removeprefix("stock:"))

    @rx.event
    def next_signal_page(self):
        page = max(int(self.signal_page or 1), 1)
        total_pages = max(int(self.signal_total_pages or 1), 1)
        if page >= total_pages:
            return
        self.signal_page = page + 1
        return self.load_stock_page(self.entity_key.removeprefix("stock:"))

    @rx.event
    def set_signal_page_size(self, value: str):
        self.signal_page_size = _normalize_signal_page_size(value)
        self.signal_page = 1
        return self.load_stock_page(self.entity_key.removeprefix("stock:"))

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
        self.stock_load_warning = ""
        self.extras_loading = False
        self.signals_ready = True
        self.extras_ready = True
        self.extras_updated_at = ""
        self.worker_status_text = ""
        self.worker_next_run_at = ""
        self.worker_cycle_updated_at = ""
        self.worker_running = False
        self.primary_signals = _coerce_rows(view.get("signals"))
        self.related_items = _prepare_stock_links(view.get("related_stocks"))
        self.pending_candidates = _coerce_rows(view.get("pending_candidates"))
        self.backfill_posts = []
        self.loaded_once = True
        self.loading = False

    @rx.event
    def accept_candidate(self, candidate_id: str):
        return self._mutate_candidate(candidate_id, action="accept")

    @rx.event
    def ignore_candidate(self, candidate_id: str):
        return self._mutate_candidate(candidate_id, action="ignore")

    @rx.event
    def block_candidate(self, candidate_id: str):
        return self._mutate_candidate(candidate_id, action="block")

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
        clear_stock_hot_read_caches()
        if self.entity_key.startswith("stock:"):
            try:
                engine = _get_turso_engine_for_post_uid(target)
                mark_stock_dirty(
                    engine,
                    stock_key=self.entity_key,
                    reason="queue_backfill",
                )
            except BaseException:
                pass
        self.backfill_notice = f"已排队：{target}（等一会再刷新）"

    def _mutate_candidate(self, candidate_id: str, *, action: str):
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
        clear_stock_hot_read_caches()
        if self.entity_type == "stock":
            return self.load_stock_page(self.entity_key.removeprefix("stock:"))
        if self.entity_type == "sector":
            return self.load_sector_page(self.entity_key.removeprefix("cluster:"))
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


def _normalize_signal_page(value: object) -> int:
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        return 1
    if parsed <= 0:
        return 1
    return int(parsed)


def _normalize_signal_page_size(value: object) -> int:
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        return DEFAULT_SIGNAL_PAGE_SIZE
    if parsed in SIGNAL_PAGE_SIZE_OPTIONS:
        return int(parsed)
    return DEFAULT_SIGNAL_PAGE_SIZE


def _normalize_signal_total(value: object, *, fallback: int) -> int:
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        return max(int(fallback), 0)
    return max(parsed, 0)


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
