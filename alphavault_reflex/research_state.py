from __future__ import annotations

import reflex as rx
from reflex import constants as rx_constants

from alphavault.research_stock_cache import mark_stock_dirty
from alphavault_reflex.services.relation_actions import apply_candidate_action
from alphavault_reflex.services.research_backfill_actions import (
    get_turso_engine_for_post_uid as _get_turso_engine_for_post_uid,
    queue_post_for_ai_backfill,
)
from alphavault_reflex.services.research_page_loader import (
    load_sector_page_view,
    load_stock_page_cached_view,
)
from alphavault_reflex.services.research_state_utils import (
    DEFAULT_SIGNAL_PAGE_SIZE,
    SIGNAL_PAGE_SIZE_OPTIONS,
    coerce_rows as _coerce_rows,
    is_stock_cache_preparing_warning as _is_stock_cache_preparing_warning,
    normalize_signal_page as _normalize_signal_page,
    normalize_signal_page_size as _normalize_signal_page_size,
    normalize_signal_total as _normalize_signal_total,
    normalize_stock_key as _normalize_stock_key,
    prepare_sector_links as _prepare_sector_links,
    prepare_stock_links as _prepare_stock_links,
    resolve_route_slug as _resolve_route_slug,
)
from alphavault_reflex.services.stock_related_feed import (
    DEFAULT_RELATED_LIMIT,
    MAX_RELATED_LIMIT,
    RELATED_FILTER_ALL,
    RELATED_FILTER_SIGNAL,
    RELATED_LIMIT_STEP,
    build_related_feed,
    normalize_related_filter,
    normalize_related_limit,
)
from alphavault_reflex.services.stock_hot_read import clear_stock_hot_read_caches
from alphavault_reflex.services.turso_read import clear_reflex_source_caches


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
    related_posts: list[dict[str, str]] = []
    related_total: int = 0
    related_filter: str = RELATED_FILTER_ALL
    related_limit: int = DEFAULT_RELATED_LIMIT

    @rx.var
    def has_signals(self) -> bool:
        return bool(self.primary_signals)

    @rx.var
    def has_related_posts(self) -> bool:
        return bool(self.related_posts)

    @rx.var
    def show_related_posts_empty(self) -> bool:
        return bool(
            self.loaded_once
            and (not self.loading)
            and (str(self.load_error or "").strip() == "")
            and (not self.related_posts)
        )

    @rx.var
    def related_has_more(self) -> bool:
        total = max(int(self.related_total or 0), 0)
        return bool(total > len(self.related_posts))

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
            self.related_filter = RELATED_FILTER_ALL
            self.related_limit = DEFAULT_RELATED_LIMIT
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
        self.related_posts = []
        self.related_total = 0

        view = load_stock_page_cached_view(
            slug,
            signal_page=1,
            signal_page_size=normalize_related_limit(self.related_limit),
        )
        self._apply_stock_primary_view(view, fallback_stock_key=stock_key)
        self.pending_candidates = _coerce_rows(view.get("pending_candidates"))
        self.backfill_posts = _coerce_rows(view.get("backfill_posts"))
        self._refresh_related_posts()
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

    def _refresh_related_posts(self) -> None:
        self.related_filter = normalize_related_filter(self.related_filter)
        self.related_limit = normalize_related_limit(self.related_limit)
        feed = build_related_feed(
            signals=self.primary_signals,
            backfill_posts=self.backfill_posts,
            related_filter=self.related_filter,
            limit=self.related_limit,
        )
        self.related_posts = feed.rows
        if self.related_filter == RELATED_FILTER_SIGNAL:
            self.related_total = max(int(self.signal_total or 0), 0)
        else:
            self.related_total = max(int(self.signal_total or 0), 0) + len(
                self.backfill_posts or []
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
    def set_related_filter(self, value: str) -> None:
        self.related_filter = normalize_related_filter(value)
        self.related_limit = DEFAULT_RELATED_LIMIT
        if self.entity_key.startswith("stock:"):
            return self.load_stock_page(self.entity_key.removeprefix("stock:"))

    @rx.event
    def load_more_related(self) -> None:
        current = normalize_related_limit(self.related_limit)
        self.related_limit = min(
            int(MAX_RELATED_LIMIT),
            current + int(RELATED_LIMIT_STEP),
        )
        if self.entity_key.startswith("stock:"):
            return self.load_stock_page(self.entity_key.removeprefix("stock:"))

    @rx.event
    def refresh_stock_related(self) -> None:
        clear_reflex_source_caches()
        clear_stock_hot_read_caches()
        if self.entity_key.startswith("stock:"):
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
