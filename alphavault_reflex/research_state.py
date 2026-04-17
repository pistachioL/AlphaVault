from __future__ import annotations

import importlib
from functools import cache
import reflex as rx
from reflex import constants as rx_constants
from types import ModuleType

from alphavault_reflex.services.research_page_loader import (
    load_sector_page_view,
    load_stock_page_cached_view,
    load_stock_sidebar_cached_view,
    load_stock_signal_detail_view,
)
from alphavault_reflex.services.analysis_feedback import (
    ENTRYPOINT_STOCK_RESEARCH,
    submit_post_analysis_feedback,
)
from alphavault_reflex.services.research_models import build_stock_route
from alphavault_reflex.services.research_state_utils import (
    DEFAULT_SIGNAL_PAGE_SIZE,
    SIGNAL_PAGE_SIZE_OPTIONS,
    coerce_rows as _coerce_rows,
    is_stock_cache_preparing_warning as _is_stock_cache_preparing_warning,
    normalize_author_filter as _normalize_author_filter,
    normalize_signal_page as _normalize_signal_page,
    normalize_signal_page_size as _normalize_signal_page_size,
    normalize_signal_total as _normalize_signal_total,
    normalize_stock_key as _normalize_stock_key,
    prepare_sector_links as _prepare_sector_links,
    prepare_stock_links as _prepare_stock_links,
    resolve_query_value as _resolve_query_value,
    resolve_route_slug as _resolve_route_slug,
)
from alphavault_reflex.services.stock_related_feed import (
    DEFAULT_RELATED_LIMIT,
    MAX_RELATED_LIMIT,
    RELATED_FILTER_ALL,
    RELATED_FILTER_SIGNAL,
    RELATED_LIMIT_STEP,
    StockRelatedPostRow,
    build_related_feed,
    normalize_related_filter,
    normalize_related_limit,
)
from alphavault_reflex.services.thread_tree_lines import build_tree_render_lines


@cache
def _load_source_read_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.source_read")


@cache
def _load_stock_hot_read_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.stock_hot_read")


class ResearchState(rx.State):
    loading: bool = False
    stock_sidebar_open: bool = False
    stock_sidebar_loaded: bool = False
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
    signal_page: int = 1
    signal_page_size: int = DEFAULT_SIGNAL_PAGE_SIZE
    signal_total: int = 0
    related_posts: list[StockRelatedPostRow] = []
    related_total: int = 0
    related_filter: str = RELATED_FILTER_ALL
    related_limit: int = DEFAULT_RELATED_LIMIT
    author_filter: str = ""
    signal_detail_open: bool = False
    signal_detail_loading: bool = False
    signal_detail_post_uid: str = ""
    signal_detail_title: str = ""
    signal_detail_raw_text: str = ""
    signal_detail_tree_text: str = ""
    signal_detail_message: str = ""
    feedback_dialog_open: bool = False
    feedback_submitting: bool = False
    feedback_post_uid: str = ""
    feedback_tag: str = ""
    feedback_note: str = ""
    feedback_error: str = ""
    feedback_success: str = ""

    def _reset_feedback_state(self, *, close_dialog: bool, clear_success: bool) -> None:
        if close_dialog:
            self.feedback_dialog_open = False
        self.feedback_submitting = False
        self.feedback_post_uid = ""
        self.feedback_tag = ""
        self.feedback_note = ""
        self.feedback_error = ""
        if clear_success:
            self.feedback_success = ""

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
    def stock_sidebar_ready(self) -> bool:
        return bool(self.entity_type != "stock" or self.stock_sidebar_loaded)

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
            and self.stock_sidebar_ready
            and (str(self.load_error or "").strip() == "")
            and (not self.related_items)
        )

    @rx.var
    def has_related_items(self) -> bool:
        return bool(self.related_items)

    @rx.var
    def has_author_filter(self) -> bool:
        return bool(str(self.author_filter or "").strip())

    @rx.var
    def author_filter_clear_href(self) -> str:
        if not str(self.entity_key or "").startswith("stock:"):
            return ""
        return build_stock_route(self.entity_key)

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

    @rx.var
    def signal_detail_tree_lines(self) -> list[dict[str, str]]:
        return build_tree_render_lines(self.signal_detail_tree_text)

    def _reset_signal_detail_state(self, *, close_dialog: bool) -> None:
        if close_dialog:
            self.signal_detail_open = False
        self.signal_detail_loading = False
        self.signal_detail_post_uid = ""
        self.signal_detail_title = ""
        self.signal_detail_raw_text = ""
        self.signal_detail_tree_text = ""
        self.signal_detail_message = ""

    @rx.event
    def load_stock_page(self, stock_slug: str | None = None, author: str | None = None):
        self.loading = True
        slug = _resolve_route_slug(
            self, explicit_slug=stock_slug, route_key="stock_slug"
        )
        author_filter = _normalize_author_filter(
            _resolve_query_value(self, explicit_value=author, query_key="author")
        )
        is_new_stock = bool(
            self.entity_type != "stock" or _normalize_stock_key(slug) != self.entity_key
        )
        author_changed = author_filter != _normalize_author_filter(self.author_filter)
        if is_new_stock or author_changed:
            self.signal_page = 1
            self.related_limit = DEFAULT_RELATED_LIMIT
            self._reset_feedback_state(close_dialog=True, clear_success=True)
            self._reset_signal_detail_state(close_dialog=True)
        if is_new_stock:
            self.related_filter = RELATED_FILTER_ALL
            self._reset_stock_sidebar_state(close_sidebar=True)
        stock_key = _normalize_stock_key(slug)
        self.stock_load_request_id = max(int(self.stock_load_request_id), 0) + 1
        self.entity_type = "stock"
        self.entity_key = stock_key
        self.author_filter = author_filter
        self.load_error = ""
        self.stock_load_warning = ""
        self.signals_ready = False
        self.worker_status_text = ""
        self.worker_next_run_at = ""
        self.worker_cycle_updated_at = ""
        self.worker_running = False
        self.related_posts = []
        self.related_total = 0

        normalized_signal_page = _normalize_signal_page(self.signal_page)
        normalized_signal_page_size = _normalize_signal_page_size(self.signal_page_size)
        if author_filter:
            view = load_stock_page_cached_view(
                slug,
                signal_page=normalized_signal_page,
                signal_page_size=normalized_signal_page_size,
                author=author_filter,
            )
        else:
            view = load_stock_page_cached_view(
                slug,
                signal_page=normalized_signal_page,
                signal_page_size=normalized_signal_page_size,
            )
        self._apply_stock_primary_view(view, fallback_stock_key=stock_key)
        self._refresh_related_posts()
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
            self.entity_type != "stock"
            or (self.stock_sidebar_loaded and (not self.extras_loading))
        )

    @rx.event
    def load_stock_page_if_needed(self, stock_slug: str | None = None):
        slug = _resolve_route_slug(
            self, explicit_slug=stock_slug, route_key="stock_slug"
        )
        stock_key = _normalize_stock_key(slug)
        author_filter = _normalize_author_filter(
            _resolve_query_value(self, explicit_value=None, query_key="author")
        )
        if (
            self.loaded_once
            and self.entity_type == "stock"
            and stock_key == self.entity_key
            and author_filter == _normalize_author_filter(self.author_filter)
        ):
            return
        self.stock_sidebar_open = False
        self.stock_sidebar_loaded = False
        if author_filter:
            return self.load_stock_page(slug, author=author_filter)
        return self.load_stock_page(slug)

    @rx.event
    def close_stock_sidebar(self) -> None:
        self.stock_sidebar_open = False

    @rx.event
    def set_feedback_dialog_open(self, value: bool) -> None:
        if value:
            self.feedback_dialog_open = True
            return
        self.close_feedback_dialog()

    @rx.event
    def open_feedback_dialog(self, post_uid: str) -> None:
        self._reset_feedback_state(close_dialog=False, clear_success=True)
        self.feedback_dialog_open = True
        self.feedback_post_uid = str(post_uid or "").strip()

    @rx.event
    def set_signal_detail_open(self, value: bool) -> None:
        if value:
            self.signal_detail_open = True
            return
        self.close_signal_detail()

    @rx.event
    def close_signal_detail(self) -> None:
        self._reset_signal_detail_state(close_dialog=True)

    @rx.event
    def open_signal_detail(self, post_uid: str, title: str = ""):
        uid = str(post_uid or "").strip()
        self.signal_detail_open = True
        self.signal_detail_loading = True
        self.signal_detail_post_uid = uid
        self.signal_detail_title = str(title or "").strip()
        self.signal_detail_raw_text = ""
        self.signal_detail_tree_text = ""
        self.signal_detail_message = ""
        if not uid:
            self.signal_detail_loading = False
            self.signal_detail_message = "没有对话流。"
            return

        yield
        detail = load_stock_signal_detail_view(uid)
        self.signal_detail_post_uid = str(detail.get("post_uid") or uid).strip()
        if not self.signal_detail_title:
            self.signal_detail_title = str(detail.get("title") or "").strip()
        self.signal_detail_raw_text = str(detail.get("raw_text") or "").strip()
        self.signal_detail_tree_text = str(detail.get("tree_text") or "").strip()
        self.signal_detail_message = str(detail.get("message") or "").strip()
        self.signal_detail_loading = False

    @rx.event
    def close_feedback_dialog(self) -> None:
        self._reset_feedback_state(close_dialog=True, clear_success=False)

    @rx.event
    def set_feedback_tag(self, value: str) -> None:
        self.feedback_tag = str(value or "").strip()
        self.feedback_error = ""

    @rx.event
    def set_feedback_note(self, value: str) -> None:
        self.feedback_note = str(value or "")
        self.feedback_error = ""

    @rx.event
    def submit_feedback(self):
        self.feedback_submitting = True
        self.feedback_error = ""
        self.feedback_success = ""
        result = submit_post_analysis_feedback(
            post_uid=str(self.feedback_post_uid or "").strip(),
            feedback_tag=str(self.feedback_tag or "").strip(),
            feedback_note=str(self.feedback_note or ""),
            entrypoint=ENTRYPOINT_STOCK_RESEARCH,
        )
        self.feedback_submitting = False
        if str(result.get("ok") or "") != "1":
            self.feedback_error = str(result.get("message") or "").strip()
            return

        success_message = str(result.get("message") or "").strip()
        self._reset_feedback_state(close_dialog=True, clear_success=False)
        _load_source_read_module().clear_reflex_source_caches()
        _load_stock_hot_read_module().clear_stock_hot_read_caches()
        if self.entity_key.startswith("stock:"):
            self.load_stock_page(
                self.entity_key.removeprefix("stock:"),
                author=self.author_filter or None,
            )
        self.feedback_success = success_message

    @rx.event
    def open_stock_sidebar(self):
        self.stock_sidebar_open = True
        if (
            self.entity_type != "stock"
            or self.stock_sidebar_loaded
            or self.extras_loading
        ):
            return
        self.extras_loading = True
        return self.load_stock_sidebar(self.entity_key)

    @rx.event
    def load_stock_sidebar(self, stock_slug: str | None = None):
        target = str(stock_slug or self.entity_key or "").strip()
        if not target:
            self.extras_loading = False
            return
        view = load_stock_sidebar_cached_view(target)
        self.related_items = _prepare_sector_links(view.get("related_sectors"))
        self.extras_updated_at = str(view.get("extras_updated_at") or "").strip()
        self.stock_sidebar_loaded = True
        self.extras_ready = True
        self.extras_loading = False

    def _refresh_related_posts(self) -> None:
        self.related_filter = normalize_related_filter(self.related_filter)
        self.related_limit = normalize_related_limit(self.related_limit)
        feed = build_related_feed(
            signals=self.primary_signals,
            related_filter=self.related_filter,
            limit=self.related_limit,
        )
        self.related_posts = [
            {
                **row,
                "author_href": (
                    build_stock_route(
                        self.entity_key,
                        author=str(row.get("author") or "").strip(),
                    )
                    if str(row.get("author") or "").strip()
                    else ""
                ),
            }
            for row in feed.rows
        ]
        if self.related_filter == RELATED_FILTER_SIGNAL:
            self.related_total = max(int(self.signal_total or 0), 0)
        else:
            self.related_total = max(int(self.signal_total or 0), 0)

    def _apply_stock_primary_view(
        self, view: dict[str, object], *, fallback_stock_key: str
    ) -> None:
        self.page_title = str(view.get("page_title") or "").strip()
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
        warning = str(view.get("load_warning") or "").strip()
        if warning:
            self.stock_load_warning = warning

    def _reset_stock_sidebar_state(self, *, close_sidebar: bool) -> None:
        if close_sidebar:
            self.stock_sidebar_open = False
        self.stock_sidebar_loaded = False
        self.extras_loading = False
        self.extras_ready = False
        self.extras_updated_at = ""
        self.related_items = []

    @rx.event
    def prev_signal_page(self):
        if int(self.signal_page or 1) <= 1:
            return
        self.signal_page = max(int(self.signal_page) - 1, 1)
        return self.load_stock_page(
            self.entity_key.removeprefix("stock:"),
            author=self.author_filter or None,
        )

    @rx.event
    def next_signal_page(self):
        page = max(int(self.signal_page or 1), 1)
        total_pages = max(int(self.signal_total_pages or 1), 1)
        if page >= total_pages:
            return
        self.signal_page = page + 1
        return self.load_stock_page(
            self.entity_key.removeprefix("stock:"),
            author=self.author_filter or None,
        )

    @rx.event
    def set_signal_page_size(self, value: str):
        self.signal_page_size = _normalize_signal_page_size(value)
        self.signal_page = 1
        return self.load_stock_page(
            self.entity_key.removeprefix("stock:"),
            author=self.author_filter or None,
        )

    @rx.event
    def set_related_filter(self, value: str) -> None:
        self.related_filter = normalize_related_filter(value)
        self.related_limit = DEFAULT_RELATED_LIMIT
        if self.entity_key.startswith("stock:"):
            return self.load_stock_page(
                self.entity_key.removeprefix("stock:"),
                author=self.author_filter or None,
            )

    @rx.event
    def load_more_related(self) -> None:
        current = normalize_related_limit(self.related_limit)
        self.related_limit = min(
            int(MAX_RELATED_LIMIT),
            current + int(RELATED_LIMIT_STEP),
        )
        if self.entity_key.startswith("stock:"):
            return self.load_stock_page(
                self.entity_key.removeprefix("stock:"),
                author=self.author_filter or None,
            )

    @rx.event
    def refresh_stock_related(self) -> None:
        _load_source_read_module().clear_reflex_source_caches()
        _load_stock_hot_read_module().clear_stock_hot_read_caches()
        if self.entity_key.startswith("stock:"):
            return self.load_stock_page(
                self.entity_key.removeprefix("stock:"),
                author=self.author_filter or None,
            )

    @rx.event
    def load_sector_page(self, sector_slug: str | None = None) -> None:
        self.loading = True
        self._reset_feedback_state(close_dialog=True, clear_success=True)
        self._reset_signal_detail_state(close_dialog=True)
        slug = _resolve_route_slug(
            self,
            explicit_slug=sector_slug,
            route_key="sector_slug",
        )
        sector_key = str(slug or "").strip()
        view = load_sector_page_view(sector_key)
        self.page_title = str(view.get("page_title") or "").strip()
        self.entity_key = f"cluster:{sector_key}" if sector_key else ""
        self.entity_type = "sector"
        self.author_filter = ""
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
        self.loaded_once = True
        self.loading = False

    @rx.event
    def load_sector_page_if_needed(self, sector_slug: str | None = None) -> None:
        slug = _resolve_route_slug(
            self,
            explicit_slug=sector_slug,
            route_key="sector_slug",
        )
        sector_key = str(slug or "").strip()
        target_entity_key = f"cluster:{sector_key}" if sector_key else ""
        if (
            self.loaded_once
            and self.entity_type == "sector"
            and target_entity_key == self.entity_key
        ):
            return
        return self.load_sector_page(slug)


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
