from __future__ import annotations

import importlib
from functools import cache
from types import ModuleType

import reflex as rx

from alphavault_reflex.services.research_page_loader import (
    load_stock_signal_detail_view,
)
from alphavault_reflex.services.research_state_utils import coerce_rows as _coerce_rows
from alphavault_reflex.services.research_state_utils import (
    resolve_query_value as _resolve_query_value,
)
from alphavault_reflex.services.thread_tree_lines import build_tree_render_lines


@cache
def _load_post_search_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.post_search")


class PostSearchState(rx.State):
    search_query: str = ""
    loaded_query: str = ""
    results: list[dict[str, str]] = []
    loading: bool = False
    loading_more: bool = False
    loaded_once: bool = False
    load_error: str = ""
    next_cursor: str = ""
    has_more: bool = False
    signal_detail_open: bool = False
    signal_detail_loading: bool = False
    signal_detail_post_uid: str = ""
    signal_detail_title: str = ""
    signal_detail_raw_text: str = ""
    signal_detail_tree_text: str = ""
    signal_detail_message: str = ""

    @rx.var
    def has_results(self) -> bool:
        return bool(self.results)

    @rx.var
    def show_empty(self) -> bool:
        return bool(
            self.loaded_once
            and (not self.loading)
            and (str(self.load_error or "").strip() == "")
            and (not self.results)
        )

    @rx.var
    def show_load_more(self) -> bool:
        return bool(
            self.loaded_once
            and (not self.loading)
            and (not self.loading_more)
            and self.has_more
            and str(self.next_cursor or "").strip() != ""
        )

    @rx.var
    def show_loading_more(self) -> bool:
        return bool(self.loaded_once and (not self.loading) and self.loading_more)

    @rx.var
    def show_reach_end(self) -> bool:
        return bool(
            self.loaded_once
            and (not self.loading)
            and (not self.loading_more)
            and self.results
            and (not self.has_more)
            and str(self.load_error or "").strip() == ""
        )

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
    def set_search_query(self, value: str) -> None:
        self.search_query = str(value or "")
        if str(self.load_error or "").strip():
            self.load_error = ""

    @rx.event
    def load_search(self, query: str | None = None):
        resolved_query = _resolve_query_value(
            self,
            explicit_value=query,
            query_key="q",
        )
        self.search_query = resolved_query
        self.loaded_query = resolved_query
        self.results = []
        self.load_error = ""
        self.loading = False
        self.loading_more = False
        self.next_cursor = ""
        self.has_more = False
        self._reset_signal_detail_state(close_dialog=True)
        if not resolved_query:
            self.loaded_once = False
            return

        post_search = _load_post_search_module()
        query_error = post_search.validate_post_search_query(resolved_query)
        if query_error:
            self.loaded_once = True
            self.load_error = query_error
            return

        self.loading = True
        yield
        result = post_search.search_posts_from_env(resolved_query)
        self.results = _coerce_rows(result.get("rows"))
        self.load_error = str(result.get("error") or "").strip()
        self.next_cursor = str(result.get("next_cursor") or "").strip()
        self.has_more = bool(result.get("has_more"))
        self.loaded_once = True
        self.loading = False
        self.loading_more = False

    @rx.event
    def load_search_if_needed(self):
        resolved_query = _resolve_query_value(
            self,
            explicit_value=None,
            query_key="q",
        )
        if not resolved_query:
            self.search_query = ""
            self.loaded_query = ""
            self.results = []
            self.load_error = ""
            self.loading = False
            self.loading_more = False
            self.loaded_once = False
            self.next_cursor = ""
            self.has_more = False
            return
        if self.loaded_once and resolved_query == str(self.loaded_query or "").strip():
            self.search_query = resolved_query
            return
        return self.load_search(resolved_query)

    @rx.event
    def submit_search(self):
        post_search = _load_post_search_module()
        query_error = post_search.validate_post_search_query(self.search_query)
        if query_error:
            self.results = []
            self.loaded_query = str(self.search_query or "").strip()
            self.loaded_once = True
            self.load_error = query_error
            return
        current_query = _resolve_query_value(
            self,
            explicit_value=None,
            query_key="q",
        )
        if str(current_query or "").strip() != str(self.search_query or "").strip():
            return rx.redirect(
                post_search.build_post_search_route(self.search_query),
                replace=True,
            )
        return self.load_search(self.search_query)

    @rx.event
    def load_more(self):
        if (
            self.loading
            or self.loading_more
            or (not self.has_more)
            or str(self.next_cursor or "").strip() == ""
            or str(self.loaded_query or "").strip() == ""
        ):
            return
        self.loading_more = True
        yield
        result = _load_post_search_module().search_posts_from_env(
            self.loaded_query,
            cursor=self.next_cursor,
        )
        error = str(result.get("error") or "").strip()
        if error:
            self.load_error = error
            self.loading_more = False
            return
        self.results = [*self.results, *_coerce_rows(result.get("rows"))]
        self.next_cursor = str(result.get("next_cursor") or "").strip()
        self.has_more = bool(result.get("has_more"))
        self.load_error = ""
        self.loading_more = False

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


__all__ = ["PostSearchState"]
