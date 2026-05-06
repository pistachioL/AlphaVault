from __future__ import annotations

import importlib
from functools import cache
from types import ModuleType

import reflex as rx

from alphavault_reflex.services.thread_tree_lines import build_tree_render_lines


@cache
def _load_mcp_history_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.mcp_history")


@cache
def _load_research_page_loader_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.research_page_loader")


class McpHistoryState(rx.State):
    rows: list[dict[str, str]] = []
    loading: bool = False
    loaded_once: bool = False
    load_error: str = ""
    detail_open: bool = False
    detail_loading: bool = False
    detail_row: dict[str, str] = {}
    detail_posts: list[dict[str, str]] = []
    detail_error: str = ""
    signal_detail_open: bool = False
    signal_detail_loading: bool = False
    signal_detail_post_uid: str = ""
    signal_detail_title: str = ""
    signal_detail_raw_text: str = ""
    signal_detail_tree_text: str = ""
    signal_detail_message: str = ""

    @rx.var
    def has_rows(self) -> bool:
        return bool(self.rows)

    @rx.var
    def show_loading(self) -> bool:
        return bool(self.loading and (not self.loaded_once))

    @rx.var
    def show_empty(self) -> bool:
        return bool(
            self.loaded_once
            and (not self.loading)
            and (str(self.load_error or "").strip() == "")
            and (not self.rows)
        )

    @rx.var
    def detail_has_posts(self) -> bool:
        return bool(self.detail_posts)

    @rx.var
    def detail_has_row(self) -> bool:
        return bool(self.detail_row)

    @rx.var
    def signal_detail_tree_lines(self) -> list[dict[str, str]]:
        return build_tree_render_lines(self.signal_detail_tree_text)

    def _reset_detail_state(self, *, close_dialog: bool) -> None:
        if close_dialog:
            self.detail_open = False
        self.detail_loading = False
        self.detail_row = {}
        self.detail_posts = []
        self.detail_error = ""

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
    def load_history(self):
        self.loading = True
        self.load_error = ""
        yield
        rows, load_error = _load_mcp_history_module().load_mcp_history_rows_from_env()
        self.rows = rows
        self.load_error = str(load_error or "").strip()
        self.loading = False
        self.loaded_once = True

    @rx.event
    def load_history_if_needed(self):
        if self.loaded_once:
            return
        return self.load_history()

    @rx.event
    def set_detail_open(self, value: bool) -> None:
        if value:
            self.detail_open = True
            return
        self.close_detail()

    @rx.event
    def close_detail(self) -> None:
        self._reset_detail_state(close_dialog=True)

    @rx.event
    def open_detail(self, call_id: str):
        resolved_call_id = str(call_id or "").strip()
        self.detail_open = True
        self.detail_loading = True
        self.detail_row = {"call_id": resolved_call_id}
        self.detail_posts = []
        self.detail_error = ""
        if not resolved_call_id:
            self.detail_loading = False
            self.detail_error = "missing_call_id"
            return
        yield
        detail_row, detail_posts, load_error = (
            _load_mcp_history_module().load_mcp_history_detail_from_env(
                resolved_call_id
            )
        )
        self.detail_row = detail_row
        self.detail_posts = detail_posts
        self.detail_error = str(load_error or "").strip()
        self.detail_loading = False

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
        detail = _load_research_page_loader_module().load_stock_signal_detail_view(uid)
        self.signal_detail_post_uid = str(detail.get("post_uid") or uid).strip()
        if not self.signal_detail_title:
            self.signal_detail_title = str(detail.get("title") or "").strip()
        self.signal_detail_raw_text = str(detail.get("raw_text") or "").strip()
        self.signal_detail_tree_text = str(detail.get("tree_text") or "").strip()
        self.signal_detail_message = str(detail.get("message") or "").strip()
        self.signal_detail_loading = False


__all__ = ["McpHistoryState"]
