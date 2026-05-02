from __future__ import annotations

import importlib
from functools import cache
from types import ModuleType

import reflex as rx


@cache
def _load_home_search_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.home_search")


@cache
def _load_post_search_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.post_search")


class HomeSearchState(rx.State):
    stock_query: str = ""
    stock_results: list[dict[str, str]] = []
    stock_error: str = ""
    stock_loading: bool = False
    fulltext_query: str = ""
    fulltext_error: str = ""
    fulltext_loading: bool = False

    @rx.var
    def has_stock_results(self) -> bool:
        return bool(self.stock_results)

    @rx.event
    def set_stock_query(self, value: str) -> None:
        self.stock_query = str(value or "")
        self.stock_error = ""

    @rx.event
    def set_fulltext_query(self, value: str) -> None:
        self.fulltext_query = str(value or "")
        self.fulltext_error = ""

    @rx.event
    def run_stock_search(self):
        if self.stock_loading:
            return
        self.stock_loading = True
        self.stock_error = ""
        self.stock_results = []
        yield
        exact_href = ""
        try:
            result = _load_home_search_module().search_stocks(self.stock_query)
            self.stock_error = str(result.get("error") or "").strip()
            self.stock_results = result.get("rows") or []
            exact_href = str(result.get("exact_href") or "").strip()
        finally:
            self.stock_loading = False
        if self.stock_error or not exact_href:
            return
        yield rx.redirect(exact_href)

    @rx.event
    def run_fulltext_search(self):
        if self.fulltext_loading:
            return
        self.fulltext_loading = True
        self.fulltext_error = ""
        yield
        target_route = ""
        try:
            post_search = _load_post_search_module()
            error = post_search.validate_post_search_query(self.fulltext_query)
            if error:
                self.fulltext_error = error
                return
            target_route = post_search.build_post_search_route(self.fulltext_query)
        finally:
            self.fulltext_loading = False
        if not target_route:
            return
        yield rx.redirect(
            target_route,
            replace=True,
        )


__all__ = ["HomeSearchState"]
