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
    fulltext_query: str = ""
    fulltext_error: str = ""

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
        self.stock_error = ""
        self.stock_results = []
        result = _load_home_search_module().search_stocks(self.stock_query)
        self.stock_error = str(result.get("error") or "").strip()
        self.stock_results = result.get("rows") or []
        exact_href = str(result.get("exact_href") or "").strip()
        if self.stock_error or not exact_href:
            return
        return rx.redirect(exact_href)

    @rx.event
    def run_fulltext_search(self):
        self.fulltext_error = ""
        post_search = _load_post_search_module()
        error = post_search.validate_post_search_query(self.fulltext_query)
        if error:
            self.fulltext_error = error
            return
        return rx.redirect(
            post_search.build_post_search_route(self.fulltext_query),
            replace=True,
        )


__all__ = ["HomeSearchState"]
