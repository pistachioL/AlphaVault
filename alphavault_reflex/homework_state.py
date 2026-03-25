from __future__ import annotations

import pandas as pd
import reflex as rx

from alphavault_reflex.services.homework_board import (
    CONSENSUS_FILTER_OPTIONS,
    SORT_MODE_OPTIONS,
    build_board,
    build_tree,
)
from alphavault_reflex.services.research_models import (
    build_sector_route,
    build_stock_route,
)
from alphavault_reflex.services.turso_read import (
    load_post_urls_from_env,
    load_posts_for_tree_from_env,
    load_trade_assertions_from_env,
)


class HomeworkState(rx.State):
    loading: bool = False
    loaded_once: bool = False
    load_error: str = ""
    caption: str = ""

    window_max_days: int = 60
    window_days: int = 7
    sort_mode: str = SORT_MODE_OPTIONS[0]
    consensus_filter: str = CONSENSUS_FILTER_OPTIONS[0]

    rows: list[dict[str, str]] = []

    tree_dialog_open: bool = False
    tree_loading: bool = False
    selected_tree_label: str = ""
    selected_tree_text: str = ""
    selected_tree_message: str = ""

    @rx.var
    def sort_mode_options(self) -> list[str]:
        return SORT_MODE_OPTIONS

    @rx.var
    def consensus_filter_options(self) -> list[str]:
        return CONSENSUS_FILTER_OPTIONS

    @rx.var
    def show_table_loading(self) -> bool:
        return bool(self.loading or not self.loaded_once)

    @rx.var
    def show_table_empty(self) -> bool:
        return bool(
            self.loaded_once
            and (not self.loading)
            and (str(self.load_error or "").strip() == "")
            and (not self.rows)
        )

    def _refresh(self) -> None:
        assertions, err = load_trade_assertions_from_env()
        if err:
            self.load_error = err
            self.caption = ""
            self.rows = []
            self.selected_tree_label = ""
            self.selected_tree_text = ""
            self.selected_tree_message = ""
            return

        result = build_board(
            assertions,
            pd.DataFrame(),
            group_col="topic_key",
            group_label="主题",
            window_days=int(self.window_days),
            sort_mode=str(self.sort_mode),
            consensus_filter=str(self.consensus_filter),
        )
        url_map: dict[str, str] = {}
        if result.rows:
            post_uids = [row.get("tree_post_uid", "") for row in result.rows]
            url_map, _ = load_post_urls_from_env(post_uids)
        if url_map:
            for row in result.rows:
                if row.get("url"):
                    continue
                uid = str(row.get("tree_post_uid") or "").strip()
                if uid and uid in url_map:
                    row["url"] = url_map[uid]
        for row in result.rows:
            topic_key = str(row.get("topic") or "").strip()
            row["topic_label"] = _topic_label(topic_key)
            row["stock_route"] = (
                build_stock_route(topic_key) if topic_key.startswith("stock:") else ""
            )
            row["sector_route"] = (
                build_sector_route(topic_key)
                if topic_key.startswith("cluster:")
                else ""
            )

        self.caption = result.caption
        self.window_max_days = int(result.window_max_days or 1)
        self.window_days = int(result.used_window_days or 1)
        self.rows = result.rows
        if not self.rows:
            self.selected_tree_label = ""
            self.selected_tree_text = ""
            self.selected_tree_message = ""

    def _refresh_with_loading(self):
        self.loading = True
        self.load_error = ""
        self.tree_dialog_open = False
        self.tree_loading = False
        yield
        self._refresh()
        self.loaded_once = True
        self.loading = False

    @rx.event
    def load_data(self):
        yield from self._refresh_with_loading()

    @rx.event
    def set_window_days(self, value: list[int | float]):
        if value:
            self.window_days = int(value[0])
        yield from self._refresh_with_loading()

    @rx.event
    def set_sort_mode(self, value: str):
        self.sort_mode = str(value or SORT_MODE_OPTIONS[0])
        yield from self._refresh_with_loading()

    @rx.event
    def set_consensus_filter(self, value: str):
        self.consensus_filter = str(value or CONSENSUS_FILTER_OPTIONS[0])
        yield from self._refresh_with_loading()

    @rx.event
    def set_tree_dialog_open(self, value: bool) -> None:
        if value:
            self.tree_dialog_open = True
            return
        self.close_tree_dialog()

    @rx.event
    def close_tree_dialog(self) -> None:
        self.tree_dialog_open = False
        self.tree_loading = False

    @rx.event
    def open_tree_dialog(self, post_uid: str):
        uid = str(post_uid or "").strip()
        self.tree_dialog_open = True
        self.tree_loading = True
        self.selected_tree_label = ""
        self.selected_tree_text = ""
        self.selected_tree_message = ""
        if not uid:
            self.tree_loading = False
            self.selected_tree_message = "没有对话流。"
            return

        yield
        posts, err = load_posts_for_tree_from_env()
        if err:
            self.tree_loading = False
            self.selected_tree_message = f"加载失败：{err}"
            return

        label, tree_text = build_tree(post_uid=uid, posts=posts)
        self.selected_tree_label = label
        self.selected_tree_text = tree_text
        self.selected_tree_message = (
            "" if str(tree_text or "").strip() else "没有对话流。"
        )
        self.tree_loading = False


def _topic_label(topic_key: str) -> str:
    value = str(topic_key or "").strip()
    if ":" not in value:
        return value
    return value.split(":", 1)[1].strip()
