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
    CLUSTER_KEY_PREFIX,
    STOCK_KEY_PREFIX,
    build_sector_route,
    build_stock_route,
)
from alphavault_reflex.services.stock_objects import (
    build_stock_object_index,
)
from alphavault_reflex.services.turso_read import (
    load_post_urls_from_env,
    load_posts_for_tree_from_env,
    load_stock_alias_relations_from_env,
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

        stock_relations, relation_err = load_stock_alias_relations_from_env()
        if relation_err:
            stock_relations = pd.DataFrame()
        board_assertions, board_topic_labels = _prepare_board_assertions(
            assertions,
            stock_relations=stock_relations,
        )

        result = build_board(
            board_assertions,
            pd.DataFrame(),
            group_col="board_group_key",
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
            stock_slug = (
                topic_key.removeprefix(STOCK_KEY_PREFIX)
                if topic_key.startswith(STOCK_KEY_PREFIX)
                else ""
            )
            sector_slug = (
                topic_key.removeprefix(CLUSTER_KEY_PREFIX)
                if topic_key.startswith(CLUSTER_KEY_PREFIX)
                else ""
            )
            row["topic_label"] = str(
                board_topic_labels.get(topic_key) or _topic_label(topic_key)
            ).strip()
            row["stock_slug"] = stock_slug
            row["sector_slug"] = sector_slug
            row["stock_route"] = build_stock_route(topic_key) if stock_slug else ""
            row["sector_route"] = build_sector_route(topic_key) if sector_slug else ""

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


def _prepare_board_assertions(
    assertions: pd.DataFrame,
    *,
    stock_relations: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, str]]:
    if assertions.empty:
        return assertions.copy(), {}
    board_assertions = assertions.copy()
    stock_index = build_stock_object_index(
        board_assertions,
        stock_relations=stock_relations,
    )
    board_group_keys: list[str] = []
    board_topic_labels: dict[str, str] = {}
    for topic_key in board_assertions.get("topic_key", pd.Series(dtype=str)).tolist():
        topic = str(topic_key or "").strip()
        if topic.startswith("stock:"):
            group_key = stock_index.resolve(topic)
            board_group_keys.append(group_key)
            if group_key:
                board_topic_labels[group_key] = stock_index.header_title(group_key)
            continue
        board_group_keys.append(topic)
        if topic:
            board_topic_labels[topic] = _topic_label(topic)
    board_assertions["board_group_key"] = board_group_keys
    return board_assertions, board_topic_labels
