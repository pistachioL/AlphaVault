from __future__ import annotations

import pandas as pd
import reflex as rx

from alphavault.db.turso_env import infer_platform_from_post_uid
from alphavault_reflex.services.homework_constants import (
    TRADE_BOARD_DEFAULT_WINDOW_DAYS,
    TRADE_BOARD_MAX_WINDOW_DAYS,
)
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
from alphavault_reflex.services.thread_tree import normalize_tree_lookup_post_uid
from alphavault_reflex.services.thread_tree import slice_posts_for_single_post_tree
from alphavault_reflex.services.stock_objects import (
    build_stock_object_index,
)
from alphavault_reflex.services.turso_read import (
    clear_reflex_source_caches,
    load_homework_board_payload_from_env,
    load_post_urls_from_env,
    load_single_post_for_tree_from_env,
)

TREE_MESSAGE_EMPTY = "没有对话流。"
TREE_MESSAGE_LOAD_ERROR_PREFIX = "加载失败："
TREE_DEBUG_UNKNOWN = "-"
TREE_DEBUG_STAGE_UID_EMPTY = "uid_empty"
TREE_DEBUG_STAGE_LOAD_ERROR = "load_error"
TREE_DEBUG_STAGE_POSTS_EMPTY = "posts_empty"
TREE_DEBUG_STAGE_SLICE_EMPTY = "slice_empty"
TREE_DEBUG_STAGE_TREE_EMPTY = "tree_empty"


class HomeworkState(rx.State):
    loading: bool = False
    loaded_once: bool = False
    load_error: str = ""
    caption: str = ""

    window_max_days: int = TRADE_BOARD_MAX_WINDOW_DAYS
    window_days: int = TRADE_BOARD_DEFAULT_WINDOW_DAYS
    sort_mode: str = SORT_MODE_OPTIONS[0]
    consensus_filter: str = CONSENSUS_FILTER_OPTIONS[0]

    rows: list[dict[str, str]] = []

    tree_dialog_open: bool = False
    tree_loading: bool = False
    selected_tree_label: str = ""
    selected_tree_text: str = ""
    selected_tree_message: str = ""
    selected_tree_debug_text: str = ""

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
        assertions, stock_relations, err = load_homework_board_payload_from_env(
            int(self.window_days)
        )
        if err:
            self.load_error = err
            self.caption = ""
            self.rows = []
            self.selected_tree_label = ""
            self.selected_tree_text = ""
            self.selected_tree_message = ""
            self.selected_tree_debug_text = ""
            return

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
        _fill_trade_board_urls(result.rows)
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
        self.window_max_days = TRADE_BOARD_MAX_WINDOW_DAYS
        self.window_days = int(result.used_window_days or 1)
        self.rows = result.rows
        if not self.rows:
            self.selected_tree_label = ""
            self.selected_tree_text = ""
            self.selected_tree_message = ""
            self.selected_tree_debug_text = ""

    def _refresh_with_loading(self):
        self.loading = True
        self.load_error = ""
        self.tree_dialog_open = False
        self.tree_loading = False
        self.selected_tree_debug_text = ""
        clear_reflex_source_caches()
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
        self.selected_tree_debug_text = ""

    @rx.event
    def open_tree_dialog(self, post_uid: str):
        uid = normalize_tree_lookup_post_uid(post_uid)
        debug_platform = infer_platform_from_post_uid(uid) or TREE_DEBUG_UNKNOWN
        debug_posts_count: int | None = None
        debug_slice_count: int | None = None
        self.tree_dialog_open = True
        self.tree_loading = True
        self.selected_tree_label = ""
        self.selected_tree_text = ""
        self.selected_tree_message = ""
        self.selected_tree_debug_text = ""
        if not uid:
            self.tree_loading = False
            self.selected_tree_message = TREE_MESSAGE_EMPTY
            self.selected_tree_debug_text = _build_tree_debug_text(
                requested_uid=uid,
                platform=debug_platform,
                stage_code=TREE_DEBUG_STAGE_UID_EMPTY,
                posts_count=debug_posts_count,
                slice_count=debug_slice_count,
                error_text="",
            )
            return

        yield
        posts, err = load_single_post_for_tree_from_env(uid)
        debug_posts_count = int(len(posts.index))
        if err:
            self.tree_loading = False
            self.selected_tree_message = f"{TREE_MESSAGE_LOAD_ERROR_PREFIX}{err}"
            self.selected_tree_debug_text = _build_tree_debug_text(
                requested_uid=uid,
                platform=debug_platform,
                stage_code=TREE_DEBUG_STAGE_LOAD_ERROR,
                posts_count=debug_posts_count,
                slice_count=debug_slice_count,
                error_text=err,
            )
            return

        if posts.empty:
            self.tree_loading = False
            self.selected_tree_message = TREE_MESSAGE_EMPTY
            self.selected_tree_debug_text = _build_tree_debug_text(
                requested_uid=uid,
                platform=debug_platform,
                stage_code=TREE_DEBUG_STAGE_POSTS_EMPTY,
                posts_count=debug_posts_count,
                slice_count=debug_slice_count,
                error_text="",
            )
            return

        posts_view = slice_posts_for_single_post_tree(post_uid=uid, posts=posts)
        debug_slice_count = int(len(posts_view.index))
        if posts_view.empty:
            self.tree_loading = False
            self.selected_tree_message = TREE_MESSAGE_EMPTY
            self.selected_tree_debug_text = _build_tree_debug_text(
                requested_uid=uid,
                platform=debug_platform,
                stage_code=TREE_DEBUG_STAGE_SLICE_EMPTY,
                posts_count=debug_posts_count,
                slice_count=debug_slice_count,
                error_text="",
            )
            return

        label, tree_text = build_tree(post_uid=uid, posts=posts_view)
        self.selected_tree_label = label
        self.selected_tree_text = tree_text
        if str(tree_text or "").strip():
            self.selected_tree_message = ""
            self.selected_tree_debug_text = ""
        else:
            self.selected_tree_message = TREE_MESSAGE_EMPTY
            self.selected_tree_debug_text = _build_tree_debug_text(
                requested_uid=uid,
                platform=debug_platform,
                stage_code=TREE_DEBUG_STAGE_TREE_EMPTY,
                posts_count=debug_posts_count,
                slice_count=debug_slice_count,
                error_text="",
            )
        self.tree_loading = False


def _topic_label(topic_key: str) -> str:
    value = str(topic_key or "").strip()
    if ":" not in value:
        return value
    return value.split(":", 1)[1].strip()


def _build_tree_debug_text(
    *,
    requested_uid: str,
    platform: str,
    stage_code: str,
    posts_count: int | None,
    slice_count: int | None,
    error_text: str,
) -> str:
    post_rows = TREE_DEBUG_UNKNOWN if posts_count is None else str(int(posts_count))
    slice_rows = TREE_DEBUG_UNKNOWN if slice_count is None else str(int(slice_count))
    error_line = str(error_text or "").strip() or TREE_DEBUG_UNKNOWN
    lines = [
        f"请求UID: {str(requested_uid or TREE_DEBUG_UNKNOWN)}",
        f"平台: {str(platform or TREE_DEBUG_UNKNOWN)}",
        f"阶段码: {str(stage_code or TREE_DEBUG_UNKNOWN)}",
        f"DB行数: {post_rows}",
        f"切片行数: {slice_rows}",
        f"错误: {error_line}",
    ]
    return "\n".join(lines)


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


def _fill_trade_board_urls(rows: list[dict[str, str]]) -> None:
    if not rows:
        return

    needs_urls = any(str(row.get("url") or "").strip() == "" for row in rows)
    if not needs_urls:
        return

    post_uids = [row.get("tree_post_uid", "") for row in rows]
    url_map, _ = load_post_urls_from_env(post_uids)
    if not url_map:
        return

    for row in rows:
        if str(row.get("url") or "").strip():
            continue
        uid = str(row.get("tree_post_uid") or "").strip()
        if uid and uid in url_map:
            row["url"] = url_map[uid]
