from __future__ import annotations

import importlib
from functools import cache
import reflex as rx
from types import ModuleType

from alphavault.db.postgres_env import infer_platform_from_post_uid
from alphavault_reflex.services.analysis_feedback import (
    ENTRYPOINT_HOMEWORK_TREE,
    submit_post_analysis_feedback,
)
from alphavault_reflex.services.homework_constants import (
    TRADE_BOARD_DEFAULT_WINDOW_DAYS,
    TRADE_BOARD_MAX_WINDOW_DAYS,
)
from alphavault_reflex.services.homework_board import (
    TRADE_FILTER_OPTIONS,
    build_board,
    build_tree,
)
from alphavault_reflex.services.research_models import (
    CLUSTER_KEY_PREFIX,
    STOCK_KEY_PREFIX,
    build_sector_route,
    build_stock_route,
)
from alphavault.domains.thread_tree.service import normalize_tree_lookup_post_uid
from alphavault.domains.thread_tree.service import slice_posts_for_single_post_tree
from alphavault_reflex.services.thread_tree_lines import TREE_COLLAPSE_HINT_PREFIX
from alphavault_reflex.services.thread_tree_lines import build_tree_render_lines

TREE_MESSAGE_EMPTY = "没有对话流。"
TREE_MESSAGE_LOAD_ERROR_PREFIX = "加载失败："
TREE_PREVIEW_LINE_COUNT = 32
TREE_DEBUG_UNKNOWN = "-"
TREE_DEBUG_STAGE_UID_EMPTY = "uid_empty"
TREE_DEBUG_STAGE_LOAD_ERROR = "load_error"
TREE_DEBUG_STAGE_POSTS_EMPTY = "posts_empty"
TREE_DEBUG_STAGE_SLICE_EMPTY = "slice_empty"
TREE_DEBUG_STAGE_TREE_EMPTY = "tree_empty"


@cache
def _load_source_read_module() -> ModuleType:
    return importlib.import_module("alphavault_reflex.services.source_read")


@cache
def _load_stock_object_index_module() -> ModuleType:
    return importlib.import_module("alphavault.domains.stock.object_index")


class HomeworkState(rx.State):
    loading: bool = False
    loaded_once: bool = False
    load_error: str = ""
    caption: str = ""

    window_max_days: int = TRADE_BOARD_MAX_WINDOW_DAYS
    window_days: int = TRADE_BOARD_DEFAULT_WINDOW_DAYS
    trade_filter: str = TRADE_FILTER_OPTIONS[0]

    rows: list[dict[str, str]] = []

    tree_dialog_open: bool = False
    tree_loading: bool = False
    selected_tree_post_uid: str = ""
    selected_tree_label: str = ""
    selected_tree_text: str = ""
    selected_tree_message: str = ""
    selected_tree_debug_text: str = ""
    tree_wrap_lines: bool = True
    tree_show_full_text: bool = False
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
    def trade_filter_options(self) -> list[str]:
        return TRADE_FILTER_OPTIONS

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

    @rx.var
    def selected_tree_line_count(self) -> int:
        text = str(self.selected_tree_text or "")
        if not text:
            return 0
        return len(text.splitlines()) or 1

    @rx.var
    def tree_text_collapsible(self) -> bool:
        return int(self.selected_tree_line_count) > TREE_PREVIEW_LINE_COUNT

    @rx.var
    def selected_tree_render_text(self) -> str:
        text = str(self.selected_tree_text or "")
        if not text or not self.tree_text_collapsible:
            return text
        if self.tree_show_full_text:
            return text
        lines = text.splitlines()
        hidden_lines = max(0, len(lines) - TREE_PREVIEW_LINE_COUNT)
        preview = "\n".join(lines[:TREE_PREVIEW_LINE_COUNT]).rstrip()
        if hidden_lines <= 0:
            return text
        return (
            f"{preview}\n\n{TREE_COLLAPSE_HINT_PREFIX}{hidden_lines} "
            "行，点「展开全文」继续查看。"
        )

    @rx.var
    def selected_tree_render_lines(self) -> list[dict[str, str]]:
        return build_tree_render_lines(self.selected_tree_render_text)

    def _refresh(self) -> None:
        assertions, stock_relations, err = (
            _load_source_read_module().load_homework_board_payload_from_env(
                int(self.window_days)
            )
        )
        if err:
            self.load_error = err
            self.caption = ""
            self.rows = []
            _clear_selected_tree(self)
            return

        board_assertions, board_topic_labels = _prepare_board_assertions(
            assertions,
            stock_relations=stock_relations,
        )

        result = build_board(
            board_assertions,
            [],
            group_col="board_group_key",
            group_label="主题",
            window_days=int(self.window_days),
            trade_filter=str(self.trade_filter),
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

        _apply_homework_rows(
            self,
            caption=result.caption,
            used_window_days=result.used_window_days,
            rows=result.rows,
        )

    def _refresh_with_loading(self):
        self.loading = True
        self.load_error = ""
        self.tree_dialog_open = False
        self.tree_loading = False
        self.selected_tree_debug_text = ""
        _load_source_read_module().clear_reflex_source_caches()
        yield
        self._refresh()
        self.loaded_once = True
        self.loading = False

    @rx.event
    def load_data(self):
        yield from self._refresh_with_loading()

    @rx.event
    def load_data_if_needed(self):
        if self.loaded_once:
            return
        yield from self._refresh_with_loading()

    @rx.event
    def set_window_days(self, value: list[int | float]):
        if value:
            self.window_days = int(value[0])
        yield from self._refresh_with_loading()

    @rx.event
    def set_trade_filter(self, value: str):
        self.trade_filter = str(value or TRADE_FILTER_OPTIONS[0])
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
        self.tree_wrap_lines = True
        self.tree_show_full_text = False
        self._reset_feedback_state(close_dialog=True, clear_success=True)

    @rx.event
    def toggle_tree_wrap_lines(self) -> None:
        self.tree_wrap_lines = not self.tree_wrap_lines

    @rx.event
    def expand_tree_text(self) -> None:
        self.tree_show_full_text = True

    @rx.event
    def collapse_tree_text(self) -> None:
        self.tree_show_full_text = False

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
        self.feedback_post_uid = str(
            post_uid or self.selected_tree_post_uid or ""
        ).strip()

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
    def submit_feedback(self) -> None:
        self.feedback_submitting = True
        self.feedback_error = ""
        self.feedback_success = ""
        result = submit_post_analysis_feedback(
            post_uid=str(
                self.feedback_post_uid or self.selected_tree_post_uid or ""
            ).strip(),
            feedback_tag=str(self.feedback_tag or "").strip(),
            feedback_note=str(self.feedback_note or ""),
            entrypoint=ENTRYPOINT_HOMEWORK_TREE,
        )
        self.feedback_submitting = False
        if str(result.get("ok") or "") != "1":
            self.feedback_error = str(result.get("message") or "").strip()
            return

        success_message = str(result.get("message") or "").strip()
        self._reset_feedback_state(close_dialog=True, clear_success=False)
        self.feedback_success = success_message
        _load_source_read_module().clear_reflex_source_caches()
        self._refresh()

    @rx.event
    def open_tree_dialog(self, post_uid: str):
        uid = normalize_tree_lookup_post_uid(post_uid)
        debug_platform = infer_platform_from_post_uid(uid) or TREE_DEBUG_UNKNOWN
        debug_posts_count: int | None = None
        debug_slice_count: int | None = None
        self._reset_feedback_state(close_dialog=True, clear_success=True)
        self.tree_dialog_open = True
        self.tree_loading = True
        self.selected_tree_post_uid = uid
        self.selected_tree_label = ""
        self.selected_tree_text = ""
        self.selected_tree_message = ""
        self.selected_tree_debug_text = ""
        self.tree_wrap_lines = True
        self.tree_show_full_text = False
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
        posts, err = _load_source_read_module().load_single_post_for_tree_from_env(uid)
        debug_posts_count = int(len(posts))
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

        if not posts:
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
        debug_slice_count = int(len(posts_view))
        if not posts_view:
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


def _clear_selected_tree(state: HomeworkState) -> None:
    state.selected_tree_post_uid = ""
    state.selected_tree_label = ""
    state.selected_tree_text = ""
    state.selected_tree_message = ""
    state.selected_tree_debug_text = ""


def _coerce_positive_window_days(value: object) -> int:
    text = str(value or "").strip()
    if not text:
        return 1
    try:
        return max(1, int(text))
    except (TypeError, ValueError):
        return 1


def _apply_homework_rows(
    state: HomeworkState,
    *,
    caption: object,
    used_window_days: object,
    rows: object,
) -> None:
    state.caption = str(caption or "").strip()
    state.window_max_days = TRADE_BOARD_MAX_WINDOW_DAYS
    state.window_days = _coerce_positive_window_days(used_window_days)
    state.rows = _coerce_homework_rows(rows)
    if not state.rows:
        _clear_selected_tree(state)


def _coerce_homework_rows(rows: object) -> list[dict[str, str]]:
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _prepare_board_assertions(
    assertions: list[dict[str, object]],
    *,
    stock_relations: list[dict[str, object]],
) -> tuple[list[dict[str, object]], dict[str, str]]:
    if not assertions:
        return [], {}
    board_assertions = [dict(row) for row in assertions]
    stock_index = _load_stock_object_index_module().build_stock_object_index(
        board_assertions,
        stock_relations=stock_relations,
    )
    board_topic_labels: dict[str, str] = {}
    for row in board_assertions:
        entity_key = str(row.get("entity_key") or "").strip()
        if entity_key.startswith("stock:"):
            group_key = stock_index.resolve(entity_key)
            row["board_group_key"] = group_key
            if group_key:
                board_topic_labels[group_key] = stock_index.page_title(group_key)
            continue
        row["board_group_key"] = entity_key
        if entity_key:
            board_topic_labels[entity_key] = _topic_label(entity_key)
    return board_assertions, board_topic_labels


def _fill_trade_board_urls(rows: list[dict[str, str]]) -> None:
    if not rows:
        return

    needs_urls = any(str(row.get("url") or "").strip() == "" for row in rows)
    if not needs_urls:
        return

    post_uids = [row.get("tree_post_uid", "") for row in rows]
    url_map, _ = _load_source_read_module().load_post_urls_from_env(post_uids)
    if not url_map:
        return

    for row in rows:
        if str(row.get("url") or "").strip():
            continue
        uid = str(row.get("tree_post_uid") or "").strip()
        if uid and uid in url_map:
            row["url"] = url_map[uid]
