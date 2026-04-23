from __future__ import annotations

import reflex as rx

from alphavault_reflex.homework_state import HomeworkState
from alphavault_reflex.pages.thread_tree_components import tree_line_row
from alphavault_reflex.services.analysis_feedback import (
    ANALYSIS_FEEDBACK_CANCEL_TEXT,
    ANALYSIS_FEEDBACK_NOTE_LABEL,
    ANALYSIS_FEEDBACK_NOTE_PLACEHOLDER,
    ANALYSIS_FEEDBACK_SUBMIT_TEXT,
    ANALYSIS_FEEDBACK_TAG_LABEL,
    ANALYSIS_FEEDBACK_TAG_OPTIONS,
    ANALYSIS_FEEDBACK_TAG_PLACEHOLDER,
)
from alphavault_reflex.services.homework_time_range import (
    HOMEWORK_TIME_SHORTCUT_RECENT_3_DAYS,
    HOMEWORK_TIME_SHORTCUT_RECENT_7_DAYS,
    HOMEWORK_TIME_SHORTCUT_RECENT_HALF_MONTH,
    HOMEWORK_TIME_SHORTCUT_RECENT_ONE_MONTH,
    HOMEWORK_TIME_SHORTCUT_RECENT_THREE_MONTHS,
    HOMEWORK_TIME_SHORTCUT_TODAY,
)


def index_page() -> rx.Component:
    return rx.center(
        rx.vstack(
            rx.heading("AlphaVault", size="6"),
            rx.text("打开作业板："),
            rx.link("作业板（列表）", href="/homework", underline="always"),
            spacing="3",
            align="center",
        ),
        min_height="100vh",
        padding="24px",
    )


def _controls() -> rx.Component:
    return rx.vstack(
        rx.hstack(
            rx.vstack(
                rx.text("开始时间（北京时间）", class_name="av-label"),
                rx.input(
                    value=HomeworkState.window_start_local,
                    on_change=HomeworkState.set_window_start_local,
                    type="datetime-local",
                    step=60,
                    max=HomeworkState.window_end_local,
                    width="220px",
                    disabled=HomeworkState.show_table_loading,
                ),
                spacing="2",
            ),
            rx.vstack(
                rx.text("结束时间（北京时间）", class_name="av-label"),
                rx.input(
                    value=HomeworkState.window_end_local,
                    on_change=HomeworkState.set_window_end_local,
                    type="datetime-local",
                    step=60,
                    min=HomeworkState.window_start_local,
                    width="220px",
                    disabled=HomeworkState.show_table_loading,
                ),
                spacing="2",
            ),
            rx.vstack(
                rx.text("trade 筛选", class_name="av-label"),
                rx.select(
                    HomeworkState.trade_filter_options,
                    value=HomeworkState.trade_filter,
                    on_change=HomeworkState.set_trade_filter,
                    width="220px",
                    disabled=HomeworkState.show_table_loading,
                ),
                spacing="2",
            ),
            rx.vstack(
                rx.text("手动改完点刷新", class_name="av-label"),
                rx.button(
                    "刷新",
                    on_click=HomeworkState.load_data,
                    class_name="av-btn",
                    loading=HomeworkState.show_table_loading,
                    disabled=HomeworkState.show_table_loading,
                ),
                spacing="2",
            ),
            spacing="4",
            align="end",
            width="100%",
            class_name="av-controls-row",
        ),
        rx.vstack(
            rx.text("快捷选择", class_name="av-label"),
            rx.hstack(
                _shortcut_button(HOMEWORK_TIME_SHORTCUT_TODAY),
                _shortcut_button(HOMEWORK_TIME_SHORTCUT_RECENT_3_DAYS),
                _shortcut_button(HOMEWORK_TIME_SHORTCUT_RECENT_7_DAYS),
                _shortcut_button(HOMEWORK_TIME_SHORTCUT_RECENT_HALF_MONTH),
                _shortcut_button(HOMEWORK_TIME_SHORTCUT_RECENT_ONE_MONTH),
                _shortcut_button(HOMEWORK_TIME_SHORTCUT_RECENT_THREE_MONTHS),
                spacing="2",
                class_name="av-shortcuts",
            ),
            spacing="2",
            width="100%",
        ),
        spacing="3",
        width="100%",
        class_name="av-controls",
    )


def _shortcut_button(label: str) -> rx.Component:
    return rx.button(
        label,
        on_click=lambda: HomeworkState.apply_time_shortcut(label),
        variant="soft",
        color_scheme="gray",
        class_name="av-btn-small",
        disabled=HomeworkState.show_table_loading,
    )


def _table_header_cell(label: str, *, col_index: int) -> rx.Component:
    return rx.el.th(
        rx.el.div(
            rx.el.span(label),
            rx.el.div(
                class_name="av-col-resizer",
                custom_attrs={"data-col-index": str(int(col_index))},
            ),
            class_name="av-th-inner",
        ),
        class_name="av-th",
        custom_attrs={"data-col-index": str(int(col_index))},
    )


def _topic_cell(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.td(
        rx.cond(
            row["stock_route"] != "",
            rx.link(
                row["topic_label"],
                href=row["stock_route"],
                class_name="av-topic-link",
            ),
            rx.cond(
                row["sector_route"] != "",
                rx.link(
                    row["topic_label"],
                    href=row["sector_route"],
                    class_name="av-topic-link",
                ),
                rx.el.span(row["topic_label"]),
            ),
        ),
        class_name="av-td av-topic",
    )


def _row_tr(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.tr(
        _topic_cell(row),
        rx.el.td(row["recent_action"], class_name="av-td av-recent-action"),
        rx.el.td(row["recent_author"], class_name="av-td av-recent-author"),
        rx.el.td(row["summary"], class_name="av-td av-summary"),
        rx.el.td(
            rx.cond(
                row["tree_post_uid"] != "",
                rx.button(
                    "对话流",
                    on_click=lambda: HomeworkState.open_tree_dialog(
                        row["tree_post_uid"]
                    ),
                    class_name="av-btn av-btn-small",
                ),
                rx.el.span("无", class_name="av-muted"),
            ),
            class_name="av-td av-tree",
        ),
        rx.el.td(
            rx.cond(
                row["url"] != "",
                rx.link("打开", href=row["url"], is_external=True),
                rx.el.span("—", class_name="av-muted"),
            ),
            class_name="av-td av-link",
        ),
        rx.el.td(row["recent_age"], class_name="av-td av-recent-age"),
        class_name="av-tr",
    )


def _board_table() -> rx.Component:
    labels = [
        "主题",
        "动作",
        "大佬",
        "总结",
        "对话流",
        "链接",
        "最近",
    ]
    col_widths = [
        "240px",
        "220px",
        "180px",
        "520px",
        "84px",
        "84px",
        "90px",
    ]
    cols = [rx.el.col(style={"width": w}) for w in col_widths]
    header_cells = [
        _table_header_cell(label, col_index=i) for i, label in enumerate(labels)
    ]

    return rx.el.div(
        rx.el.div(
            rx.el.table(
                rx.el.colgroup(*cols),
                rx.el.thead(rx.el.tr(*header_cells), class_name="av-thead"),
                rx.el.tbody(
                    rx.foreach(HomeworkState.rows, _row_tr),
                    class_name="av-tbody",
                ),
                id="av-homework-table",
                class_name="av-table",
            ),
            class_name="av-table-wrap",
        ),
        rx.cond(
            HomeworkState.show_table_loading,
            rx.el.div(
                rx.spinner(size="3"),
                rx.text("加载中…", class_name="av-muted"),
                class_name="av-table-overlay",
            ),
            rx.el.div(),
        ),
        rx.cond(
            HomeworkState.show_table_empty,
            rx.el.div(
                rx.el.div(
                    rx.text("没有数据", class_name="av-empty-title"),
                    rx.text(
                        "试试：改一下时间范围，或者点「刷新」。",
                        class_name="av-muted",
                    ),
                    class_name="av-empty",
                ),
                class_name="av-table-empty-overlay",
            ),
            rx.el.div(),
        ),
        class_name="av-table-shell",
    )


def _tree_dialog() -> rx.Component:
    return rx.dialog.root(
        rx.dialog.content(
            rx.hstack(
                rx.dialog.title("对话流"),
                rx.spacer(),
                rx.cond(
                    HomeworkState.selected_tree_post_uid != "",
                    rx.button(
                        "标错并重跑",
                        on_click=lambda: HomeworkState.open_feedback_dialog(
                            HomeworkState.selected_tree_post_uid
                        ),
                        variant="soft",
                        size="1",
                    ),
                    rx.el.div(),
                ),
                width="100%",
                align="center",
            ),
            rx.cond(
                HomeworkState.selected_tree_label != "",
                rx.text(HomeworkState.selected_tree_label, class_name="av-muted"),
                rx.el.div(),
            ),
            rx.cond(
                HomeworkState.feedback_success != "",
                rx.text(HomeworkState.feedback_success, class_name="av-muted"),
                rx.el.div(),
            ),
            rx.cond(
                HomeworkState.feedback_dialog_open,
                rx.el.div(
                    rx.vstack(
                        rx.text(ANALYSIS_FEEDBACK_TAG_LABEL, class_name="av-label"),
                        rx.select(
                            ANALYSIS_FEEDBACK_TAG_OPTIONS,
                            value=HomeworkState.feedback_tag,
                            on_change=HomeworkState.set_feedback_tag,
                            placeholder=ANALYSIS_FEEDBACK_TAG_PLACEHOLDER,
                            width="100%",
                        ),
                        rx.text(ANALYSIS_FEEDBACK_NOTE_LABEL, class_name="av-label"),
                        rx.text_area(
                            value=HomeworkState.feedback_note,
                            on_change=HomeworkState.set_feedback_note,
                            placeholder=ANALYSIS_FEEDBACK_NOTE_PLACEHOLDER,
                            min_height="120px",
                            width="100%",
                        ),
                        rx.cond(
                            HomeworkState.feedback_error != "",
                            rx.text(
                                HomeworkState.feedback_error,
                                class_name="av-error-text",
                            ),
                            rx.el.div(),
                        ),
                        rx.hstack(
                            rx.button(
                                ANALYSIS_FEEDBACK_CANCEL_TEXT,
                                variant="soft",
                                size="1",
                                on_click=HomeworkState.close_feedback_dialog,
                            ),
                            rx.spacer(),
                            rx.button(
                                ANALYSIS_FEEDBACK_SUBMIT_TEXT,
                                size="1",
                                on_click=HomeworkState.submit_feedback,
                                loading=HomeworkState.feedback_submitting,
                                disabled=(
                                    HomeworkState.feedback_submitting
                                    | (HomeworkState.selected_tree_post_uid == "")
                                    | (HomeworkState.feedback_tag == "")
                                ),
                            ),
                            width="100%",
                            align="center",
                        ),
                        spacing="3",
                        align="stretch",
                    ),
                    class_name="av-tree-feedback-box",
                ),
                rx.el.div(),
            ),
            rx.cond(
                HomeworkState.tree_loading,
                rx.el.div(
                    rx.spinner(size="3"),
                    rx.text("加载中…", class_name="av-muted"),
                    class_name="av-dialog-loading",
                ),
                rx.cond(
                    HomeworkState.selected_tree_text != "",
                    rx.vstack(
                        rx.hstack(
                            rx.text(
                                "共 ",
                                HomeworkState.selected_tree_line_count,
                                " 行",
                                class_name="av-muted av-tree-meta",
                            ),
                            rx.spacer(),
                            rx.cond(
                                HomeworkState.tree_text_collapsible,
                                rx.cond(
                                    HomeworkState.tree_show_full_text,
                                    rx.button(
                                        "收起",
                                        variant="soft",
                                        size="1",
                                        on_click=HomeworkState.collapse_tree_text,
                                    ),
                                    rx.button(
                                        "展开全文",
                                        variant="soft",
                                        size="1",
                                        on_click=HomeworkState.expand_tree_text,
                                    ),
                                ),
                                rx.el.div(),
                            ),
                            rx.cond(
                                HomeworkState.tree_wrap_lines,
                                rx.button(
                                    "关闭自动换行",
                                    variant="soft",
                                    size="1",
                                    on_click=HomeworkState.toggle_tree_wrap_lines,
                                ),
                                rx.button(
                                    "开启自动换行",
                                    variant="soft",
                                    size="1",
                                    on_click=HomeworkState.toggle_tree_wrap_lines,
                                ),
                            ),
                            class_name="av-tree-toolbar",
                            width="100%",
                            align="center",
                            spacing="2",
                        ),
                        rx.cond(
                            HomeworkState.tree_wrap_lines,
                            rx.el.div(
                                rx.foreach(
                                    HomeworkState.selected_tree_render_lines,
                                    tree_line_row,
                                ),
                                class_name="av-tree-lines",
                            ),
                            rx.el.pre(
                                HomeworkState.selected_tree_render_text,
                                class_name="av-tree-pre av-tree-pre-nowrap",
                            ),
                        ),
                        spacing="2",
                        align="stretch",
                    ),
                    rx.cond(
                        HomeworkState.selected_tree_message != "",
                        rx.vstack(
                            rx.text(
                                HomeworkState.selected_tree_message,
                                class_name="av-muted",
                            ),
                            rx.cond(
                                HomeworkState.selected_tree_debug_text != "",
                                rx.el.pre(
                                    HomeworkState.selected_tree_debug_text,
                                    class_name="av-tree-pre",
                                ),
                                rx.el.div(),
                            ),
                            spacing="2",
                            align="start",
                        ),
                        rx.text("没有对话流。", class_name="av-muted"),
                    ),
                ),
            ),
            rx.flex(
                rx.dialog.close(
                    rx.button(
                        "关",
                        variant="soft",
                        on_click=HomeworkState.close_tree_dialog,
                    )
                ),
                justify="end",
                margin_top="14px",
            ),
            style={
                "max_width": "min(1100px, 92vw)",
                "max_height": "85vh",
                "overflow": "auto",
            },
        ),
        open=HomeworkState.tree_dialog_open,
        on_open_change=HomeworkState.set_tree_dialog_open,
    )


def homework_page() -> rx.Component:
    return rx.el.div(
        rx.el.div(
            rx.heading("作业板（列表）", size="6"),
            rx.text(
                "点「对话流」会弹窗。列宽可以拖拽。",
                class_name="av-muted",
            ),
            class_name="av-page-head",
        ),
        _controls(),
        rx.cond(
            HomeworkState.load_error != "",
            rx.el.div(
                rx.text("错误：", class_name="av-error-title"),
                rx.text(HomeworkState.load_error, class_name="av-error-text"),
                class_name="av-error",
            ),
            rx.el.div(),
        ),
        rx.cond(
            HomeworkState.caption != "",
            rx.text(
                HomeworkState.caption,
                class_name="av-caption",
                style={"whiteSpace": "pre-line"},
            ),
            rx.el.div(),
        ),
        rx.el.div(_board_table(), class_name="av-main"),
        _tree_dialog(),
        class_name="av-page",
    )
