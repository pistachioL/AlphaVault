from __future__ import annotations

import reflex as rx

from alphavault_reflex.homework_state import HomeworkState


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
    return rx.hstack(
        rx.vstack(
            rx.text("时间窗（天）", class_name="av-label"),
            rx.slider(
                default_value=HomeworkState.window_days,
                min_=1,
                max=HomeworkState.window_max_days,
                on_value_commit=HomeworkState.set_window_days,
                disabled=HomeworkState.show_table_loading,
            ),
            width="240px",
            spacing="2",
        ),
        rx.vstack(
            rx.text("排序", class_name="av-label"),
            rx.select(
                HomeworkState.sort_mode_options,
                value=HomeworkState.sort_mode,
                on_change=HomeworkState.set_sort_mode,
                width="180px",
                disabled=HomeworkState.show_table_loading,
            ),
            spacing="2",
        ),
        rx.vstack(
            rx.text("共识筛选", class_name="av-label"),
            rx.select(
                HomeworkState.consensus_filter_options,
                value=HomeworkState.consensus_filter,
                on_change=HomeworkState.set_consensus_filter,
                width="240px",
                disabled=HomeworkState.show_table_loading,
            ),
            spacing="2",
        ),
        rx.spacer(),
        rx.button(
            "刷新",
            on_click=HomeworkState.load_data,
            class_name="av-btn",
            loading=HomeworkState.show_table_loading,
            disabled=HomeworkState.show_table_loading,
        ),
        spacing="4",
        align="end",
        width="100%",
        class_name="av-controls",
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
        rx.el.td(row["consensus"], class_name="av-td av-consensus"),
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
        rx.el.td(row["recent_action"], class_name="av-td av-recent-action"),
        rx.el.td(row["recent_age"], class_name="av-td av-recent-age"),
        rx.el.td(row["recent_author"], class_name="av-td av-recent-author"),
        rx.el.td(row["net_strength"], class_name="av-td av-num"),
        rx.el.td(row["buy_strength"], class_name="av-td av-num"),
        rx.el.td(row["sell_strength"], class_name="av-td av-num"),
        rx.el.td(row["mentions"], class_name="av-td av-num"),
        rx.el.td(row["author_count"], class_name="av-td av-num"),
        class_name="av-tr",
        on_click=lambda: HomeworkState.open_tree_dialog(row["tree_post_uid"]),
    )


def _board_table() -> rx.Component:
    labels = [
        "主题",
        "共识",
        "总结",
        "对话流",
        "链接",
        "最近动作",
        "最近",
        "最近大佬",
        "净",
        "买",
        "卖",
        "提及",
        "大佬",
    ]
    col_widths = [
        "240px",
        "120px",
        "520px",
        "84px",
        "84px",
        "220px",
        "90px",
        "180px",
        "72px",
        "72px",
        "72px",
        "72px",
        "72px",
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
                        "试试：调大时间窗，或者点「刷新」。",
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
            rx.dialog.title("对话流"),
            rx.cond(
                HomeworkState.selected_tree_label != "",
                rx.text(HomeworkState.selected_tree_label, class_name="av-muted"),
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
                    rx.el.pre(
                        HomeworkState.selected_tree_text,
                        class_name="av-tree-pre",
                    ),
                    rx.cond(
                        HomeworkState.selected_tree_message != "",
                        rx.text(
                            HomeworkState.selected_tree_message,
                            class_name="av-muted",
                        ),
                        rx.text("没有对话流。", class_name="av-muted"),
                    ),
                ),
            ),
            rx.flex(
                rx.dialog.close(rx.button("关", variant="soft")),
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
