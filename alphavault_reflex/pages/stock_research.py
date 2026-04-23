from __future__ import annotations

import reflex as rx

from alphavault_reflex.research_state import ResearchState
from alphavault_reflex.research_state import research_page_loading_var
from alphavault_reflex.research_state import stock_page_title_var
from alphavault_reflex.pages.thread_tree_components import tree_line_row
from alphavault_reflex.services.analysis_feedback import (
    ANALYSIS_FEEDBACK_CANCEL_TEXT,
    ANALYSIS_FEEDBACK_DIALOG_TITLE,
    ANALYSIS_FEEDBACK_NOTE_LABEL,
    ANALYSIS_FEEDBACK_NOTE_PLACEHOLDER,
    ANALYSIS_FEEDBACK_SUBMIT_TEXT,
    ANALYSIS_FEEDBACK_TAG_LABEL,
    ANALYSIS_FEEDBACK_TAG_OPTIONS,
    ANALYSIS_FEEDBACK_TAG_PLACEHOLDER,
)
from alphavault_reflex.services.stock_related_feed import StockRelatedPostRow
from alphavault_reflex.services.research_status_text import (
    BACKGROUND_PROCESSING_TEXT,
    BACKGROUND_PROCESSING_TOOLTIP,
)

EMPTY_TEXT = "暂无。"
LOADING_TEXT = "加载中…"
PAGE_LOADING = research_page_loading_var()
PAGE_TITLE = stock_page_title_var()
HELP_MARK_TEXT = "?"
SIDEBAR_TOGGLE_TEXT = "关系"
SIDEBAR_TITLE = "关系"
SIDEBAR_CLOSE_TEXT = "关"
RELATED_SECTION_TITLE = "相关板块"
SIDEBAR_READY_TEXT = "关系数据已就绪。"
SIDEBAR_UPDATED_AT_PREFIX = "扩展数据更新时间："
DETAIL_BUTTON_TEXT = "详情"
DETAIL_DIALOG_TITLE = "帖子详情"
DETAIL_TREE_TITLE = "对话流"
DETAIL_RAW_TITLE = "原文"
SAME_COMPANY_TITLE = "已合并代码"
SAME_COMPANY_HINT = "当前页已一起展示这些代码的帖子。"
TREE_EXPAND_ALL_TEXT = "展开帖子树"
TREE_COLLAPSE_ALL_TEXT = "收起帖子树"
PAGE_PREV_TEXT = "上一页"
PAGE_NEXT_TEXT = "下一页"
PAGE_SIZE_LABEL = "每页"


def _signal_meta_row(row: rx.Var[StockRelatedPostRow]) -> rx.Component:
    return rx.el.div(
        rx.cond(
            row["signal_badge"] != "",
            rx.el.span(
                row["signal_badge"],
                class_name="av-research-chip",
            ),
            rx.el.span(""),
        ),
        rx.text(row["action"], class_name="av-research-muted"),
        rx.cond(
            row["author_href"] != "",
            rx.link(
                row["author"],
                href=row["author_href"],
                class_name="av-research-muted",
            ),
            rx.text(row["author"], class_name="av-research-muted"),
        ),
        rx.cond(
            row["created_at_line"] != "",
            rx.text(
                "发言时间：" + row["created_at_line"],
                class_name="av-research-muted",
            ),
            rx.el.div(),
        ),
        style={
            "display": "flex",
            "flexWrap": "wrap",
            "gap": "10px",
            "alignItems": "center",
        },
    )


def _author_filter_notice() -> rx.Component:
    return rx.cond(
        ResearchState.has_author_filter,
        rx.hstack(
            rx.text(
                "当前作者：" + ResearchState.author_filter,
                class_name="av-research-muted",
            ),
            rx.link(
                "清空作者",
                href=ResearchState.author_filter_clear_href,
                class_name="av-research-chip",
            ),
            spacing="3",
            align="center",
        ),
        rx.el.div(),
    )


def _same_company_chip(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.span(
        row["label"],
        title=row["title"],
        class_name=rx.cond(
            row["is_current"] == "1",
            "av-research-chip av-research-chip-current",
            "av-research-chip",
        ),
    )


def _same_company_notice() -> rx.Component:
    return rx.cond(
        ResearchState.has_same_company_items,
        rx.vstack(
            rx.text(SAME_COMPANY_TITLE, class_name="av-research-muted"),
            rx.text(SAME_COMPANY_HINT, class_name="av-research-muted"),
            rx.el.div(
                rx.foreach(ResearchState.same_company_items, _same_company_chip),
                class_name="av-research-chip-wrap",
            ),
            spacing="2",
            align="stretch",
            width="100%",
        ),
        rx.el.div(),
    )


def _related_post_tree(row: rx.Var[StockRelatedPostRow]) -> rx.Component:
    return rx.cond(
        row["tree_text"] != "",
        rx.cond(
            ResearchState.related_tree_expanded,
            rx.el.div(
                rx.foreach(row["tree_lines"], tree_line_row),
                class_name="av-tree-lines",
            ),
            rx.el.div(
                rx.foreach(row["tree_preview_lines"], tree_line_row),
                class_name="av-tree-lines",
            ),
        ),
        rx.el.div(),
    )


def _related_post_card(row: rx.Var[StockRelatedPostRow]) -> rx.Component:
    return rx.el.div(
        rx.text(row["title"], class_name="av-research-signal-title"),
        _signal_meta_row(row),
        rx.cond(
            row["preview"] != "",
            rx.text(row["preview"], class_name="av-research-signal-body"),
            rx.el.div(),
        ),
        _related_post_tree(row),
        rx.hstack(
            rx.cond(
                row["post_uid"] != "",
                rx.button(
                    DETAIL_BUTTON_TEXT,
                    on_click=lambda: ResearchState.open_signal_detail(
                        row["post_uid"],
                        row["title"],
                    ),
                    variant="soft",
                ),
                rx.el.span(""),
            ),
            rx.cond(
                row["url"] != "",
                rx.link(
                    "原文链",
                    href=row["url"],
                    is_external=True,
                    class_name="av-research-chip",
                ),
                rx.el.span(""),
            ),
            rx.cond(
                row["post_uid"] != "",
                rx.button(
                    "标错并重跑",
                    on_click=lambda: ResearchState.open_feedback_dialog(
                        row["post_uid"]
                    ),
                    variant="soft",
                ),
                rx.el.span(""),
            ),
            spacing="3",
            margin_top="10px",
        ),
        class_name="av-research-card",
    )


def _feedback_dialog() -> rx.Component:
    return rx.dialog.root(
        rx.dialog.content(
            rx.dialog.title(ANALYSIS_FEEDBACK_DIALOG_TITLE),
            rx.vstack(
                rx.text(ANALYSIS_FEEDBACK_TAG_LABEL, class_name="av-label"),
                rx.select(
                    ANALYSIS_FEEDBACK_TAG_OPTIONS,
                    value=ResearchState.feedback_tag,
                    on_change=ResearchState.set_feedback_tag,
                    placeholder=ANALYSIS_FEEDBACK_TAG_PLACEHOLDER,
                    width="100%",
                ),
                rx.text(ANALYSIS_FEEDBACK_NOTE_LABEL, class_name="av-label"),
                rx.text_area(
                    value=ResearchState.feedback_note,
                    on_change=ResearchState.set_feedback_note,
                    placeholder=ANALYSIS_FEEDBACK_NOTE_PLACEHOLDER,
                    min_height="120px",
                    width="100%",
                ),
                rx.cond(
                    ResearchState.feedback_error != "",
                    rx.text(ResearchState.feedback_error, class_name="av-error-text"),
                    rx.el.div(),
                ),
                rx.hstack(
                    rx.dialog.close(
                        rx.button(
                            ANALYSIS_FEEDBACK_CANCEL_TEXT,
                            variant="soft",
                            on_click=ResearchState.close_feedback_dialog,
                        )
                    ),
                    rx.spacer(),
                    rx.button(
                        ANALYSIS_FEEDBACK_SUBMIT_TEXT,
                        on_click=ResearchState.submit_feedback,
                        loading=ResearchState.feedback_submitting,
                        disabled=(
                            ResearchState.feedback_submitting
                            | (ResearchState.feedback_post_uid == "")
                            | (ResearchState.feedback_tag == "")
                        ),
                    ),
                    width="100%",
                    align="center",
                ),
                spacing="3",
                align="stretch",
            ),
            style={"max_width": "min(560px, 92vw)"},
        ),
        open=ResearchState.feedback_dialog_open,
        on_open_change=ResearchState.set_feedback_dialog_open,
    )


def _related_link(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.link(
        row["label"],
        href=row["href"],
        class_name="av-research-chip",
    )


def _signal_detail_dialog() -> rx.Component:
    return rx.dialog.root(
        rx.dialog.content(
            rx.hstack(
                rx.dialog.title(DETAIL_DIALOG_TITLE),
                rx.spacer(),
                rx.dialog.close(
                    rx.button(
                        SIDEBAR_CLOSE_TEXT,
                        variant="soft",
                        on_click=ResearchState.close_signal_detail,
                    )
                ),
                width="100%",
                align="center",
            ),
            rx.cond(
                ResearchState.signal_detail_title != "",
                rx.text(
                    ResearchState.signal_detail_title,
                    class_name="av-research-signal-title",
                ),
                rx.el.div(),
            ),
            rx.cond(
                ResearchState.signal_detail_loading,
                _section_loading(),
                rx.vstack(
                    rx.cond(
                        ResearchState.signal_detail_tree_text != "",
                        rx.vstack(
                            rx.text(DETAIL_TREE_TITLE, class_name="av-research-muted"),
                            rx.el.div(
                                rx.foreach(
                                    ResearchState.signal_detail_tree_lines,
                                    tree_line_row,
                                ),
                                class_name="av-tree-lines",
                            ),
                            spacing="2",
                            align="stretch",
                            width="100%",
                        ),
                        rx.el.div(),
                    ),
                    rx.cond(
                        ResearchState.signal_detail_raw_text != "",
                        rx.vstack(
                            rx.text(DETAIL_RAW_TITLE, class_name="av-research-muted"),
                            rx.el.pre(
                                ResearchState.signal_detail_raw_text,
                                class_name="av-tree-pre",
                            ),
                            spacing="2",
                            align="stretch",
                            width="100%",
                        ),
                        rx.el.div(),
                    ),
                    rx.cond(
                        (ResearchState.signal_detail_tree_text == "")
                        & (ResearchState.signal_detail_raw_text == "")
                        & (ResearchState.signal_detail_message != ""),
                        rx.text(
                            ResearchState.signal_detail_message,
                            class_name="av-research-muted",
                        ),
                        rx.el.div(),
                    ),
                    spacing="3",
                    align="stretch",
                    width="100%",
                ),
            ),
            style={
                "max_width": "min(960px, 92vw)",
                "max_height": "85vh",
                "overflow": "auto",
            },
        ),
        open=ResearchState.signal_detail_open,
        on_open_change=ResearchState.set_signal_detail_open,
    )


def _section_loading() -> rx.Component:
    return rx.el.div(
        rx.spinner(size="3"),
        rx.text(LOADING_TEXT, class_name="av-research-muted"),
        style={
            "display": "flex",
            "alignItems": "center",
            "gap": "10px",
            "marginTop": "12px",
        },
    )


def _hint_with_help(text: str, help_text: str) -> rx.Component:
    return rx.el.div(
        rx.text(text, class_name="av-research-muted"),
        rx.el.span(
            HELP_MARK_TEXT,
            title=help_text,
            class_name="av-research-muted",
            style={
                "display": "inline-flex",
                "alignItems": "center",
                "justifyContent": "center",
                "width": "16px",
                "height": "16px",
                "border": "1px solid #9ca3af",
                "borderRadius": "999px",
                "cursor": "help",
                "fontWeight": "600",
                "fontSize": "12px",
                "lineHeight": "1",
            },
        ),
        style={
            "display": "flex",
            "alignItems": "center",
            "gap": "6px",
        },
    )


def _stock_sidebar_sections() -> rx.Component:
    return rx.el.div(
        rx.cond(
            ResearchState.show_extras_loading,
            _section_loading(),
            rx.el.div(
                rx.cond(
                    ResearchState.extras_updated_at != "",
                    rx.text(
                        SIDEBAR_UPDATED_AT_PREFIX + ResearchState.extras_updated_at,
                        class_name="av-research-muted",
                    ),
                    rx.text(SIDEBAR_READY_TEXT, class_name="av-research-muted"),
                ),
                rx.heading(RELATED_SECTION_TITLE, size="4", margin_top="12px"),
                rx.cond(
                    ResearchState.has_related_items,
                    rx.el.div(
                        rx.foreach(ResearchState.related_items, _related_link),
                        class_name="av-research-chip-wrap",
                    ),
                    rx.cond(
                        ResearchState.show_related_empty,
                        rx.text(EMPTY_TEXT, class_name="av-research-muted"),
                        rx.el.div(),
                    ),
                ),
            ),
        ),
    )


def _stock_sidebar() -> rx.Component:
    return rx.el.div(
        rx.cond(
            ResearchState.stock_sidebar_open,
            rx.el.div(
                on_click=ResearchState.close_stock_sidebar,
                class_name="av-stock-sidebar-backdrop",
            ),
            rx.el.div(),
        ),
        rx.el.aside(
            rx.el.div(
                rx.heading(SIDEBAR_TITLE, size="4"),
                rx.button(
                    SIDEBAR_CLOSE_TEXT,
                    on_click=ResearchState.close_stock_sidebar,
                    variant="soft",
                    class_name="av-stock-sidebar-close",
                ),
                class_name="av-stock-sidebar-head",
            ),
            _stock_sidebar_sections(),
            class_name=rx.cond(
                ResearchState.stock_sidebar_open,
                "av-research-side av-stock-sidebar-panel av-stock-sidebar-panel-open",
                "av-research-side av-stock-sidebar-panel",
            ),
        ),
        class_name=rx.cond(
            ResearchState.stock_sidebar_open,
            "av-stock-sidebar-shell av-stock-sidebar-shell-open",
            "av-stock-sidebar-shell",
        ),
    )


def _related_tree_toggle_button() -> rx.Component:
    return rx.cond(
        ResearchState.has_related_posts,
        rx.cond(
            ResearchState.related_tree_expanded,
            rx.button(
                TREE_COLLAPSE_ALL_TEXT,
                on_click=ResearchState.collapse_related_tree,
                variant="soft",
                disabled=PAGE_LOADING,
            ),
            rx.button(
                TREE_EXPAND_ALL_TEXT,
                on_click=ResearchState.expand_related_tree,
                variant="soft",
                disabled=PAGE_LOADING,
            ),
        ),
        rx.el.div(),
    )


def _related_pagination() -> rx.Component:
    return rx.cond(
        ResearchState.signal_total > 0,
        rx.hstack(
            rx.text(ResearchState.signal_page_caption, class_name="av-research-muted"),
            rx.spacer(),
            rx.text(PAGE_SIZE_LABEL, class_name="av-research-muted"),
            rx.select(
                ResearchState.signal_page_size_options,
                value=ResearchState.signal_page_size_text,
                on_change=ResearchState.set_signal_page_size,
                width="88px",
            ),
            rx.button(
                PAGE_PREV_TEXT,
                on_click=ResearchState.prev_signal_page,
                variant="soft",
                disabled=PAGE_LOADING | (ResearchState.signal_page <= 1),
            ),
            rx.button(
                PAGE_NEXT_TEXT,
                on_click=ResearchState.next_signal_page,
                variant="soft",
                disabled=PAGE_LOADING
                | (ResearchState.signal_page >= ResearchState.signal_total_pages),
            ),
            spacing="3",
            align="center",
            width="100%",
            margin_top="12px",
        ),
        rx.el.div(),
    )


def stock_research_page() -> rx.Component:
    return rx.el.div(
        rx.el.div(
            rx.heading(PAGE_TITLE, size="6"),
            rx.text("相关帖子", class_name="av-research-muted"),
            _same_company_notice(),
            class_name="av-research-head",
        ),
        rx.cond(
            ResearchState.load_error != "",
            rx.el.div(
                rx.text("错误：", class_name="av-error-title"),
                rx.text(ResearchState.load_error, class_name="av-error-text"),
                class_name="av-error",
            ),
            rx.el.div(),
        ),
        rx.cond(
            (ResearchState.stock_load_warning != "")
            & (ResearchState.stock_load_warning != BACKGROUND_PROCESSING_TEXT),
            rx.text(ResearchState.stock_load_warning, class_name="av-research-muted"),
            rx.el.div(),
        ),
        rx.cond(
            ResearchState.worker_status_text != "",
            rx.text(ResearchState.worker_status_text, class_name="av-research-muted"),
            rx.el.div(),
        ),
        rx.cond(
            ResearchState.feedback_success != "",
            rx.text(ResearchState.feedback_success, class_name="av-research-muted"),
            rx.el.div(),
        ),
        rx.cond(
            ResearchState.worker_next_run_at != "",
            rx.text(
                "下一轮时间：" + ResearchState.worker_next_run_at,
                class_name="av-research-muted",
            ),
            rx.el.div(),
        ),
        rx.cond(
            ResearchState.worker_cycle_updated_at != "",
            rx.text(
                "状态更新时间：" + ResearchState.worker_cycle_updated_at,
                class_name="av-research-muted",
            ),
            rx.el.div(),
        ),
        rx.cond(
            ResearchState.signals_ready,
            rx.text("数据已就绪。", class_name="av-research-muted"),
            _hint_with_help(BACKGROUND_PROCESSING_TEXT, BACKGROUND_PROCESSING_TOOLTIP),
        ),
        _author_filter_notice(),
        rx.el.div(
            rx.el.div(
                rx.hstack(
                    rx.heading("相关帖子", size="4"),
                    rx.spacer(),
                    rx.button(
                        "全部帖子",
                        on_click=lambda: ResearchState.set_related_filter("all"),
                        variant=rx.cond(
                            ResearchState.related_filter == "all",
                            "solid",
                            "soft",
                        ),
                        disabled=PAGE_LOADING,
                    ),
                    rx.button(
                        "只看交易信号",
                        on_click=lambda: ResearchState.set_related_filter("signal"),
                        variant=rx.cond(
                            ResearchState.related_filter == "signal",
                            "solid",
                            "soft",
                        ),
                        disabled=PAGE_LOADING,
                    ),
                    _related_tree_toggle_button(),
                    rx.button(
                        SIDEBAR_TOGGLE_TEXT,
                        on_click=ResearchState.open_stock_sidebar,
                        variant="soft",
                        disabled=PAGE_LOADING,
                        class_name="av-stock-sidebar-toggle",
                    ),
                    rx.button(
                        "刷新",
                        on_click=ResearchState.refresh_stock_related,
                        variant="soft",
                        disabled=PAGE_LOADING,
                    ),
                    spacing="2",
                    align="center",
                    width="100%",
                ),
                rx.cond(
                    PAGE_LOADING,
                    _section_loading(),
                    rx.cond(
                        ResearchState.has_related_posts,
                        rx.el.div(
                            rx.foreach(ResearchState.related_posts, _related_post_card),
                            class_name="av-research-list",
                        ),
                        rx.cond(
                            ResearchState.show_related_posts_empty,
                            rx.text(EMPTY_TEXT, class_name="av-research-muted"),
                            rx.el.div(),
                        ),
                    ),
                ),
                _related_pagination(),
                class_name="av-research-main",
            ),
            class_name="av-research-layout av-stock-research-layout",
        ),
        _feedback_dialog(),
        _signal_detail_dialog(),
        _stock_sidebar(),
        class_name="av-research-page",
    )
