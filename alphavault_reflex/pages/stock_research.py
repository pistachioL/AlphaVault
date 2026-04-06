from __future__ import annotations

import reflex as rx

from alphavault_reflex.research_state import ResearchState
from alphavault_reflex.research_state import research_page_loading_var
from alphavault_reflex.research_state import stock_page_title_var
from alphavault_reflex.pages.thread_tree_components import tree_line_row
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
        rx.text(row["author"], class_name="av-research-muted"),
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


def _related_post_card(row: rx.Var[StockRelatedPostRow]) -> rx.Component:
    return rx.el.div(
        rx.text(row["title"], class_name="av-research-signal-title"),
        _signal_meta_row(row),
        rx.cond(
            row["tree_text"] != "",
            rx.el.div(
                rx.foreach(row["tree_lines"], tree_line_row),
                class_name="av-tree-lines",
            ),
            rx.cond(
                row["raw_text"] != "",
                rx.text(row["raw_text"], class_name="av-research-signal-body"),
                rx.cond(
                    row["preview"] != "",
                    rx.text(row["preview"], class_name="av-research-signal-body"),
                    rx.el.div(),
                ),
            ),
        ),
        rx.hstack(
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
            spacing="3",
            margin_top="10px",
        ),
        class_name="av-research-card",
    )


def _related_link(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.link(
        row["label"],
        href=row["href"],
        class_name="av-research-chip",
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


def stock_research_page() -> rx.Component:
    return rx.el.div(
        rx.el.div(
            rx.heading(PAGE_TITLE, size="6"),
            rx.text("相关帖子", class_name="av-research-muted"),
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
        rx.el.div(
            rx.el.div(
                rx.hstack(
                    rx.heading("相关帖子", size="4"),
                    rx.spacer(),
                    rx.button(
                        "全部",
                        on_click=lambda: ResearchState.set_related_filter("all"),
                        variant=rx.cond(
                            ResearchState.related_filter == "all",
                            "solid",
                            "soft",
                        ),
                        disabled=PAGE_LOADING,
                    ),
                    rx.button(
                        "只看信号",
                        on_click=lambda: ResearchState.set_related_filter("signal"),
                        variant=rx.cond(
                            ResearchState.related_filter == "signal",
                            "solid",
                            "soft",
                        ),
                        disabled=PAGE_LOADING,
                    ),
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
                rx.cond(
                    ResearchState.related_has_more,
                    rx.button(
                        "加载更多",
                        on_click=ResearchState.load_more_related,
                        variant="soft",
                        disabled=PAGE_LOADING,
                        margin_top="12px",
                    ),
                    rx.el.div(),
                ),
                class_name="av-research-main",
            ),
            class_name="av-research-layout av-stock-research-layout",
        ),
        _stock_sidebar(),
        class_name="av-research-page",
    )
