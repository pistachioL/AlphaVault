from __future__ import annotations

import reflex as rx

from alphavault_reflex.research_state import ResearchState
from alphavault_reflex.research_state import research_page_loading_var
from alphavault_reflex.research_state import stock_page_title_var
from alphavault_reflex.services.research_status_text import (
    BACKGROUND_PROCESSING_TEXT,
    BACKGROUND_PROCESSING_TOOLTIP,
)

EMPTY_TEXT = "暂无。"
LOADING_TEXT = "加载中…"
PAGE_LOADING = research_page_loading_var()
PAGE_TITLE = stock_page_title_var()
HELP_MARK_TEXT = "?"


def _signal_meta_row(row: rx.Var[dict[str, str]]) -> rx.Component:
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
            rx.text(row["created_at_line"], class_name="av-research-muted"),
            rx.el.div(),
        ),
        style={
            "display": "flex",
            "flexWrap": "wrap",
            "gap": "10px",
            "alignItems": "center",
        },
    )


def _related_post_card(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
        rx.text(row["title"], class_name="av-research-signal-title"),
        _signal_meta_row(row),
        rx.cond(
            row["tree_text"] != "",
            rx.el.pre(row["tree_text"], class_name="av-research-signal-tree"),
            rx.cond(
                row["display_md"] != "",
                rx.text(row["display_md"], class_name="av-research-signal-body"),
                rx.cond(
                    row["raw_text"] != "",
                    rx.text(row["raw_text"], class_name="av-research-signal-body"),
                    rx.text(row["preview"], class_name="av-research-signal-body"),
                ),
            ),
        ),
        rx.hstack(
            rx.cond(
                row["is_signal"] != "1",
                rx.button(
                    "排队 AI 回补",
                    on_click=lambda: ResearchState.queue_backfill_post(row["post_uid"]),
                    class_name="av-btn av-btn-small",
                ),
                rx.el.span(""),
            ),
            rx.cond(
                row["url"] != "",
                rx.link("打开原文", href=row["url"], is_external=True),
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


def _pending_item(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
        rx.text(row["candidate_key"], class_name="av-research-side-title"),
        rx.text(row["evidence_summary"], class_name="av-research-muted"),
        rx.hstack(
            rx.button(
                "确认",
                on_click=lambda: ResearchState.accept_candidate(row["candidate_id"]),
                class_name="av-btn av-btn-small",
            ),
            rx.button(
                "忽略",
                on_click=lambda: ResearchState.ignore_candidate(row["candidate_id"]),
                variant="soft",
            ),
            rx.button(
                "不再推荐",
                on_click=lambda: ResearchState.block_candidate(row["candidate_id"]),
                variant="soft",
                color_scheme="gray",
            ),
            spacing="2",
            margin_top="10px",
        ),
        class_name="av-research-side-item",
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
            ResearchState.signals_ready & ResearchState.extras_ready,
            rx.cond(
                ResearchState.extras_updated_at != "",
                rx.text(
                    "扩展数据更新时间：" + ResearchState.extras_updated_at,
                    class_name="av-research-muted",
                ),
                rx.text("数据已就绪。", class_name="av-research-muted"),
            ),
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
            rx.el.aside(
                rx.heading("相关板块", size="4"),
                rx.cond(
                    PAGE_LOADING,
                    _section_loading(),
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
                rx.heading("待确认关系", size="4", margin_top="18px"),
                rx.cond(
                    ResearchState.show_extras_loading,
                    _section_loading(),
                    rx.cond(
                        ResearchState.has_pending_candidates,
                        rx.el.div(
                            rx.foreach(ResearchState.pending_candidates, _pending_item),
                            class_name="av-research-side-list",
                        ),
                        rx.cond(
                            ResearchState.show_pending_empty,
                            rx.text(EMPTY_TEXT, class_name="av-research-muted"),
                            rx.el.div(),
                        ),
                    ),
                ),
                rx.cond(
                    ResearchState.backfill_notice != "",
                    rx.text(
                        ResearchState.backfill_notice, class_name="av-research-muted"
                    ),
                    rx.el.div(),
                ),
                class_name="av-research-side",
            ),
            class_name="av-research-layout",
        ),
        class_name="av-research-page",
    )
