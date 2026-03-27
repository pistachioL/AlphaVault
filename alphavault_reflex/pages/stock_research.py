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
NO_SIGNAL_TEXT = "没有信号。"
PAGE_LOADING = research_page_loading_var()
PAGE_TITLE = stock_page_title_var()
HELP_MARK_TEXT = "?"


def _signal_meta_row(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
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


def _signal_pager() -> rx.Component:
    return rx.hstack(
        rx.button(
            "上一页",
            on_click=ResearchState.prev_signal_page,
            variant="soft",
            disabled=PAGE_LOADING | (ResearchState.signal_page <= 1),
        ),
        rx.text(ResearchState.signal_page_caption, class_name="av-research-muted"),
        rx.button(
            "下一页",
            on_click=ResearchState.next_signal_page,
            variant="soft",
            disabled=PAGE_LOADING
            | (ResearchState.signal_page >= ResearchState.signal_total_pages),
        ),
        rx.spacer(),
        rx.text("每页", class_name="av-research-muted"),
        rx.select(
            ResearchState.signal_page_size_options,
            value=ResearchState.signal_page_size_text,
            on_change=ResearchState.set_signal_page_size,
            width="120px",
            disabled=PAGE_LOADING,
        ),
        spacing="3",
        align="center",
        width="100%",
        margin_top="10px",
    )


def _signal_card(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
        rx.text(row["summary"], class_name="av-research-signal-title"),
        _signal_meta_row(row),
        rx.cond(
            row["tree_text"] != "",
            rx.el.pre(row["tree_text"], class_name="av-research-signal-tree"),
            rx.cond(
                row["display_md"] != "",
                rx.text(row["display_md"], class_name="av-research-signal-body"),
                rx.text(row["raw_text"], class_name="av-research-signal-body"),
            ),
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


def _backfill_item(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
        rx.text(row["matched_terms"], class_name="av-research-side-title"),
        rx.text(row["author"], class_name="av-research-muted"),
        rx.text(row["preview"], class_name="av-research-muted"),
        rx.hstack(
            rx.button(
                "排队 AI 回补",
                on_click=lambda: ResearchState.queue_backfill_post(row["post_uid"]),
                class_name="av-btn av-btn-small",
            ),
            rx.cond(
                row["url"] != "",
                rx.link("打开原文", href=row["url"], is_external=True),
                rx.el.span(""),
            ),
            spacing="3",
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
            rx.text("最近买卖信号 + 原文", class_name="av-research-muted"),
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
                rx.heading("最近信号", size="4"),
                rx.cond(
                    PAGE_LOADING,
                    rx.el.div(),
                    rx.cond(
                        ResearchState.signal_page_caption != "",
                        _signal_pager(),
                        rx.el.div(),
                    ),
                ),
                rx.cond(
                    PAGE_LOADING,
                    _section_loading(),
                    rx.cond(
                        ResearchState.has_signals,
                        rx.el.div(
                            rx.foreach(ResearchState.primary_signals, _signal_card),
                            class_name="av-research-list",
                        ),
                        rx.cond(
                            ResearchState.show_signal_empty,
                            rx.text(NO_SIGNAL_TEXT, class_name="av-research-muted"),
                            rx.el.div(),
                        ),
                    ),
                ),
                rx.heading("待回补文章", size="4", margin_top="24px"),
                rx.cond(
                    ResearchState.show_extras_loading,
                    _section_loading(),
                    rx.cond(
                        ResearchState.has_backfill_posts,
                        rx.el.div(
                            rx.foreach(ResearchState.backfill_posts, _backfill_item),
                            class_name="av-research-side-list",
                        ),
                        rx.cond(
                            ResearchState.show_backfill_empty,
                            rx.text(EMPTY_TEXT, class_name="av-research-muted"),
                            rx.el.div(),
                        ),
                    ),
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
