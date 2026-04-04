from __future__ import annotations

import reflex as rx

from alphavault_reflex.research_state import ResearchState
from alphavault_reflex.research_state import research_page_loading_var
from alphavault_reflex.research_state import sector_page_title_var

EMPTY_TEXT = "暂无。"
LOADING_TEXT = "加载中…"
NO_SIGNAL_TEXT = "没有信号。"
PAGE_LOADING = research_page_loading_var()
PAGE_TITLE = sector_page_title_var()


def _signal_card(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
        rx.text(row["summary"], class_name="av-research-signal-title"),
        rx.text(row["action"], class_name="av-research-muted"),
        rx.text(row["author"], class_name="av-research-muted"),
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


def sector_research_page() -> rx.Component:
    return rx.el.div(
        rx.el.div(
            rx.heading(PAGE_TITLE, size="6"),
            rx.text("板块内最近最强信号", class_name="av-research-muted"),
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
        rx.el.div(
            rx.el.div(
                rx.heading("板块信号", size="4"),
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
                class_name="av-research-main",
            ),
            rx.el.aside(
                rx.heading("板块内个股", size="4"),
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
                class_name="av-research-side",
            ),
            class_name="av-research-layout",
        ),
        class_name="av-research-page",
    )
