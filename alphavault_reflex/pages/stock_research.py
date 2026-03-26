from __future__ import annotations

import reflex as rx

from alphavault_reflex.research_state import ResearchState

EMPTY_TEXT = "暂无。"
LOADING_TEXT = "加载中…"
NO_SIGNAL_TEXT = "没有信号。"


def _signal_card(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
        rx.text(row["summary"], class_name="av-research-signal-title"),
        rx.text(row["action"], class_name="av-research-muted"),
        rx.text(row["author"], class_name="av-research-muted"),
        rx.cond(
            row["display_md"] != "",
            rx.text(row["display_md"], class_name="av-research-signal-body"),
            rx.text(row["raw_text"], class_name="av-research-signal-body"),
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
                "立即 AI 回补",
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


def stock_research_page() -> rx.Component:
    return rx.el.div(
        rx.el.div(
            rx.heading(ResearchState.page_title, size="6"),
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
        rx.el.div(
            rx.el.div(
                rx.heading("最近信号", size="4"),
                rx.cond(
                    ResearchState.show_loading,
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
                    ResearchState.show_loading,
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
                    ResearchState.show_loading,
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
                    ResearchState.show_loading,
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
