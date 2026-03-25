from __future__ import annotations

import reflex as rx

from alphavault_reflex.organizer_state import (
    OrganizerState,
    SECTION_SECTOR_SECTOR,
    SECTION_STOCK_ALIAS,
    SECTION_STOCK_SECTOR,
)


def _candidate_card(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
        rx.text(row["candidate_key"], class_name="av-research-side-title"),
        rx.text(row["suggestion_reason"], class_name="av-research-muted"),
        rx.text(row["evidence_summary"], class_name="av-research-muted"),
        rx.hstack(
            rx.button(
                "确认",
                on_click=lambda: OrganizerState.accept_candidate(row["candidate_id"]),
                class_name="av-btn av-btn-small",
            ),
            rx.button(
                "忽略",
                on_click=lambda: OrganizerState.ignore_candidate(row["candidate_id"]),
                variant="soft",
            ),
            rx.button(
                "不再推荐",
                on_click=lambda: OrganizerState.block_candidate(row["candidate_id"]),
                variant="soft",
                color_scheme="gray",
            ),
            spacing="2",
            margin_top="10px",
        ),
        class_name="av-research-side-item",
    )


def organizer_page() -> rx.Component:
    return rx.el.div(
        rx.el.div(
            rx.heading("整理中心", size="6"),
            rx.text("先看个股别名，再看个股到板块，最后看板块关系。"),
            class_name="av-research-head",
        ),
        rx.hstack(
            rx.button(
                "个股归并",
                on_click=lambda: OrganizerState.set_active_section(SECTION_STOCK_ALIAS),
                variant="soft",
            ),
            rx.button(
                "个股→板块",
                on_click=lambda: OrganizerState.set_active_section(
                    SECTION_STOCK_SECTOR
                ),
                variant="soft",
            ),
            rx.button(
                "板块→板块",
                on_click=lambda: OrganizerState.set_active_section(
                    SECTION_SECTOR_SECTOR
                ),
                variant="soft",
            ),
            spacing="3",
        ),
        rx.cond(
            OrganizerState.load_error != "",
            rx.el.div(
                rx.text("错误：", class_name="av-error-title"),
                rx.text(OrganizerState.load_error, class_name="av-error-text"),
                class_name="av-error",
            ),
            rx.el.div(),
        ),
        rx.cond(
            OrganizerState.has_pending_rows,
            rx.el.div(
                rx.foreach(OrganizerState.pending_rows, _candidate_card),
                class_name="av-research-side-list",
            ),
            rx.text("当前分区没有待确认项。", class_name="av-research-muted"),
        ),
        class_name="av-research-page",
    )
