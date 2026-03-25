from __future__ import annotations

import reflex as rx

from alphavault_reflex.organizer_state import OrganizerState


def _search_result(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.link(
        rx.hstack(
            rx.text(row["label"]),
            rx.text(row["entity_type"], class_name="av-research-muted"),
            spacing="2",
        ),
        href=row["href"],
        class_name="av-research-search-result",
    )


def index_page() -> rx.Component:
    return rx.center(
        rx.vstack(
            rx.heading("AlphaVault", size="7"),
            rx.text("交易流是入口，也可以直接搜个股和板块。"),
            rx.hstack(
                rx.input(
                    value=OrganizerState.search_query,
                    on_change=OrganizerState.set_search_query,
                    placeholder="搜个股代码、名字、板块",
                    width="360px",
                ),
                rx.button(
                    "搜索", on_click=OrganizerState.run_search, class_name="av-btn"
                ),
                spacing="3",
            ),
            rx.cond(
                OrganizerState.has_search_results,
                rx.el.div(
                    rx.foreach(OrganizerState.search_results, _search_result),
                    class_name="av-research-search-list",
                ),
                rx.el.div(),
            ),
            rx.hstack(
                rx.link("打开交易流", href="/homework", class_name="av-research-chip"),
                rx.link(
                    "打开整理中心", href="/organizer", class_name="av-research-chip"
                ),
                spacing="3",
                margin_top="10px",
            ),
            spacing="4",
            align="center",
        ),
        min_height="100vh",
        padding="24px",
        class_name="av-research-page",
    )
