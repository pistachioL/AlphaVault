from __future__ import annotations

import reflex as rx

from alphavault_reflex.research_state import ResearchState


def stock_route_link(
    *children: rx.Component | str | rx.Var[str],
    href: str | rx.Var[str],
    class_name: str = "",
    **props: object,
) -> rx.Component:
    return rx.link(
        *children,
        href=href,
        on_click=ResearchState.prepare_stock_href_navigation(href),
        class_name=class_name,
        **props,
    )


__all__ = ["stock_route_link"]
