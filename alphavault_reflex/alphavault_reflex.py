from __future__ import annotations

import reflex as rx

from alphavault_reflex.organizer_state import OrganizerState
from alphavault_reflex.pages.homework import homework_page
from alphavault_reflex.pages.index import index_page
from alphavault_reflex.pages.organizer import organizer_page
from alphavault_reflex.homework_state import HomeworkState
from alphavault_reflex.pages.sector_research import sector_research_page
from alphavault_reflex.pages.stock_research import stock_research_page
from alphavault_reflex.research_state import ResearchState

app = rx.App(
    theme=rx.theme(appearance="light"),
    stylesheets=[
        "/homework_board.css",
        "/research_workbench.css",
    ],
    head_components=[
        rx.script(src="/table_resizer.js"),
    ],
)

app.add_page(index_page, route="/", title="AlphaVault")
app.add_page(
    homework_page,
    route="/homework",
    title="作业板",
    on_load=HomeworkState.load_data,
)
app.add_page(
    stock_research_page,
    route="/research/stocks/[stock_slug]",
    title="个股研究",
    on_load=ResearchState.load_stock_page,
)
app.add_page(
    sector_research_page,
    route="/research/sectors/[sector_slug]",
    title="板块研究",
    on_load=ResearchState.load_sector_page,
)
app.add_page(
    organizer_page,
    route="/organizer",
    title="整理中心",
    on_load=OrganizerState.load_pending,
)
