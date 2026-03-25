from __future__ import annotations

import reflex as rx

from alphavault_reflex.pages.homework import homework_page, index_page
from alphavault_reflex.state import HomeworkState

app = rx.App(
    theme=rx.theme(appearance="light"),
    stylesheets=[
        "/homework_board.css",
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
