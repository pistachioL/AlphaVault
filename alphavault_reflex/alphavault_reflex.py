from __future__ import annotations

import time

import reflex as rx
from starlette.requests import Request
from starlette.responses import JSONResponse

from alphavault.env import load_dotenv_if_present
from alphavault.worker.manual_rss_trigger import (
    load_manual_rss_trigger_key,
    run_manual_rss_ingest_once,
)
from alphavault_reflex.organizer_state import OrganizerState
from alphavault_reflex.homework_state import HomeworkState
from alphavault_reflex.pages.homework import homework_page
from alphavault_reflex.pages.index import index_page
from alphavault_reflex.pages.organizer import organizer_page
from alphavault_reflex.pages.sector_research import sector_research_page
from alphavault_reflex.pages.stock_research import stock_research_page
from alphavault_reflex.research_state import ResearchState
from alphavault_reflex.research_state import sector_browser_title_var
from alphavault_reflex.research_state import stock_browser_title_var

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
MANUAL_RSS_TRIGGER_API_PATH = "/api/rss/trigger"

load_dotenv_if_present()

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
    on_load=HomeworkState.load_data_if_needed,
)
app.add_page(
    stock_research_page,
    route="/research/stocks/[stock_slug]",
    title=stock_browser_title_var(),
    on_load=ResearchState.load_stock_page_if_needed,
)
app.add_page(
    sector_research_page,
    route="/research/sectors/[sector_slug]",
    title=sector_browser_title_var(),
    on_load=ResearchState.load_sector_page_if_needed,
)
app.add_page(
    organizer_page,
    route="/organizer",
    title="整理中心",
    on_load=OrganizerState.load_pending,
)


async def _manual_rss_trigger_get(request: Request) -> JSONResponse:
    expected_key = load_manual_rss_trigger_key()
    if not expected_key:
        return JSONResponse(
            {"ok": False, "error": "missing_manual_trigger_key"},
            status_code=500,
        )

    passed_key = str(request.query_params.get("key") or "").strip()
    if passed_key != expected_key:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    started_at = time.perf_counter()
    try:
        result = run_manual_rss_ingest_once()
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        return JSONResponse(
            {"ok": False, "error": "manual_rss_trigger_failed"},
            status_code=500,
        )

    payload: dict[str, object] = {
        "ok": True,
        "duration_ms": int((time.perf_counter() - started_at) * 1000),
    }
    payload.update(result)
    return JSONResponse(payload, status_code=200)


app._api.add_route(
    MANUAL_RSS_TRIGGER_API_PATH, _manual_rss_trigger_get, methods=["GET"]
)
