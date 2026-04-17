from __future__ import annotations

import importlib
import time

import reflex as rx
from starlette.requests import Request
from starlette.responses import JSONResponse

from alphavault.env import load_dotenv_if_present
from alphavault.logging_config import get_logger
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
from alphavault_reflex.services.process_metrics import load_container_memory_metrics
from alphavault_reflex.services.process_metrics import load_process_metrics

_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)
MANUAL_RSS_TRIGGER_API_PATH = "/api/rss/trigger"
MANUAL_DB_REQUEUE_API_PATH = "/api/admin/requeue-from-db"
MANUAL_PROCESS_METRICS_API_PATH = "/api/admin/processes"
logger = get_logger(__name__)

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


def _load_manual_worker_admin_module():
    return importlib.import_module("alphavault.worker.manual_rss_trigger")


async def _manual_rss_trigger_get(request: Request) -> JSONResponse:
    manual_admin_module = _load_manual_worker_admin_module()
    expected_key = manual_admin_module.load_worker_admin_trigger_key()
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
        result = manual_admin_module.run_manual_rss_ingest_once()
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


def _parse_query_int(value: object, *, default: int) -> int:
    text = str(value or "").strip()
    if not text:
        return int(default)
    try:
        return int(text)
    except Exception:
        return int(default)


def _parse_query_bool(value: object) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


async def _manual_db_requeue_get(request: Request) -> JSONResponse:
    manual_admin_module = _load_manual_worker_admin_module()
    expected_key = manual_admin_module.load_worker_admin_trigger_key()
    if not expected_key:
        return JSONResponse(
            {"ok": False, "error": "missing_manual_trigger_key"},
            status_code=500,
        )

    passed_key = str(request.query_params.get("key") or "").strip()
    if passed_key != expected_key:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    mode = str(request.query_params.get("mode") or "").strip().lower()
    if mode not in {"failed", "legacy_unprocessed"}:
        return JSONResponse({"ok": False, "error": "invalid_mode"}, status_code=400)

    platform = str(request.query_params.get("platform") or "").strip().lower() or None
    limit = _parse_query_int(request.query_params.get("limit"), default=200)
    dry_run = _parse_query_bool(request.query_params.get("dry_run"))

    started_at = time.perf_counter()
    try:
        result = manual_admin_module.run_manual_db_requeue_once(
            mode=mode,
            platform=platform,
            limit=int(limit),
            dry_run=bool(dry_run),
        )
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        return JSONResponse(
            {"ok": False, "error": "manual_db_requeue_failed"},
            status_code=500,
        )

    payload: dict[str, object] = {
        "ok": True,
        "duration_ms": int((time.perf_counter() - started_at) * 1000),
    }
    payload.update(result)
    return JSONResponse(payload, status_code=200)


async def _manual_process_metrics_get(request: Request) -> JSONResponse:
    manual_admin_module = _load_manual_worker_admin_module()
    expected_key = manual_admin_module.load_worker_admin_trigger_key()
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
        result: dict[str, object] = {"processes": load_process_metrics()}
        result.update(load_container_memory_metrics())
    except BaseException as err:
        if isinstance(err, _FATAL_BASE_EXCEPTIONS):
            raise
        logger.exception("manual_process_metrics_failed")
        return JSONResponse(
            {"ok": False, "error": "manual_process_metrics_failed"},
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
app._api.add_route(MANUAL_DB_REQUEUE_API_PATH, _manual_db_requeue_get, methods=["GET"])
app._api.add_route(
    MANUAL_PROCESS_METRICS_API_PATH, _manual_process_metrics_get, methods=["GET"]
)
