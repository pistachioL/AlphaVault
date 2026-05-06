from __future__ import annotations

from starlette.routing import Mount
from starlette.testclient import TestClient

from alphavault.mcp_server import MCP_ROUTE_MOUNT_PATH
from alphavault_reflex.alphavault_reflex import app
from alphavault_reflex.mcp_history_constants import MCP_HISTORY_ROUTE
from alphavault_reflex.pages.mcp_history import mcp_history_page
from alphavault_reflex.pages.organizer import organizer_page
from alphavault_reflex.pages.search_posts import search_posts_page
from alphavault_reflex.services.original_link import ORIGINAL_LINK_SCRIPT_PATH


def test_homework_page_on_load_uses_if_needed_handler() -> None:
    handler = app._load_events["homework"][0]

    assert handler.fn.__name__ == "load_data_if_needed"


def test_stock_page_on_load_uses_if_needed_handler() -> None:
    handler = app._load_events["research/stocks/[stock_slug]"][0]

    assert handler.fn.__name__ == "load_stock_page_if_needed"


def test_search_posts_page_on_load_uses_if_needed_handler() -> None:
    handler = app._load_events["search/posts"][0]

    assert handler.fn.__name__ == "load_search_if_needed"


def test_mcp_history_page_on_load_uses_if_needed_handler() -> None:
    handler = app._load_events[MCP_HISTORY_ROUTE.lstrip("/")][0]

    assert handler.fn.__name__ == "load_history_if_needed"


def test_sector_page_on_load_uses_if_needed_handler() -> None:
    handler = app._load_events["research/sectors/[sector_slug]"][0]

    assert handler.fn.__name__ == "load_sector_page_if_needed"


def test_organizer_page_builds_without_type_errors() -> None:
    organizer_page()


def test_search_posts_page_builds_without_type_errors() -> None:
    search_posts_page()


def test_mcp_history_page_builds_without_type_errors() -> None:
    mcp_history_page()


def test_app_mounts_mcp_http_app() -> None:
    assert any(
        isinstance(route, Mount) and route.path == MCP_ROUTE_MOUNT_PATH
        for route in app._api.routes
    )


def test_mcp_http_app_redirects_slashless_mount_path() -> None:
    with TestClient(app._api) as client:
        response = client.get(
            MCP_ROUTE_MOUNT_PATH,
            headers={"CF-Access-Client-Id": "svc-alpha"},
            follow_redirects=False,
        )

    assert response.status_code == 307
    assert (
        response.headers.get("location") == f"http://testserver{MCP_ROUTE_MOUNT_PATH}/"
    )


def test_mcp_http_app_runs_under_parent_lifespan() -> None:
    with TestClient(app._api) as client:
        response = client.get(
            f"{MCP_ROUTE_MOUNT_PATH}/",
            headers={"CF-Access-Client-Id": "svc-alpha"},
            follow_redirects=False,
        )

    assert response.status_code < 500


def test_mcp_http_app_runs_under_reflex_factory_lifespan() -> None:
    with TestClient(app()) as client:
        response = client.get(
            f"{MCP_ROUTE_MOUNT_PATH}/",
            headers={"CF-Access-Client-Id": "svc-alpha"},
            follow_redirects=False,
        )

    assert response.status_code < 500


def test_app_registers_original_link_script() -> None:
    head_children = []
    for component in app.head_components:
        rendered = component.render()
        head_children.extend(rendered["children"])

    assert {
        "name": '"script"',
        "props": [f'src:"{ORIGINAL_LINK_SCRIPT_PATH}"'],
        "children": [],
    } in head_children
