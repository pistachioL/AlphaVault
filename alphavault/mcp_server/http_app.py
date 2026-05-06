from __future__ import annotations

import asyncio

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from .request_meta import McpRequestMetaMiddleware, require_current_request_meta
from .tool_runner import (
    run_get_post_detail_tool,
    run_get_stock_page_tool,
    run_resolve_stock_tool,
    run_search_posts_tool,
)

MCP_ROUTE_MOUNT_PATH = "/mcp"
_DEFAULT_STOCK_CANDIDATE_LIMIT = 5
_DEFAULT_SIGNAL_PAGE = 1
_DEFAULT_SIGNAL_PAGE_SIZE = 10
_DEFAULT_POST_SEARCH_LIMIT = 10
_DEFAULT_RELATED_FILTER = "all"


def create_mcp_http_app():
    server = FastMCP(
        "AlphaVault MCP",
        instructions="通过 AlphaVault 数据库检索个股、帖子和帖子详情。",
        streamable_http_path="/",
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=False
        ),
    )

    @server.tool(
        name="resolve_stock",
        description="把自然语言股票输入解析成唯一股票或候选列表。",
    )
    async def resolve_stock(
        query: str,
        limit: int = _DEFAULT_STOCK_CANDIDATE_LIMIT,
    ) -> dict[str, object]:
        request_meta = require_current_request_meta()
        return await asyncio.to_thread(
            run_resolve_stock_tool,
            request_meta=request_meta,
            query=query,
            limit=limit,
        )

    @server.tool(
        name="get_stock_page",
        description="读取个股研究页信号列表、相关板块和同公司股票。",
    )
    async def get_stock_page(
        stock: str,
        signal_page: int = _DEFAULT_SIGNAL_PAGE,
        signal_page_size: int = _DEFAULT_SIGNAL_PAGE_SIZE,
        author: str = "",
        related_filter: str = _DEFAULT_RELATED_FILTER,
    ) -> dict[str, object]:
        request_meta = require_current_request_meta()
        return await asyncio.to_thread(
            run_get_stock_page_tool,
            request_meta=request_meta,
            stock=stock,
            signal_page=signal_page,
            signal_page_size=signal_page_size,
            author=author,
            related_filter=related_filter,
        )

    @server.tool(
        name="search_posts",
        description="按关键字搜索帖子正文、提及词和结构化实体。",
    )
    async def search_posts(
        query: str,
        limit: int = _DEFAULT_POST_SEARCH_LIMIT,
        cursor: str = "",
    ) -> dict[str, object]:
        request_meta = require_current_request_meta()
        return await asyncio.to_thread(
            run_search_posts_tool,
            request_meta=request_meta,
            query=query,
            limit=limit,
            cursor=cursor,
        )

    @server.tool(
        name="get_post_detail",
        description="读取单条帖子详情，返回原文和对话流。",
    )
    async def get_post_detail(post_uid: str) -> dict[str, object]:
        request_meta = require_current_request_meta()
        return await asyncio.to_thread(
            run_get_post_detail_tool,
            request_meta=request_meta,
            post_uid=post_uid,
        )

    app = server.streamable_http_app()
    app.state.mcp_session_manager = server.session_manager
    app.add_middleware(McpRequestMetaMiddleware)
    return app


def require_mcp_session_manager(http_app):
    return http_app.state.mcp_session_manager


__all__ = [
    "MCP_ROUTE_MOUNT_PATH",
    "create_mcp_http_app",
    "require_mcp_session_manager",
]
