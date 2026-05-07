from __future__ import annotations

import asyncio

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from alphavault.domains.stock.view_scope import DEFAULT_STOCK_VIEW_SCOPE

from .request_meta import McpRequestMetaMiddleware, require_current_request_meta
from .tool_runner import (
    run_get_post_detail_tool,
    run_get_portfolio_context_tool,
    run_get_stock_evidence_pack_tool,
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
_DEFAULT_VIEW_SCOPE = DEFAULT_STOCK_VIEW_SCOPE


def create_mcp_http_app():
    from alphavault.capabilities.stock_analysis import (
        DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS,
        DEFAULT_STOCK_EVIDENCE_MAX_POSTS,
        DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
    )

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
        description="读取个股研究页信号列表。默认只查单票；传 view_scope='company' 时聚合同公司 A/H。",
    )
    async def get_stock_page(
        stock: str,
        signal_page: int = _DEFAULT_SIGNAL_PAGE,
        signal_page_size: int = _DEFAULT_SIGNAL_PAGE_SIZE,
        author: str = "",
        related_filter: str = _DEFAULT_RELATED_FILTER,
        view_scope: str = _DEFAULT_VIEW_SCOPE,
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
            view_scope=view_scope,
        )

    @server.tool(
        name="get_stock_evidence_pack",
        description="读取公司级单票证据包，返回 A/H 覆盖、样本统计、分歧分数和压缩后的证据行。",
    )
    async def get_stock_evidence_pack(
        stock: str,
        window_days: int = DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
        max_posts: int = DEFAULT_STOCK_EVIDENCE_MAX_POSTS,
    ) -> dict[str, object]:
        request_meta = require_current_request_meta()
        return await asyncio.to_thread(
            run_get_stock_evidence_pack_tool,
            request_meta=request_meta,
            stock=stock,
            window_days=window_days,
            max_posts=max_posts,
        )

    @server.tool(
        name="get_portfolio_context",
        description="读取一组持仓的公司级组合上下文，返回去重后的公司列表、分歧排序和共享作者。",
    )
    async def get_portfolio_context(
        stocks: list[str],
        window_days: int = DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
        max_posts_per_stock: int = DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS,
    ) -> dict[str, object]:
        request_meta = require_current_request_meta()
        return await asyncio.to_thread(
            run_get_portfolio_context_tool,
            request_meta=request_meta,
            stocks=stocks,
            window_days=window_days,
            max_posts_per_stock=max_posts_per_stock,
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
