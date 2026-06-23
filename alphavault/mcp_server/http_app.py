from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from alphavault.domains.stock.view_scope import DEFAULT_STOCK_VIEW_SCOPE

from .request_meta import McpRequestMetaMiddleware, require_current_request_meta
from .tool_runner import (
    run_get_post_detail_tool,
    run_get_portfolio_context_tool,
    run_get_stock_obvious_trades_tool,
    run_get_stock_evidence_pack_tool,
    run_get_stock_page_tool,
    run_get_stock_summary_tool,
    run_list_obvious_trades_tool,
    run_resolve_stock_tool,
    run_search_posts_semantic_tool,
    run_search_posts_tool,
)

if TYPE_CHECKING:
    from alphavault.agent_tools.post_tools import (
        AgentPostDetailResult,
        AgentPostSearchResult,
    )
    from alphavault.agent_tools.stock_tools import (
        AgentObviousTradeListResult,
        AgentResolveStockResult,
        AgentStockObviousTradeResult,
        AgentStockPageResult,
    )
    from alphavault.capabilities.stock_analysis import (
        PortfolioContext,
        StockEvidencePack,
    )
    from alphavault.capabilities.stock_summary import StockSummaryResult

MCP_ROUTE_MOUNT_PATH = "/mcp"
_DEFAULT_STOCK_CANDIDATE_LIMIT = 5
_DEFAULT_SIGNAL_PAGE = 1
_DEFAULT_SIGNAL_PAGE_SIZE = 10
_DEFAULT_POST_SEARCH_LIMIT = 10
_DEFAULT_RELATED_FILTER = "all"
_DEFAULT_VIEW_SCOPE = DEFAULT_STOCK_VIEW_SCOPE
_SEARCH_POSTS_TOOL_DESCRIPTION = (
    "按关键字搜索帖子正文、提及词和结构化实体。"
    "`query` 支持 PGroonga 查询语法：空格表示同时匹配多个词，"
    "`OR` 表示任选其一，双引号表示短语，前缀 `-` 表示排除词。"
)
_SEARCH_POSTS_SEMANTIC_TOOL_DESCRIPTION = (
    "按语义搜索帖子内容。先在 Zilliz 向量库里做向量召回，再按需要用重排模型重排。"
)


def _load_mcp_output_types() -> None:
    if "PortfolioContext" in globals():
        return

    from alphavault.agent_tools.post_tools import (
        AgentPostDetailResult as _AgentPostDetailResult,
        AgentPostSearchResult as _AgentPostSearchResult,
    )
    from alphavault.agent_tools.stock_tools import (
        AgentObviousTradeListResult as _AgentObviousTradeListResult,
        AgentResolveStockResult as _AgentResolveStockResult,
        AgentStockObviousTradeResult as _AgentStockObviousTradeResult,
        AgentStockPageResult as _AgentStockPageResult,
    )
    from alphavault.capabilities.stock_analysis import (
        PortfolioContext as _PortfolioContext,
        StockEvidencePack as _StockEvidencePack,
    )
    from alphavault.capabilities.stock_summary import (
        StockSummaryResult as _StockSummaryResult,
    )

    globals().update(
        {
            "AgentPostDetailResult": _AgentPostDetailResult,
            "AgentPostSearchResult": _AgentPostSearchResult,
            "AgentObviousTradeListResult": _AgentObviousTradeListResult,
            "AgentResolveStockResult": _AgentResolveStockResult,
            "AgentStockObviousTradeResult": _AgentStockObviousTradeResult,
            "AgentStockPageResult": _AgentStockPageResult,
            "PortfolioContext": _PortfolioContext,
            "StockEvidencePack": _StockEvidencePack,
            "StockSummaryResult": _StockSummaryResult,
        }
    )


def create_mcp_http_app():
    _load_mcp_output_types()
    from alphavault.capabilities.post_search_semantic import (
        DEFAULT_SEMANTIC_POST_CANDIDATE_LIMIT,
    )
    from alphavault.capabilities.stock_analysis import (
        DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS,
        DEFAULT_STOCK_EVIDENCE_MAX_POSTS,
        DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
    )
    from alphavault.capabilities.stock_obvious_trades import (
        DEFAULT_OBVIOUS_TRADE_FILTER,
        DEFAULT_OBVIOUS_TRADE_LIMIT,
        DEFAULT_OBVIOUS_TRADE_LOOKBACK_DAYS,
        DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH,
        DEFAULT_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE,
        DEFAULT_STOCK_OBVIOUS_TRADE_LOOKBACK_DAYS,
    )

    server = FastMCP(
        "AlphaVault MCP",
        instructions="通过 AlphaVault 数据库检索个股、明显交易、帖子和帖子详情。",
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
    ) -> AgentResolveStockResult:
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
    ) -> AgentStockPageResult:
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
        name="list_obvious_trades",
        description="读取最近一段时间的明显交易标的列表，按标的聚合最近动作、买卖强度和提及人数。",
    )
    async def list_obvious_trades(
        lookback_days: int = DEFAULT_OBVIOUS_TRADE_LOOKBACK_DAYS,
        trade_filter: str = DEFAULT_OBVIOUS_TRADE_FILTER,
        min_strength: int = DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH,
        limit: int = DEFAULT_OBVIOUS_TRADE_LIMIT,
    ) -> AgentObviousTradeListResult:
        request_meta = require_current_request_meta()
        return await asyncio.to_thread(
            run_list_obvious_trades_tool,
            request_meta=request_meta,
            lookback_days=lookback_days,
            trade_filter=trade_filter,
            min_strength=min_strength,
            limit=limit,
        )

    @server.tool(
        name="get_stock_obvious_trades",
        description="读取单只股票最近一段时间的明显交易信号明细；传 view_scope='company' 时聚合同公司 A/H。",
    )
    async def get_stock_obvious_trades(
        stock: str,
        lookback_days: int = DEFAULT_STOCK_OBVIOUS_TRADE_LOOKBACK_DAYS,
        trade_filter: str = DEFAULT_OBVIOUS_TRADE_FILTER,
        min_strength: int = DEFAULT_OBVIOUS_TRADE_MIN_STRENGTH,
        signal_page: int = _DEFAULT_SIGNAL_PAGE,
        signal_page_size: int = DEFAULT_OBVIOUS_TRADE_SIGNAL_PAGE_SIZE,
        view_scope: str = _DEFAULT_VIEW_SCOPE,
    ) -> AgentStockObviousTradeResult:
        request_meta = require_current_request_meta()
        return await asyncio.to_thread(
            run_get_stock_obvious_trades_tool,
            request_meta=request_meta,
            stock=stock,
            lookback_days=lookback_days,
            trade_filter=trade_filter,
            min_strength=min_strength,
            signal_page=signal_page,
            signal_page_size=signal_page_size,
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
    ) -> StockEvidencePack:
        request_meta = require_current_request_meta()
        return await asyncio.to_thread(
            run_get_stock_evidence_pack_tool,
            request_meta=request_meta,
            stock=stock,
            window_days=window_days,
            max_posts=max_posts,
        )

    @server.tool(
        name="get_stock_summary",
        description="读取公司级单票 AI 总结，返回压缩证据、分歧统计和结构化摘要。",
    )
    async def get_stock_summary(
        stock: str,
        window_days: int = DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
        max_posts: int = DEFAULT_STOCK_EVIDENCE_MAX_POSTS,
    ) -> StockSummaryResult:
        request_meta = require_current_request_meta()
        return await asyncio.to_thread(
            run_get_stock_summary_tool,
            request_meta=request_meta,
            stock=stock,
            window_days=window_days,
            max_posts=max_posts,
        )

    @server.tool(
        name="get_portfolio_context",
        description="读取一组持仓的公司级组合上下文，返回去重后的公司列表、组合级候选分桶和共享作者。",
    )
    async def get_portfolio_context(
        stocks: list[str],
        window_days: int = DEFAULT_STOCK_EVIDENCE_WINDOW_DAYS,
        max_posts_per_stock: int = DEFAULT_PORTFOLIO_EVIDENCE_MAX_POSTS,
    ) -> PortfolioContext:
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
        description=_SEARCH_POSTS_TOOL_DESCRIPTION,
    )
    async def search_posts(
        query: str,
        limit: int = _DEFAULT_POST_SEARCH_LIMIT,
        cursor: str = "",
    ) -> AgentPostSearchResult:
        request_meta = require_current_request_meta()
        return await asyncio.to_thread(
            run_search_posts_tool,
            request_meta=request_meta,
            query=query,
            limit=limit,
            cursor=cursor,
        )

    @server.tool(
        name="search_posts_semantic",
        description=_SEARCH_POSTS_SEMANTIC_TOOL_DESCRIPTION,
    )
    async def search_posts_semantic(
        query: str,
        limit: int = _DEFAULT_POST_SEARCH_LIMIT,
        cursor: str = "",
        candidate_limit: int = DEFAULT_SEMANTIC_POST_CANDIDATE_LIMIT,
    ) -> AgentPostSearchResult:
        request_meta = require_current_request_meta()
        return await asyncio.to_thread(
            run_search_posts_semantic_tool,
            request_meta=request_meta,
            query=query,
            limit=limit,
            cursor=cursor,
            candidate_limit=candidate_limit,
        )

    @server.tool(
        name="get_post_detail",
        description="读取单条帖子详情，返回原文和对话流。",
    )
    async def get_post_detail(post_uid: str) -> AgentPostDetailResult:
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
