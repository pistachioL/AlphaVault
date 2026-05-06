from __future__ import annotations

from alphavault.constants import SCHEMA_STANDARD

MCP_CALL_HISTORY_TABLE = f"{SCHEMA_STANDARD}.mcp_call_history"
MCP_CALL_HISTORY_POSTS_TABLE = f"{SCHEMA_STANDARD}.mcp_call_history_posts"

__all__ = [
    "MCP_CALL_HISTORY_POSTS_TABLE",
    "MCP_CALL_HISTORY_TABLE",
]
