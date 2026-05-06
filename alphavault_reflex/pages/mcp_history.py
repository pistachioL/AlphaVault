from __future__ import annotations

import reflex as rx

from alphavault_reflex.mcp_history_constants import MCP_HISTORY_PAGE_TITLE
from alphavault_reflex.mcp_history_state import McpHistoryState
from alphavault_reflex.pages.original_link_components import original_post_link
from alphavault_reflex.pages.thread_tree_components import tree_line_row

DETAIL_BUTTON_TEXT = "详情"
DETAIL_DIALOG_TITLE = "MCP 调用详情"
DETAIL_TREE_TITLE = "对话流"
DETAIL_RAW_TITLE = "原文"
POST_DETAIL_DIALOG_TITLE = "帖子详情"
CLOSE_TEXT = "关"
EMPTY_TEXT = "还没有 MCP 调用记录。"
LOADING_TEXT = "加载中…"
REFRESH_TEXT = "刷新"
ORIGINAL_LINK_TEXT = "原文链"


def _section_loading() -> rx.Component:
    return rx.el.div(
        rx.spinner(size="3"),
        rx.text(LOADING_TEXT, class_name="av-research-muted"),
        style={
            "display": "flex",
            "alignItems": "center",
            "gap": "10px",
            "marginTop": "12px",
        },
    )


def _history_row(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
        rx.hstack(
            rx.text(row["tool_name"], class_name="av-research-signal-title"),
            rx.spacer(),
            rx.el.span(row["status"], class_name="av-research-chip"),
            width="100%",
            align="center",
        ),
        rx.hstack(
            rx.text(row["created_at"], class_name="av-research-muted"),
            rx.cond(
                row["trace_id"] != "",
                rx.text(f"trace_id={row['trace_id']}", class_name="av-research-muted"),
                rx.el.span(""),
            ),
            rx.cond(
                row["access_email"] != "",
                rx.text(row["access_email"], class_name="av-research-muted"),
                rx.el.span(""),
            ),
            spacing="3",
            align="center",
            style={"flexWrap": "wrap"},
        ),
        rx.text(row["input_summary"], class_name="av-post-search-preview"),
        rx.hstack(
            rx.cond(
                row["resolved_stock_key"] != "",
                rx.text(row["resolved_stock_key"], class_name="av-research-chip"),
                rx.el.span(""),
            ),
            rx.text(f"命中 {row['result_count']} 条", class_name="av-research-muted"),
            rx.text(f"{row['duration_ms']}ms", class_name="av-research-muted"),
            spacing="3",
            align="center",
            style={"flexWrap": "wrap"},
        ),
        rx.cond(
            row["error_text"] != "",
            rx.text(row["error_text"], class_name="av-error-text"),
            rx.el.div(),
        ),
        rx.hstack(
            rx.button(
                DETAIL_BUTTON_TEXT,
                on_click=lambda: McpHistoryState.open_detail(row["call_id"]),
                variant="soft",
            ),
            spacing="3",
            margin_top="10px",
        ),
        class_name="av-post-search-card",
    )


def _detail_post_row(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
        rx.hstack(
            rx.text(row["display_title"], class_name="av-research-signal-title"),
            rx.spacer(),
            rx.el.span(row["source_kind"], class_name="av-research-chip"),
            width="100%",
            align="center",
        ),
        rx.hstack(
            rx.cond(
                row["author"] != "",
                rx.text(row["author"], class_name="av-research-muted"),
                rx.el.span(""),
            ),
            rx.cond(
                row["created_at"] != "",
                rx.text(row["created_at"], class_name="av-research-muted"),
                rx.el.span(""),
            ),
            rx.cond(
                row["match_reason"] != "",
                rx.text(row["match_reason"], class_name="av-research-muted"),
                rx.el.span(""),
            ),
            spacing="3",
            align="center",
            style={"flexWrap": "wrap"},
        ),
        rx.cond(
            row["preview"] != "",
            rx.text(row["preview"], class_name="av-post-search-preview"),
            rx.el.div(),
        ),
        rx.hstack(
            rx.cond(
                row["post_uid"] != "",
                rx.button(
                    DETAIL_BUTTON_TEXT,
                    on_click=lambda: McpHistoryState.open_signal_detail(
                        row["post_uid"],
                        row["display_title"],
                    ),
                    variant="soft",
                ),
                rx.el.span(""),
            ),
            rx.cond(
                row["url"] != "",
                original_post_link(
                    ORIGINAL_LINK_TEXT,
                    row["url"],
                    row["post_uid"],
                    class_name="av-research-chip",
                ),
                rx.el.span(""),
            ),
            spacing="3",
            margin_top="10px",
        ),
        class_name="av-post-search-card",
    )


def _call_detail_dialog() -> rx.Component:
    return rx.dialog.root(
        rx.dialog.content(
            rx.hstack(
                rx.dialog.title(DETAIL_DIALOG_TITLE),
                rx.spacer(),
                rx.dialog.close(
                    rx.button(
                        CLOSE_TEXT,
                        variant="soft",
                        on_click=McpHistoryState.close_detail,
                    )
                ),
                width="100%",
                align="center",
            ),
            rx.cond(
                McpHistoryState.detail_loading,
                _section_loading(),
                rx.vstack(
                    rx.cond(
                        McpHistoryState.detail_error != "",
                        rx.text(
                            McpHistoryState.detail_error,
                            class_name="av-error-text",
                        ),
                        rx.el.div(),
                    ),
                    rx.cond(
                        McpHistoryState.detail_has_row,
                        rx.vstack(
                            rx.hstack(
                                rx.text(
                                    McpHistoryState.detail_row["tool_name"],
                                    class_name="av-research-signal-title",
                                ),
                                rx.el.span(
                                    McpHistoryState.detail_row["status"],
                                    class_name="av-research-chip",
                                ),
                                spacing="3",
                                align="center",
                                style={"flexWrap": "wrap"},
                            ),
                            rx.text(
                                McpHistoryState.detail_row["created_at"],
                                class_name="av-research-muted",
                            ),
                            rx.text(
                                f"trace_id={McpHistoryState.detail_row['trace_id']}",
                                class_name="av-research-muted",
                            ),
                            rx.cond(
                                McpHistoryState.detail_row["cf_ray"] != "",
                                rx.text(
                                    f"Cf-Ray={McpHistoryState.detail_row['cf_ray']}",
                                    class_name="av-research-muted",
                                ),
                                rx.el.div(),
                            ),
                            rx.cond(
                                McpHistoryState.detail_row["access_email"] != "",
                                rx.text(
                                    McpHistoryState.detail_row["access_email"],
                                    class_name="av-research-muted",
                                ),
                                rx.el.div(),
                            ),
                            rx.cond(
                                McpHistoryState.detail_row["access_subject"] != "",
                                rx.text(
                                    McpHistoryState.detail_row["access_subject"],
                                    class_name="av-research-muted",
                                ),
                                rx.el.div(),
                            ),
                            rx.cond(
                                McpHistoryState.detail_row["access_aud"] != "",
                                rx.text(
                                    McpHistoryState.detail_row["access_aud"],
                                    class_name="av-research-muted",
                                ),
                                rx.el.div(),
                            ),
                            rx.text(
                                f"auth={McpHistoryState.detail_row['auth_mode']} / path={McpHistoryState.detail_row['request_path']}",
                                class_name="av-research-muted",
                            ),
                            rx.text(
                                f"命中 {McpHistoryState.detail_row['result_count']} 条 / {McpHistoryState.detail_row['duration_ms']}ms",
                                class_name="av-research-muted",
                            ),
                            rx.cond(
                                McpHistoryState.detail_row["resolved_stock_key"] != "",
                                rx.text(
                                    McpHistoryState.detail_row["resolved_stock_key"],
                                    class_name="av-research-muted",
                                ),
                                rx.el.div(),
                            ),
                            rx.cond(
                                McpHistoryState.detail_row["error_text"] != "",
                                rx.text(
                                    McpHistoryState.detail_row["error_text"],
                                    class_name="av-error-text",
                                ),
                                rx.el.div(),
                            ),
                            rx.text("输入参数", class_name="av-research-muted"),
                            rx.el.pre(
                                McpHistoryState.detail_row["input_json_pretty"],
                                class_name="av-tree-pre",
                            ),
                            rx.cond(
                                McpHistoryState.detail_has_posts,
                                rx.vstack(
                                    rx.text("命中帖子", class_name="av-research-muted"),
                                    rx.el.div(
                                        rx.foreach(
                                            McpHistoryState.detail_posts,
                                            _detail_post_row,
                                        ),
                                        class_name="av-post-search-list",
                                    ),
                                    spacing="3",
                                    align="stretch",
                                    width="100%",
                                ),
                                rx.text(
                                    "这次调用没有命中帖子。",
                                    class_name="av-research-muted",
                                ),
                            ),
                            spacing="3",
                            align="stretch",
                            width="100%",
                        ),
                        rx.el.div(),
                    ),
                    spacing="3",
                    align="stretch",
                    width="100%",
                ),
            ),
            style={
                "max_width": "min(1080px, 92vw)",
                "max_height": "85vh",
                "overflow": "auto",
            },
        ),
        open=McpHistoryState.detail_open,
        on_open_change=McpHistoryState.set_detail_open,
    )


def _signal_detail_dialog() -> rx.Component:
    return rx.dialog.root(
        rx.dialog.content(
            rx.hstack(
                rx.dialog.title(POST_DETAIL_DIALOG_TITLE),
                rx.spacer(),
                rx.dialog.close(
                    rx.button(
                        CLOSE_TEXT,
                        variant="soft",
                        on_click=McpHistoryState.close_signal_detail,
                    )
                ),
                width="100%",
                align="center",
            ),
            rx.cond(
                McpHistoryState.signal_detail_title != "",
                rx.text(
                    McpHistoryState.signal_detail_title,
                    class_name="av-research-signal-title",
                ),
                rx.el.div(),
            ),
            rx.cond(
                McpHistoryState.signal_detail_loading,
                _section_loading(),
                rx.vstack(
                    rx.cond(
                        McpHistoryState.signal_detail_tree_text != "",
                        rx.vstack(
                            rx.text(DETAIL_TREE_TITLE, class_name="av-research-muted"),
                            rx.el.div(
                                rx.foreach(
                                    McpHistoryState.signal_detail_tree_lines,
                                    tree_line_row,
                                ),
                                class_name="av-tree-lines",
                            ),
                            spacing="2",
                            align="stretch",
                            width="100%",
                        ),
                        rx.el.div(),
                    ),
                    rx.cond(
                        McpHistoryState.signal_detail_raw_text != "",
                        rx.vstack(
                            rx.text(DETAIL_RAW_TITLE, class_name="av-research-muted"),
                            rx.el.pre(
                                McpHistoryState.signal_detail_raw_text,
                                class_name="av-tree-pre",
                            ),
                            spacing="2",
                            align="stretch",
                            width="100%",
                        ),
                        rx.el.div(),
                    ),
                    rx.cond(
                        (McpHistoryState.signal_detail_tree_text == "")
                        & (McpHistoryState.signal_detail_raw_text == "")
                        & (McpHistoryState.signal_detail_message != ""),
                        rx.text(
                            McpHistoryState.signal_detail_message,
                            class_name="av-research-muted",
                        ),
                        rx.el.div(),
                    ),
                    spacing="3",
                    align="stretch",
                    width="100%",
                ),
            ),
            style={
                "max_width": "min(960px, 92vw)",
                "max_height": "85vh",
                "overflow": "auto",
            },
        ),
        open=McpHistoryState.signal_detail_open,
        on_open_change=McpHistoryState.set_signal_detail_open,
    )


def mcp_history_page() -> rx.Component:
    return rx.el.div(
        rx.el.div(
            rx.heading(MCP_HISTORY_PAGE_TITLE, size="6"),
            rx.text(
                "看每次工具调用、Cloudflare Access 元信息和命中帖子。",
                class_name="av-research-muted",
            ),
            class_name="av-research-head",
        ),
        rx.hstack(
            rx.button(
                REFRESH_TEXT,
                on_click=McpHistoryState.load_history,
                loading=McpHistoryState.loading,
                disabled=McpHistoryState.loading,
                class_name="av-btn",
            ),
            rx.link("返回首页", href="/", class_name="av-research-chip"),
            rx.link(
                "打开全文搜索页", href="/search/posts", class_name="av-research-chip"
            ),
            spacing="3",
            margin_bottom="12px",
            style={"flexWrap": "wrap"},
        ),
        rx.cond(
            McpHistoryState.load_error != "",
            rx.el.div(
                rx.text("错误：", class_name="av-error-title"),
                rx.text(McpHistoryState.load_error, class_name="av-error-text"),
                class_name="av-error",
            ),
            rx.el.div(),
        ),
        rx.cond(
            McpHistoryState.show_loading,
            _section_loading(),
            rx.cond(
                McpHistoryState.has_rows,
                rx.el.div(
                    rx.foreach(McpHistoryState.rows, _history_row),
                    class_name="av-post-search-list",
                ),
                rx.cond(
                    McpHistoryState.show_empty,
                    rx.text(EMPTY_TEXT, class_name="av-research-muted"),
                    rx.el.div(),
                ),
            ),
        ),
        _call_detail_dialog(),
        _signal_detail_dialog(),
        class_name="av-research-page",
    )


__all__ = ["mcp_history_page"]
