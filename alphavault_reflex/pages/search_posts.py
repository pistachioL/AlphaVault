from __future__ import annotations

import reflex as rx

from alphavault_reflex.pages.original_link_components import original_post_link
from alphavault_reflex.pages.thread_tree_components import tree_line_row
from alphavault_reflex.post_search_state import PostSearchState

DETAIL_DIALOG_TITLE = "帖子详情"
DETAIL_TREE_TITLE = "对话流"
DETAIL_RAW_TITLE = "原文"
DETAIL_BUTTON_TEXT = "详情"
ORIGINAL_LINK_TEXT = "原文链"
EMPTY_TEXT = "没有命中结果。"
LOADING_TEXT = "加载中…"
CLOSE_TEXT = "关"
LOAD_MORE_TEXT = "加载更多"


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


def _result_card(row: rx.Var[dict[str, str]]) -> rx.Component:
    return rx.el.div(
        rx.hstack(
            rx.text(row["title"], class_name="av-research-signal-title"),
            rx.spacer(),
            rx.el.span(row["match_reason"], class_name="av-research-chip"),
            width="100%",
            align="center",
        ),
        rx.hstack(
            rx.text(row["source_label"], class_name="av-research-muted"),
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
            spacing="3",
            align="center",
            style={"flexWrap": "wrap"},
        ),
        rx.text(row["preview"], class_name="av-post-search-preview"),
        rx.hstack(
            rx.button(
                DETAIL_BUTTON_TEXT,
                on_click=lambda: PostSearchState.open_signal_detail(
                    row["post_uid"],
                    row["title"],
                ),
                variant="soft",
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


def _signal_detail_dialog() -> rx.Component:
    return rx.dialog.root(
        rx.dialog.content(
            rx.hstack(
                rx.dialog.title(DETAIL_DIALOG_TITLE),
                rx.spacer(),
                rx.dialog.close(
                    rx.button(
                        CLOSE_TEXT,
                        variant="soft",
                        on_click=PostSearchState.close_signal_detail,
                    )
                ),
                width="100%",
                align="center",
            ),
            rx.cond(
                PostSearchState.signal_detail_title != "",
                rx.text(
                    PostSearchState.signal_detail_title,
                    class_name="av-research-signal-title",
                ),
                rx.el.div(),
            ),
            rx.cond(
                PostSearchState.signal_detail_loading,
                _section_loading(),
                rx.vstack(
                    rx.cond(
                        PostSearchState.signal_detail_tree_text != "",
                        rx.vstack(
                            rx.text(DETAIL_TREE_TITLE, class_name="av-research-muted"),
                            rx.el.div(
                                rx.foreach(
                                    PostSearchState.signal_detail_tree_lines,
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
                        PostSearchState.signal_detail_raw_text != "",
                        rx.vstack(
                            rx.text(DETAIL_RAW_TITLE, class_name="av-research-muted"),
                            rx.el.pre(
                                PostSearchState.signal_detail_raw_text,
                                class_name="av-tree-pre",
                            ),
                            spacing="2",
                            align="stretch",
                            width="100%",
                        ),
                        rx.el.div(),
                    ),
                    rx.cond(
                        (PostSearchState.signal_detail_tree_text == "")
                        & (PostSearchState.signal_detail_raw_text == "")
                        & (PostSearchState.signal_detail_message != ""),
                        rx.text(
                            PostSearchState.signal_detail_message,
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
        open=PostSearchState.signal_detail_open,
        on_open_change=PostSearchState.set_signal_detail_open,
    )


def search_posts_page() -> rx.Component:
    return rx.el.div(
        rx.el.div(
            rx.heading("全文搜索", size="6"),
            rx.text(
                "正文走中文索引，提及词和实体命中优先排在前面。",
                class_name="av-research-muted",
            ),
            class_name="av-research-head",
        ),
        rx.el.div(
            rx.hstack(
                rx.input(
                    value=PostSearchState.search_query,
                    on_change=PostSearchState.set_search_query,
                    placeholder="搜主题、行业、宏观词、事件词",
                    width="100%",
                ),
                rx.button(
                    "搜索",
                    on_click=PostSearchState.submit_search,
                    class_name="av-btn",
                ),
                spacing="3",
                width="100%",
                align="center",
            ),
            rx.text(
                "当前规则会拦截 1 个汉字，简体查询会同时照顾繁体原文。",
                class_name="av-research-muted",
            ),
            class_name="av-home-search-card",
        ),
        rx.hstack(
            rx.link("返回首页", href="/", class_name="av-research-chip"),
            rx.link("打开交易流", href="/homework", class_name="av-research-chip"),
            spacing="3",
            margin_bottom="12px",
        ),
        rx.cond(
            PostSearchState.load_error != "",
            rx.el.div(
                rx.text("错误：", class_name="av-error-title"),
                rx.text(PostSearchState.load_error, class_name="av-error-text"),
                class_name="av-error",
            ),
            rx.el.div(),
        ),
        rx.cond(
            PostSearchState.loading,
            _section_loading(),
            rx.vstack(
                rx.cond(
                    PostSearchState.has_results,
                    rx.el.div(
                        rx.foreach(PostSearchState.results, _result_card),
                        class_name="av-post-search-list",
                    ),
                    rx.cond(
                        PostSearchState.show_empty,
                        rx.text(EMPTY_TEXT, class_name="av-research-muted"),
                        rx.el.div(),
                    ),
                ),
                rx.cond(
                    PostSearchState.show_load_more,
                    rx.button(
                        LOAD_MORE_TEXT,
                        on_click=PostSearchState.load_more,
                        class_name="av-btn",
                    ),
                    rx.cond(
                        PostSearchState.show_loading_more,
                        _section_loading(),
                        rx.el.div(),
                    ),
                ),
                spacing="4",
                align="stretch",
                width="100%",
            ),
        ),
        rx.cond(
            PostSearchState.show_reach_end,
            rx.text("已经到底了。", class_name="av-research-muted"),
            rx.el.div(),
        ),
        _signal_detail_dialog(),
        class_name="av-research-page",
    )
