from __future__ import annotations

import reflex as rx

from alphavault_reflex.home_search_state import HomeSearchState
from alphavault_reflex.pages.stock_link_components import stock_route_link


def _stock_search_result(row: rx.Var[dict[str, str]]) -> rx.Component:
    return stock_route_link(
        rx.vstack(
            rx.hstack(
                rx.text(row["label"], class_name="av-research-signal-title"),
                spacing="2",
                align="center",
            ),
            rx.hstack(
                rx.cond(
                    row["subtitle"] != "",
                    rx.text(row["subtitle"], class_name="av-research-muted"),
                    rx.el.span(""),
                ),
                rx.text(row["match_reason"], class_name="av-research-muted"),
                spacing="2",
                align="center",
            ),
            spacing="1",
            align="start",
        ),
        href=row["href"],
        class_name="av-home-search-result",
    )


def index_page() -> rx.Component:
    return rx.center(
        rx.el.div(
            rx.vstack(
                rx.heading("AlphaVault", size="7"),
                rx.text("首页拆成两个入口：个股直达，全文检索独立走中文索引。"),
                rx.el.div(
                    rx.el.section(
                        rx.vstack(
                            rx.text("股票搜索", class_name="av-home-search-eyebrow"),
                            rx.heading("代码、正式名、已确认简称", size="5"),
                            rx.text(
                                "单一精确命中直接跳个股页，多候选保留列表。",
                                class_name="av-research-muted",
                            ),
                            rx.hstack(
                                rx.input(
                                    value=HomeSearchState.stock_query,
                                    on_change=HomeSearchState.set_stock_query,
                                    placeholder="搜股票代码、正式名、已确认简称",
                                    width="100%",
                                    disabled=HomeSearchState.stock_loading,
                                ),
                                rx.button(
                                    "搜索",
                                    on_click=HomeSearchState.run_stock_search,
                                    loading=HomeSearchState.stock_loading,
                                    disabled=HomeSearchState.stock_loading,
                                    class_name="av-btn",
                                ),
                                spacing="3",
                                width="100%",
                                align="center",
                            ),
                            rx.cond(
                                HomeSearchState.stock_error != "",
                                rx.el.div(
                                    rx.text("错误：", class_name="av-error-title"),
                                    rx.text(
                                        HomeSearchState.stock_error,
                                        class_name="av-error-text",
                                    ),
                                    class_name="av-error",
                                ),
                                rx.el.div(),
                            ),
                            rx.cond(
                                HomeSearchState.has_stock_results,
                                rx.el.div(
                                    rx.foreach(
                                        HomeSearchState.stock_results,
                                        _stock_search_result,
                                    ),
                                    class_name="av-home-search-list",
                                ),
                                rx.el.div(),
                            ),
                            spacing="3",
                            align="stretch",
                            width="100%",
                        ),
                        class_name="av-home-search-card",
                    ),
                    rx.el.section(
                        rx.vstack(
                            rx.text("全文搜索", class_name="av-home-search-eyebrow"),
                            rx.heading("关键字搜帖子正文与结构化提及", size="5"),
                            rx.text(
                                "简体关键字会同时照顾繁体原文，1 个汉字直接拦截。",
                                class_name="av-research-muted",
                            ),
                            rx.hstack(
                                rx.input(
                                    value=HomeSearchState.fulltext_query,
                                    on_change=HomeSearchState.set_fulltext_query,
                                    placeholder="搜主题、行业、宏观词、事件词",
                                    width="100%",
                                    disabled=HomeSearchState.fulltext_loading,
                                ),
                                rx.button(
                                    "进入全文搜索",
                                    on_click=HomeSearchState.run_fulltext_search,
                                    loading=HomeSearchState.fulltext_loading,
                                    disabled=HomeSearchState.fulltext_loading,
                                    class_name="av-btn",
                                ),
                                spacing="3",
                                width="100%",
                                align="center",
                            ),
                            rx.text(
                                "结果页会把 AI 提及词、实体命中排在正文命中前面。",
                                class_name="av-research-muted",
                            ),
                            rx.cond(
                                HomeSearchState.fulltext_error != "",
                                rx.el.div(
                                    rx.text("错误：", class_name="av-error-title"),
                                    rx.text(
                                        HomeSearchState.fulltext_error,
                                        class_name="av-error-text",
                                    ),
                                    class_name="av-error",
                                ),
                                rx.el.div(),
                            ),
                            spacing="3",
                            align="stretch",
                            width="100%",
                        ),
                        class_name="av-home-search-card",
                    ),
                    class_name="av-home-search-grid",
                ),
                rx.hstack(
                    rx.link(
                        "打开交易流", href="/homework", class_name="av-research-chip"
                    ),
                    rx.link(
                        "打开整理中心",
                        href="/organizer",
                        class_name="av-research-chip",
                    ),
                    rx.link(
                        "打开全文搜索页",
                        href="/search/posts",
                        class_name="av-research-chip",
                    ),
                    spacing="3",
                    margin_top="10px",
                ),
                spacing="4",
                align="center",
                class_name="av-home-shell",
            ),
            class_name="av-home-wrap",
        ),
        min_height="100vh",
        padding="24px",
        class_name="av-research-page",
    )
